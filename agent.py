import streamlit as st
from pymongo import MongoClient, ASCENDING
import redis
import pandas as pd
from datetime import datetime
import uuid
import re
import os
from dotenv import load_dotenv
from neo4j import GraphDatabase


# ============================================================================
# CONFIGURAÇÃO DO BANCO (MONGODB ATLAS - NUVEM)
# ============================================================================

# Carrega as variáveis definidas no arquivo .env (não deve ser versionado no Git)
load_dotenv()

# String de conexão lida do ambiente, com as credenciais fora do código-fonte
uri = os.getenv("MONGODB_URI")

if not uri:
    st.error("Variável MONGODB_URI não encontrada. Verifique se o arquivo .env está configurado corretamente.")
    st.stop()

try:
    # 1. Estabelece a conexão com o cluster
    client = MongoClient(uri)
    db = client['sentinel_db']

    # 2. Testa se a conexão está ativa (Ping) - falha rápido se a URI/rede estiverem erradas
    client.admin.command('ping')

    # 3. Define as coleções (equivalente a "tabelas" em bancos relacionais)
    logs_table = db['logs_brutos']
    context_table = db['contexto_negocio']
    incidents_table = db['incidentes']

    # 4. IMPLEMENTAÇÃO DE 2 ÍNDICES (Requisito do Professor)
    # Índice único: garante que não existam incidentes duplicados para a mesma assinatura de erro
    incidents_table.create_index([("assinatura_do_erro", ASCENDING)], unique=True)
    # Índice simples: acelera filtros/consultas por severidade (P1, P2, P3)
    incidents_table.create_index([("analise_da_IA.severidade", ASCENDING)])

    st.sidebar.success("Conectado ao MongoDB Atlas! ☁️")

except Exception as e:
    # Se a conexão ou os índices falharem, encerra o app para evitar estado inconsistente
    st.error(f"Erro fatal de conexão ou configuração: {e}")
    st.stop()


# ============================================================================
# CONFIGURAÇÃO DO REDIS (ESTRUTURAS EM MEMÓRIA - TEMPO REAL)
# ============================================================================

redis_uri = os.getenv("REDIS_URI")

if not redis_uri:
    st.error("Variável REDIS_URI não encontrada. Verifique se o arquivo .env está configurado corretamente.")
    st.stop()

try:
    # decode_responses=True faz o cliente devolver str em vez de bytes
    redis_client = redis.from_url(redis_uri, decode_responses=True)
    redis_client.ping()
    st.sidebar.success("Conectado ao Redis Cloud! 🔴")
except Exception as e:
    st.error(f"Erro fatal de conexão com o Redis: {e}")
    st.stop()

# Chave do Sorted Set usado como leaderboard em tempo real (Feature 1)
REDIS_RANKING_KEY = "ranking:squads"


# ============================================================================
# CONFIGURAÇÃO DO NEO4J (GRAFO DE DEPENDÊNCIAS - AURA)
# ============================================================================

neo4j_uri = os.getenv("NEO4J_URI")
neo4j_user = os.getenv("NEO4J_USER")
neo4j_password = os.getenv("NEO4J_PASSWORD")

if not neo4j_uri or not neo4j_user or not neo4j_password:
    st.error("Variáveis NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD não encontradas. Verifique o arquivo .env.")
    st.stop()

try:
    neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    neo4j_driver.verify_connectivity()
    st.sidebar.success("Conectado ao Neo4j Aura! 🟢")
except Exception as e:
    st.error(f"Erro fatal de conexão com o Neo4j: {e}")
    st.stop()

# Configuração da página Streamlit
st.set_page_config(page_title="Sentinel NoSQL - MongoDB Cloud", layout="wide")
st.title("Sentinel NoSQL: AIOps Incident Manager 🚀 (Cloud Edition)")


# ============================================================================
# AGENTES BÁSICOS
# ============================================================================

def agente_1_extrair_erro(texto_bruto: str) -> dict:
    """Agente 1: classifica o log bruto em um tipo de erro e gera uma assinatura padronizada."""
    if "PSQLException" in texto_bruto or "PostgreSQL" in texto_bruto:
        tipo, assinatura = "DATABASE", "PSQLException_Error"
    elif "Connection timeout" in texto_bruto or "timeout" in texto_bruto.lower():
        tipo, assinatura = "NETWORK", "ConnectionTimeout_Error"
    elif "Slow query" in texto_bruto:
        tipo, assinatura = "PERFORMANCE", "SlowQuery_Error"
    elif "OutOfMemory" in texto_bruto:
        tipo, assinatura = "MEMORY", "OutOfMemory_Error"
    else:
        tipo, assinatura = "OUTRO", "UnknownError"
    return {"assinatura_do_erro": assinatura, "tipo": tipo}


def agente_2_analisar(assinatura: str, tipo: str, aplicativo: str, contextos: list) -> dict:
    """Agente 2: cruza o erro com o contexto de negócio para definir severidade, impacto e squad responsável."""
    # Busca o contexto do serviço afetado; usa fallback se o serviço não estiver cadastrado
    ctx = next((c for c in contextos if c.get('nome_do_servico') == aplicativo),
               {'squad_responsavel': 'unknown', 'criticidade': 'BAIXA'})

    # Regra de severidade: serviços críticos com falha de banco/rede viram P1
    if ctx['criticidade'] == 'ALTA' and tipo in ['DATABASE', 'NETWORK']:
        severidade = "P1"
    elif tipo == 'NETWORK':
        severidade = "P2"
    else:
        severidade = "P3"

    # Mapeamento simples de impacto de negócio a partir do nome do aplicativo
    impacto = "Serviço indisponível"
    if "pagamento" in aplicativo: impacto = "Checkout indisponível"
    elif "usuario" in aplicativo: impacto = "Login indisponível"

    return {
        "severidade": severidade,
        "impacto_no_negocio": impacto,
        "squad_responsavel": ctx['squad_responsavel'],
        "acao_sugerida": f"Investigar erro: {assinatura}"
    }


def agente_3_formatar(assinatura: str, analise: dict, total_erros: int) -> str:
    """Agente 3: gera a mensagem resumida usada na integração de saída (ex: Slack)."""
    return f"[{analise['severidade']}] {assinatura} | Vol: {total_erros} | Squad: {analise['squad_responsavel']}"


# ============================================================================
# FUNÇÕES DE AGREGAÇÃO (PIPELINES - Requisito do Professor)
# ============================================================================

def run_pipeline_squad_ranking():
    """Pipeline 1: ranking de squads com mais incidentes críticos (Match, Group, Sort, Project)."""
    pipeline = [
        # Filtra apenas incidentes críticos (P1/P2)
        {"$match": {"analise_da_IA.severidade": {"$in": ["P1", "P2"]}}},
        # Agrupa por squad, somando quantidade de incidentes e volume total de erros
        {"$group": {
            "_id": "$analise_da_IA.squad_responsavel",
            "total_incidentes": {"$sum": 1},
            "volume_erros": {"$sum": "$total_de_erros"}
        }},
        # Ordena do squad com mais erros para o com menos
        {"$sort": {"volume_erros": -1}},
        # Renomeia campos para exibição final
        {"$project": {
            "squad": "$_id",
            "incidentes_criticos": "$total_incidentes",
            "volume_erros": 1,
            "_id": 0
        }}
    ]
    return list(incidents_table.aggregate(pipeline))


def run_pipeline_timeline_unwind():
    """Pipeline 2: linha do tempo dos eventos de erro (Unwind, Set, Project)."""
    pipeline = [
        # Desmembra o array de histórico temporal em um documento por evento
        {"$unwind": "$historico_temporal"},
        # Cria/renomeia campos auxiliares para facilitar o project seguinte
        {"$set": {
            "data_evento": "$historico_temporal.periodo",
            "assinatura": "$assinatura_do_erro"
        }},
        # Seleciona apenas os campos relevantes para a timeline
        {"$project": {
            "data_evento": 1,
            "assinatura": 1,
            "severidade": "$analise_da_IA.severidade",
            "_id": 0
        }},
        # Mostra os eventos mais recentes primeiro, limitado aos últimos 10
        {"$sort": {"data_evento": -1}},
        {"$limit": 10}
    ]
    return list(incidents_table.aggregate(pipeline))


# ============================================================================
# FUNÇÕES DO NEO4J (GRAFO DE DEPENDÊNCIAS + GDS)
# ============================================================================

def registrar_servico_e_squad(servico: str, squad: str):
    """Espelha o contexto_negocio do MongoDB também no grafo."""
    with neo4j_driver.session() as session:
        session.run("""
            MERGE (s:Servico {nome: $servico})
            MERGE (sq:Squad {nome: $squad})
            MERGE (s)-[:PERTENCE_A]->(sq)
        """, servico=servico, squad=squad)


def registrar_dependencia(servico_origem: str, servico_dependencia: str):
    """Ex: api-notificacao DEPENDE_DE api-pagamentos."""
    with neo4j_driver.session() as session:
        session.run("""
            MERGE (a:Servico {nome: $origem})
            MERGE (b:Servico {nome: $dependencia})
            MERGE (a)-[:DEPENDE_DE]->(b)
        """, origem=servico_origem, dependencia=servico_dependencia)


def registrar_incidente_no_grafo(assinatura: str, severidade: str, aplicativo: str):
    """Chamado dentro do loop de 'Rodar Agentes', ao lado do zincrby e do pfadd."""
    with neo4j_driver.session() as session:
        session.run("""
            MERGE (s:Servico {nome: $aplicativo})
            CREATE (i:Incidente {assinatura: $assinatura, severidade: $severidade, data: datetime()})
            CREATE (i)-[:AFETA]->(s)
        """, aplicativo=aplicativo, assinatura=assinatura, severidade=severidade)


def rodar_pagerank_servicos() -> pd.DataFrame:
    """Roda a projeção GDS + PageRank e devolve um DataFrame, no mesmo estilo dos pipelines Mongo."""
    with neo4j_driver.session() as session:
        # Remove a projeção anterior, se existir, para evitar erro de "grafo já existe"
        session.run("CALL gds.graph.exists('grafoServicos') YIELD exists "
                     "WITH exists WHERE exists CALL gds.graph.drop('grafoServicos') YIELD graphName RETURN graphName")

        session.run("""
            CALL gds.graph.project.cypher(
                'grafoServicos',
                'MATCH (s:Servico) RETURN id(s) AS id',
                'MATCH (a:Servico)-[:DEPENDE_DE]->(b:Servico) RETURN id(a) AS source, id(b) AS target'
            )
        """)

        resultado = session.run("""
            CALL gds.pageRank.stream('grafoServicos')
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).nome AS servico, score
            ORDER BY score DESC
        """)
        dados = [dict(r) for r in resultado]
        session.run("CALL gds.graph.drop('grafoServicos')")  # libera a memória depois de usar
        return pd.DataFrame(dados)


# ============================================================================
# INTERFACE E FLUXOS
# ============================================================================

# CSS para os botões de navegação da sidebar se comportarem como itens de menu:
# texto alinhado à esquerda (padrão do Streamlit é centralizado) e menos espaço
# vertical entre eles, já que o hack de espaços em branco para indentar não
# tinha efeito (o Streamlit corta espaços à esquerda do texto do botão).
st.markdown("""
<style>
section[data-testid="stSidebar"] div[data-testid="stButton"] button {
    justify-content: flex-start;
    text-align: left;
    padding: 0.35rem 0.75rem;
}
section[data-testid="stSidebar"] div[data-testid="stButton"] {
    margin-bottom: 0.15rem;
}
</style>
""", unsafe_allow_html=True)

# Navegação organizada por categoria: cada bloco representa uma etapa do fluxo
# (Operação = uso diário do sistema | Analytics = consulta/insights | Administração = manutenção)
CATEGORIAS = {
    "📥 Operação": ["📝 Contexto de Negócio", "📄 Logs (Agente 1)", "⚙️ Rodar Agentes"],
    "📊 Analytics": ["🏠 Dashboard", "📈 Analytics (MongoDB + Redis)", "🕸️ Grafo de Dependências"],
    "🛠️ Administração": ["🗄️ CRUD Geral"],
}

if "pagina_atual" not in st.session_state:
    st.session_state.pagina_atual = "🏠 Dashboard"

st.sidebar.title("Navegação")
for categoria, paginas in CATEGORIAS.items():
    st.sidebar.caption(categoria)
    for pagina in paginas:
        esta_ativa = pagina == st.session_state.pagina_atual
        # Indicador visual da página ativa: seta + cor "primary" (o CSS acima cuida do alinhamento)
        rotulo = f"➡️ {pagina}" if esta_ativa else pagina
        if st.sidebar.button(rotulo, key=f"nav_{pagina}", use_container_width=True, type="primary" if esta_ativa else "secondary"):
            st.session_state.pagina_atual = pagina
            st.rerun()

menu = st.session_state.pagina_atual

if menu == "📝 Contexto de Negócio":
    # Tela para cadastrar o contexto de negócio de cada serviço (squad responsável e criticidade)
    st.header("Contexto de Negócio")
    col1, col2 = st.columns(2)
    with col1: servico = st.text_input("Serviço (ex: api-pagamentos)")
    with col2: squad = st.text_input("Squad (ex: squad-checkout)")

    if st.button("Salvar Contexto"):
        if servico and squad:
            context_table.insert_one({'_id': f"ctx_{uuid.uuid4().hex[:6]}", 'nome_do_servico': servico, 'squad_responsavel': squad, 'criticidade': 'ALTA'})
            # Espelha o mesmo contexto no grafo Neo4j
            registrar_servico_e_squad(servico, squad)
            st.success("Contexto salvo no MongoDB Atlas e no Neo4j.")

    ctx_data = list(context_table.find())
    if ctx_data: st.table(pd.DataFrame(ctx_data))

    st.divider()
    st.subheader("Cadastrar Dependência entre Serviços")
    st.caption("Ex: 'api-notificacao' DEPENDE_DE 'api-pagamentos'")
    col1, col2 = st.columns(2)
    with col1: servico_origem = st.text_input("Serviço dependente", key="dep_origem")
    with col2: servico_alvo = st.text_input("Depende de", key="dep_alvo")
    if st.button("Salvar Dependência"):
        if servico_origem and servico_alvo:
            registrar_dependencia(servico_origem, servico_alvo)
            st.success("Dependência registrada no Neo4j.")

elif menu == "📄 Logs (Agente 1)":
    # Tela de ingestão manual de logs brutos, ainda não processados pelos agentes
    st.header("Ingestão de Logs Brutos")
    log = st.text_area("Cole o log de erro aqui:", placeholder="Ex: [ERROR] api-pagamentos - PSQLException...", height=150)
    if st.button("Injetar Log"):
        if log:
            # Extrai o nome do aplicativo a partir do padrão "- nome-do-app -" no log
            match = re.search(r'-\s+(\w+(?:-\w+)*)\s+-', log)
            app = match.group(1) if match else "desconhecido"
            logs_table.insert_one({'_id': f"log_{uuid.uuid4().hex[:6]}", 'timestamp': datetime.now().isoformat(), 'aplicativo': app, 'texto_bruto': log, 'processado_pelo_agente_1': False})
            st.success("Log salvo na nuvem.")

elif menu == "⚙️ Rodar Agentes":
    # Executa a pipeline completa dos agentes sobre todos os logs ainda não processados
    st.header("Motor de Agentes (Pipeline)")
    if st.button("Executar Agentes"):
        logs_nao_proc = list(logs_table.find({"processado_pelo_agente_1": False}))
        contextos = list(context_table.find())

        if logs_nao_proc:
            bar = st.progress(0)
            for i, log in enumerate(logs_nao_proc):
                res_ag1 = agente_1_extrair_erro(log['texto_bruto'])
                assinatura = res_ag1['assinatura_do_erro']
                incidente = incidents_table.find_one({"assinatura_do_erro": assinatura})

                if incidente:
                    # Incidente já existe: apenas incrementa o volume e registra novo evento na timeline
                    novo_total = incidente['total_de_erros'] + 1
                    msg = agente_3_formatar(assinatura, incidente['analise_da_IA'], novo_total)
                    incidents_table.update_one({'_id': incidente['_id']}, {
                        '$set': {'total_de_erros': novo_total, 'integracao_saida.slack': msg},
                        '$push': {'historico_temporal': {"periodo": datetime.now().isoformat(), "ocorrencias": 1}}
                    })
                    squad_do_evento = incidente['analise_da_IA']['squad_responsavel']
                    severidade_do_evento = incidente['analise_da_IA']['severidade']
                else:
                    # Novo tipo de erro: roda Agente 2 (análise) e cria o incidente do zero
                    res_ag2 = agente_2_analisar(assinatura, res_ag1['tipo'], log['aplicativo'], contextos)
                    incidents_table.insert_one({
                        '_id': f"inc_{uuid.uuid4().hex[:6]}", 'assinatura_do_erro': assinatura, 'total_de_erros': 1,
                        'analise_da_IA': res_ag2, 'historico_temporal': [{"periodo": datetime.now().isoformat(), "ocorrencias": 1}],
                        'integracao_saida': {'slack': agente_3_formatar(assinatura, res_ag2, 1)}
                    })
                    squad_do_evento = res_ag2['squad_responsavel']
                    severidade_do_evento = res_ag2['severidade']

                # FEATURE 1 (Sorted Set): incrementa em +1 o score do squad no leaderboard em tempo real
                redis_client.zincrby(REDIS_RANKING_KEY, 1, squad_do_evento)

                # FEATURE 2 (HyperLogLog): registra a assinatura no estimador de erros únicos do serviço.
                # PFADD é idempotente - assinaturas repetidas não inflam a contagem aproximada.
                redis_client.pfadd(f"unicos:{log['aplicativo']}", assinatura)

                # FEATURE 3 (Grafo Neo4j): registra o incidente como nó ligado ao serviço afetado
                registrar_incidente_no_grafo(assinatura, severidade_do_evento, log['aplicativo'])

                # Marca o log como processado para não ser reprocessado em execuções futuras
                logs_table.update_one({'_id': log['_id']}, {'$set': {'processado_pelo_agente_1': True}})
                bar.progress((i + 1) / len(logs_nao_proc))
            st.success("Processamento concluído!")
            st.rerun()

elif menu == "🏠 Dashboard":
    # Visão geral: métricas agregadas + tabela de incidentes
    st.header("Dashboard MongoDB Cloud")
    c1, c2, c3 = st.columns(3)
    c1.metric("Logs Pendentes", logs_table.count_documents({"processado_pelo_agente_1": False}))
    c2.metric("Incidentes Únicos", incidents_table.count_documents({}))

    # Soma o total de ocorrências de erro em todos os incidentes
    pipeline_total = [{"$group": {"_id": None, "total": {"$sum": "$total_de_erros"}}}]
    res = list(incidents_table.aggregate(pipeline_total))
    c3.metric("Total de Ocorrências", res[0]['total'] if res else 0)

    incidentes = list(incidents_table.find())
    if incidentes:
        st.dataframe(pd.DataFrame([{'Assinatura': i['assinatura_do_erro'], 'Severidade': i['analise_da_IA']['severidade'], 'Volume': i['total_de_erros'], 'Squad': i['analise_da_IA']['squad_responsavel']} for i in incidentes]), use_container_width=True)

elif menu == "📈 Analytics (MongoDB + Redis)":
    # Uma única tela reúne as duas camadas de analytics do sistema: agregação exata e
    # em lote (MongoDB) vs. estruturas em memória, aproximadas e em tempo real (Redis).
    st.header("Analytics: Exato (MongoDB) vs. Tempo Real (Redis)")

    aba_mongo, aba_redis = st.tabs(["🍃 MongoDB — Aggregation Pipelines", "🔴 Redis — Estruturas Comuns e Probabilísticas"])

    with aba_mongo:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("1. Ranking de Squads (Match, Group, Sort)")
            ranking = run_pipeline_squad_ranking()
            if ranking: st.table(pd.DataFrame(ranking))

        with col2:
            st.subheader("2. Timeline Unwind (Unwind, Set, Project)")
            timeline = run_pipeline_timeline_unwind()
            if timeline: st.dataframe(pd.DataFrame(timeline))

        st.divider()
        st.subheader("Índices Otimizados (Indexes)")
        st.write("Foram implementados índices para busca ultra-rápida por assinatura e severidade.")
        st.json(list(incidents_table.index_information().keys()))

    with aba_redis:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("1. Leaderboard de Squads (Sorted Set)")
            st.caption("Comando: `ZINCRBY` a cada log processado | Leitura: `ZREVRANGE ... WITHSCORES`")
            # Busca o ranking já ordenado do maior para o menor score (não precisa ordenar em Python)
            ranking_redis = redis_client.zrevrange(REDIS_RANKING_KEY, 0, -1, withscores=True)
            if ranking_redis:
                df_ranking = pd.DataFrame(ranking_redis, columns=["Squad", "Total de Erros"])
                df_ranking["Total de Erros"] = df_ranking["Total de Erros"].astype(int)
                st.table(df_ranking)
            else:
                st.info("Nenhum evento processado ainda. Rode a etapa 'Rodar Agentes' primeiro.")

        with col2:
            st.subheader("2. Erros Únicos por Serviço (HyperLogLog)")
            st.caption("Comando: `PFADD` a cada log processado | Leitura: `PFCOUNT`")
            # Descobre quais aplicativos já tiveram logs ingeridos, para consultar o HLL de cada um
            aplicativos = logs_table.distinct("aplicativo")
            if aplicativos:
                dados_hll = []
                for app in aplicativos:
                    chave = f"unicos:{app}"
                    # PFCOUNT devolve a cardinalidade ESTIMADA (não exata) com ~0.81% de erro típico
                    estimativa = redis_client.pfcount(chave)
                    dados_hll.append({"Aplicativo": app, "Assinaturas Únicas (estimado)": estimativa})
                st.table(pd.DataFrame(dados_hll))
                st.caption("O HyperLogLog usa memória fixa (~12KB por chave), independente do volume de dados.")
            else:
                st.info("Nenhum log foi ingerido ainda.")

        st.divider()
        if st.button("🗑️ Limpar Dados do Redis (Ranking + HyperLogLog)"):
            chaves = [REDIS_RANKING_KEY] + [f"unicos:{app}" for app in logs_table.distinct("aplicativo")]
            if chaves:
                redis_client.delete(*chaves)
            st.rerun()

elif menu == "🕸️ Grafo de Dependências":
    # Camada de grafo (Neo4j + GDS): mapeia dependências entre serviços e identifica
    # os pontos mais críticos da arquitetura via PageRank - algo que o Mongo e o Redis
    # não conseguem responder bem, pois exige atravessar relacionamentos.
    st.header("Grafo de Dependências (Neo4j + GDS)")
    st.caption("Modelo: (Incidente)-[AFETA]->(Servico)-[PERTENCE_A]->(Squad) | (Servico)-[DEPENDE_DE]->(Servico)")

    if st.button("Rodar PageRank (Serviços mais críticos)"):
        try:
            df_pagerank = rodar_pagerank_servicos()
            if not df_pagerank.empty:
                st.subheader("Ranking de criticidade estrutural")
                st.caption("Quanto maior o score, mais outros serviços dependem dele - candidato natural a P1.")
                st.dataframe(df_pagerank, use_container_width=True)
            else:
                st.info("Nenhum serviço/dependência cadastrado ainda. Cadastre em 'Contexto de Negócio'.")
        except Exception as e:
            st.error(f"Erro ao rodar GDS: {e}. Verifique se sua instância Aura tem o plugin GDS habilitado.")

    st.divider()
    st.subheader("Consulta livre em Cypher")
    query_livre = st.text_area("Digite uma query Cypher (somente leitura)", value="MATCH (n) RETURN n LIMIT 25")
    if st.button("Executar Query"):
        if not query_livre.strip().upper().startswith("MATCH"):
            st.error("Por segurança, apenas consultas iniciadas com MATCH são permitidas nesta tela.")
        else:
            try:
                with neo4j_driver.session() as session:
                    resultado = session.run(query_livre)
                    dados = [dict(r) for r in resultado]
                if dados:
                    st.dataframe(pd.DataFrame(dados), use_container_width=True)
                else:
                    st.info("Query executada, sem resultados.")
            except Exception as e:
                st.error(f"Erro na query: {e}")

elif menu == "🗄️ CRUD Geral":
    # Tela administrativa genérica para inspecionar e limpar qualquer coleção
    st.header("Admin MongoDB Atlas")
    colecao_nome = st.selectbox("Selecione a Coleção", ["logs_brutos", "contexto_negocio", "incidentes"])
    if st.button("Limpar Coleção"):
        db[colecao_nome].delete_many({})
        st.rerun()

    dados = list(db[colecao_nome].find())
    st.json(dados)