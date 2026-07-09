import streamlit as st
from pymongo import MongoClient, ASCENDING
import pandas as pd
from datetime import datetime
import uuid
import re
import os
from dotenv import load_dotenv


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
# INTERFACE E FLUXOS
# ============================================================================

menu = st.sidebar.selectbox("Menu", ["Dashboard", "Logs (Agente 1)", "Contexto", "Rodar Agentes", "Analytics & Performance", "CRUD Geral"])

if menu == "Contexto":
    # Tela para cadastrar o contexto de negócio de cada serviço (squad responsável e criticidade)
    st.header("Contexto de Negócio")
    col1, col2 = st.columns(2)
    with col1: servico = st.text_input("Serviço (ex: api-pagamentos)")
    with col2: squad = st.text_input("Squad (ex: squad-checkout)")

    if st.button("Salvar Contexto"):
        if servico and squad:
            context_table.insert_one({'_id': f"ctx_{uuid.uuid4().hex[:6]}", 'nome_do_servico': servico, 'squad_responsavel': squad, 'criticidade': 'ALTA'})
            st.success("Contexto salvo no MongoDB Atlas.")

    ctx_data = list(context_table.find())
    if ctx_data: st.table(pd.DataFrame(ctx_data))

elif menu == "Logs (Agente 1)":
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

elif menu == "Rodar Agentes":
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
                else:
                    # Novo tipo de erro: roda Agente 2 (análise) e cria o incidente do zero
                    res_ag2 = agente_2_analisar(assinatura, res_ag1['tipo'], log['aplicativo'], contextos)
                    incidents_table.insert_one({
                        '_id': f"inc_{uuid.uuid4().hex[:6]}", 'assinatura_do_erro': assinatura, 'total_de_erros': 1,
                        'analise_da_IA': res_ag2, 'historico_temporal': [{"periodo": datetime.now().isoformat(), "ocorrencias": 1}],
                        'integracao_saida': {'slack': agente_3_formatar(assinatura, res_ag2, 1)}
                    })
                # Marca o log como processado para não ser reprocessado em execuções futuras
                logs_table.update_one({'_id': log['_id']}, {'$set': {'processado_pelo_agente_1': True}})
                bar.progress((i + 1) / len(logs_nao_proc))
            st.success("Processamento concluído!")
            st.rerun()

elif menu == "Dashboard":
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

elif menu == "Analytics & Performance":
    # Demonstra o uso das pipelines de agregação e dos índices criados
    st.header("MongoDB Advanced Analytics (Aggregation Pipelines)")

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

elif menu == "CRUD Geral":
    # Tela administrativa genérica para inspecionar e limpar qualquer coleção
    st.header("Admin MongoDB Atlas")
    colecao_nome = st.selectbox("Selecione a Coleção", ["logs_brutos", "contexto_negocio", "incidentes"])
    if st.button("Limpar Coleção"):
        db[colecao_nome].delete_many({})
        st.rerun()

    dados = list(db[colecao_nome].find())
    st.json(dados)