import streamlit as st
from tinydb import TinyDB, Query
import pandas as pd
from datetime import datetime
import uuid
import re

# ============================================================================
# CONFIGURAÇÃO DO BANCO (MODELAGEM UML)
# ============================================================================

db = TinyDB('sentinel_nosql.json')
logs_table = db.table('logs_brutos')
context_table = db.table('contexto_negocio')
incidents_table = db.table('incidentes')

st.set_page_config(page_title="Sentinel NoSQL - Estruturado", layout="wide")
st.title("Sentinel NoSQL: AIOps Incident Manager")

# ============================================================================
# AGENTES BÁSICOS
# ============================================================================

def agente_1_extrair_erro(texto_bruto: str) -> dict:
    """Agente 1: Extrai padrão do erro"""
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
    """Agente 2: Injeta contexto e analisa impacto"""
    ctx = next((c for c in contextos if c.get('nome_do_servico') == aplicativo),
               {'squad_responsavel': 'unknown', 'criticidade': 'BAIXA'})

    if ctx['criticidade'] == 'ALTA' and tipo in ['DATABASE', 'NETWORK']:
        severidade = "P1"
    elif tipo == 'NETWORK':
        severidade = "P2"
    else:
        severidade = "P3"

    if "pagamento" in aplicativo:
        impacto = "Checkout indisponível - usuários não conseguem comprar"
    elif "usuario" in aplicativo or "auth" in aplicativo:
        impacto = "Login indisponível - usuários não conseguem acessar"
    else:
        impacto = "Serviço indisponível"

    return {
        "severidade": severidade,
        "impacto_no_negocio": impacto,
        "squad_responsavel": ctx['squad_responsavel'],
        "acao_sugerida": f"Investigar erro: {assinatura}"
    }

def agente_3_formatar(assinatura: str, analise: dict, total_erros: int) -> str:
    """Agente 3: Formata payload para saída"""
    return f"[{analise['severidade']}] {assinatura}\nVolume: {total_erros} ocorrência(s)\nImpacto: {analise['impacto_no_negocio']}\nSquad: {analise['squad_responsavel']}\nAção: {analise['acao_sugerida']}"

# ============================================================================
# INTERFACE E FLUXOS
# ============================================================================

menu = st.sidebar.selectbox("Menu", ["Dashboard", "Logs (Agente 1)", "Contexto", "Rodar Agentes", "CRUD Geral"])

if menu == "Contexto":
    st.header("Contexto de Negócio")
    col1, col2 = st.columns(2)
    with col1: servico = st.text_input("Serviço (ex: api-pagamentos-v2)")
    with col2: squad = st.text_input("Squad (ex: squad-checkout)")

    if st.button("Salvar Contexto"):
        if servico and squad:
            context_table.insert({
                '_id': f"ctx_{uuid.uuid4().hex[:6]}",
                'nome_do_servico': servico,
                'squad_responsavel': squad,
                'criticidade': 'ALTA'
            })
            st.success("Contexto salvo estruturado com sucesso.")
        else:
            st.error("Preencha todos os campos.")

    st.divider()
    if context_table.all(): st.table(pd.DataFrame(context_table.all()))

elif menu == "Logs (Agente 1)":
    st.header("Ingestão de Logs Brutos")
    log = st.text_area("Cole o log de erro aqui:", placeholder="[ERROR] 2026-05-11 08:05:12 - api-pagamentos-v2 - PSQLException: too many clients", height=150)

    if st.button("Injetar Log"):
        if log:
            match = re.search(r'-\s+(\w+(?:-\w+)*)\s+-', log)
            app = match.group(1) if match else "desconhecido"

            logs_table.insert({
                '_id': f"log_{uuid.uuid4().hex[:6]}",
                'timestamp': datetime.now().isoformat(),
                'aplicativo': app,
                'texto_bruto': log,
                'processado_pelo_agente_1': False
            })
            st.success("Log injetado com sucesso.")
        else:
            st.error("Cole um log válido.")

    st.divider()
    logs_nao_proc = logs_table.search(Query().processado_pelo_agente_1 == False)
    if logs_nao_proc:
        for l in logs_nao_proc: st.warning(f"Pendente - {l['_id']}: {l['texto_bruto'][:100]}...")

elif menu == "Rodar Agentes":
    st.header("Motor de Agentes")
    if st.button("Executar Agentes"):
        logs_nao_proc = logs_table.search(Query().processado_pelo_agente_1 == False)
        contextos = context_table.all()

        if not logs_nao_proc:
            st.warning("Nenhum log pendente para processar.")
        else:
            progress_bar = st.progress(0)
            Incidente = Query()

            for i, log in enumerate(logs_nao_proc):
                agora = datetime.now().isoformat()
                
                # 1. Agente 1 (Coletor)
                res_ag1 = agente_1_extrair_erro(log['texto_bruto'])
                assinatura_atual = res_ag1['assinatura_do_erro']

                # 2. Verifica se o Incidente já existe no banco
                incidente_existente = incidents_table.search(Incidente.assinatura_do_erro == assinatura_atual)

                if incidente_existente:
                    # BUCKETING (Agrega no array historico_temporal)
                    inc = incidente_existente[0]
                    novo_total = inc['total_de_erros'] + 1
                    
                    historico = inc.get('historico_temporal', [])
                    historico.append({"periodo": agora, "ocorrencias": 1})
                    
                    nova_mensagem = agente_3_formatar(assinatura_atual, inc['analise_da_IA'], novo_total)
                    
                    incidents_table.update({
                        'total_de_erros': novo_total,
                        'historico_temporal': historico,
                        'integracao_saida': {'slack': nova_mensagem}
                    }, Incidente._id == inc['_id'])
                
                else:
                    # NOVO INCIDENTE: Fluxo completo
                    res_ag2 = agente_2_analisar(assinatura_atual, res_ag1['tipo'], log['aplicativo'], contextos)
                    mensagem = agente_3_formatar(assinatura_atual, res_ag2, 1)

                    incidents_table.insert({
                        '_id': f"inc_{uuid.uuid4().hex[:6]}",
                        'status': 'ANALISADO',
                        'assinatura_do_erro': assinatura_atual,
                        'total_de_erros': 1,
                        'historico_temporal': [{"periodo": agora, "ocorrencias": 1}],
                        'analise_da_IA': res_ag2,
                        'integracao_saida': {
                            'slack': mensagem,
                            'jira': "pendente"
                        }
                    })

                # Atualiza Log Bruto como processado
                logs_table.update({'processado_pelo_agente_1': True}, Query()._id == log['_id'])
                progress_bar.progress((i + 1) / len(logs_nao_proc))
            
            progress_bar.empty()
            st.success("Processamento concluído com sucesso.")
            st.rerun()

    st.divider()
    incidentes = incidents_table.all()
    if incidentes:
        for inc in incidentes[-5:]:
            st.markdown(f"**{inc['analise_da_IA']['severidade']} | {inc['assinatura_do_erro']}**")
            st.info(inc['integracao_saida']['slack'])

elif menu == "Dashboard":
    st.header("Dashboard de Operações")
    c1, c2, c3 = st.columns(3)
    c1.metric("Logs Pendentes", len(logs_table.search(Query().processado_pelo_agente_1 == False)))
    c2.metric("Incidentes Únicos", len(incidents_table.all()))
    
    total = sum(inc.get('total_de_erros', 1) for inc in incidents_table.all())
    c3.metric("Taxa de Compressão (Logs -> Incidentes)", f"{total} ➔ {len(incidents_table.all())}")

    st.divider()
    incidentes = incidents_table.all()
    if incidentes:
        df_inc = pd.DataFrame([{
            'ID': inc['_id'],
            'Assinatura': inc['assinatura_do_erro'],
            'Severidade': inc['analise_da_IA']['severidade'],
            'Volume': inc['total_de_erros'],
            'Squad': inc['analise_da_IA']['squad_responsavel']
        } for inc in incidentes[-10:]])
        st.dataframe(df_inc, use_container_width=True)

elif menu == "CRUD Geral":
    st.header("Administração do Banco de Dados")
    
    col_view, col_action = st.columns([2, 1])
    
    with col_view:
        colecao = st.selectbox("Escolha a coleção para visualizar", ["logs_brutos", "contexto_negocio", "incidentes"])
        dados = db.table(colecao).all()
        if dados: 
            st.json(dados)
        else: 
            st.info(f"Nenhum dado encontrado na coleção: {colecao}")
            
    with col_action:
        st.subheader("Ações")
        if st.button("Resetar e Criar Dados de Exemplo"):
            logs_table.truncate()
            context_table.truncate()
            incidents_table.truncate()
            
            context_table.insert({'_id': 'ctx_1', 'nome_do_servico': 'api-pagamentos-v2', 'squad_responsavel': 'squad-checkout', 'criticidade': 'ALTA'})
            context_table.insert({'_id': 'ctx_2', 'nome_do_servico': 'api-usuarios', 'squad_responsavel': 'squad-auth', 'criticidade': 'ALTA'})
            st.success("Banco formatado na nova estrutura."); st.rerun()
            
        st.divider()
        st.subheader("Deletar Registro")
        id_para_deletar = st.text_input("ID do Documento (ex: ctx_1)")
        if st.button("Deletar"):
            if id_para_deletar:
                q = Query()
                resultado = db.table(colecao).remove(q._id == id_para_deletar)
                if resultado:
                    st.success(f"Documento {id_para_deletar} removido com sucesso!")
                    st.rerun()
                else:
                    st.error("ID não encontrado nesta coleção.")
            else:
                st.warning("Insira um ID válido.")