import streamlit as st
import pandas as pd
from datetime import datetime
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import io
import mysql.connector
import openpyxl
import pytz
from time import sleep
import urllib.parse

# Configuração da página para ocupar mais espaço na tela
st.set_page_config(page_title="Datas de Corte e Lançamento", layout="wide")

# Load environment variables from .env
load_dotenv()

import streamlit as st
from sqlalchemy import create_engine
import urllib.parse


def init_db_engine():
    try:
        db = st.secrets["supabase"]
        # O quote_plus garante que mesmo que a senha tenha algo estranho, ela seja lida
        password = urllib.parse.quote_plus(db['password'])

        conn_str = f"postgresql+psycopg2://{db['user']}:{password}@{db['host']}:{db['port']}/{db['database']}"

        return create_engine(conn_str)
    except Exception as e:
        st.error(f"Erro na configuração dos segredos: {e}")
        return None

# Atualize a função de leitura para usar a Engine
@st.cache_data(ttl=120)
def carregar_dados_do_banco():
    """Lê os dados usando a Engine (Thread-safe)"""

    # Pega a engine do cache (seguro compartilhar)
    engine = init_db_engine()

    try:
        # Pandas adora Engine! Ele gerencia a conexão sozinho (abre, lê, fecha)
        # Isso resolve o Warning e o Segmentation Fault
        df = pd.read_sql('SELECT * FROM tabela_corte', engine)

        # Seus tratamentos continuam iguais...
        cols_datas = ['Data de Corte', 'Data de Lançamento']

        # Padronização de nomes (caso precise)
        mapa_colunas = {
            'Data_Corte': 'Data de corte',
            'Data_Lancamento': 'Data de lançamento',
            'Data de Lancamento': 'Data de lançamento'
        }
        df = df.rename(columns=mapa_colunas)

        for col in cols_datas:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)

        # mês atual e próximo mês
        # mapa de meses
        mapa_meses = {
            1: 'JANEIRO',
            2: 'FEVEREIRO',
            3: 'MARÇO',
            4: 'ABRIL',
            5: 'MAIO',
            6: 'JUNHO',
            7: 'JULHO',
            8: 'AGOSTO',
            9: 'SETEMBRO',
            10: 'OUTUBRO',
            11: 'NOVEMBRO',
            12: 'DEZEMBRO'
        }

        excecoes = ['PINDARÉ-MIRIM', 'ITAPECURU-MIRIM']
        excecoes_compt_anterior = ['PREF. BARBACENA']

        # cria colunas auxiliares
        df['Data de Corte'] = pd.to_datetime(df['Data de Corte'], errors='coerce', dayfirst=True)
        df['mes_base'] = df['Data de Corte'].dt.month
        df['dia'] = df['Data de Corte'].dt.day

        # lógica do próximo mês (já trata virada de ano)
        df['mes_referencia'] = df['mes_base']
        df.loc[df['dia'] >= 21, 'mes_referencia'] = df['mes_base'] + 1

        # Exceções → sempre mês ATUAL
        mask_excecoes_anterior = df['Convênio'].isin(excecoes_compt_anterior)
        df.loc[mask_excecoes_anterior, 'mes_referencia'] = df.loc[mask_excecoes_anterior, 'mes_base']

        # ajuste dezembro → janeiro
        df.loc[df['mes_referencia'] == 13, 'mes_referencia'] = 1

        # exceções → sempre mês seguinte
        mask_excecoes = df['Convênio'].isin(excecoes)
        df.loc[mask_excecoes, 'mes_referencia'] = df.loc[mask_excecoes, 'mes_base'] + 1



        # ajustar novamente virada de ano para exceções
        df.loc[df['mes_referencia'] == 13, 'mes_referencia'] = 1

        # converter para nome do mês
        df['Referência'] = df['mes_referencia'].map(mapa_meses)

        # opcional: remover colunas auxiliares
        df.drop(columns=['mes_base', 'dia', 'mes_referencia'], inplace=True)

        return df

    except Exception as e:
        # Se tabela não existe
        if "1146" in str(e):
            return pd.DataFrame()
        else:
            st.error(f"Erro ao carregar dados: {e}")
            return pd.DataFrame()

def get_hora_brasilia():
    fuso = pytz.timezone('America/Sao_Paulo')
    # O segredo é começar pelo ANO (%Y)
    return datetime.now(fuso).strftime('%Y-%m-%d %H:%M:%S')

def salvar_no_banco(df, nome_tabela='tabela_corte'):
    st.write("🕵️‍♂️ Iniciando atualização inteligente (Upsert)...")
    engine = init_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # 1. Horário da alteração
        agora = get_hora_brasilia()

        # 2. Limpeza de duplicatas na planilha antes de subir
        df_limpo = df.drop_duplicates(subset=['Convênio'])
        df_limpo = df_limpo.where(pd.notnull(df_limpo), None)

        # 3. Query de UPSERT (Insere se novo, Atualiza se existir)
        # O segredo está no "ON DUPLICATE KEY UPDATE"
        query = text("""
            INSERT INTO tabela_corte (
                "Convênio", "Sistema", "Responsavel", "Validação", 
                "Referência", "Data de Corte", "Data de Lançamento", "Alterado em"
            )
            VALUES (:conv, :sis, :resp, :val, :ref, :dt_c, :dt_l, :alt)
            ON CONFLICT ("Convênio") 
            DO UPDATE SET
                "Sistema" = EXCLUDED."Sistema",
                "Responsavel" = EXCLUDED."Responsavel",
                "Validação" = EXCLUDED."Validação",
                "Referência" = EXCLUDED."Referência",
                "Data de Corte" = EXCLUDED."Data de Corte",
                "Data de Lançamento" = EXCLUDED."Data de Lançamento",
                "Alterado em" = CASE 
                    WHEN (tabela_corte."Sistema" IS DISTINCT FROM EXCLUDED."Sistema" OR
                          tabela_corte."Responsavel" IS DISTINCT FROM EXCLUDED."Responsavel" OR
                          tabela_corte."Validação" IS DISTINCT FROM EXCLUDED."Validação" OR
                          tabela_corte."Referência" IS DISTINCT FROM EXCLUDED."Referência" OR
                          tabela_corte."Data de Corte" IS DISTINCT FROM EXCLUDED."Data de Corte" OR
                          tabela_corte."Data de Lançamento" IS DISTINCT FROM EXCLUDED."Data de Lançamento")
                    THEN EXCLUDED."Alterado em"
                    ELSE tabela_corte."Alterado em"
                END;
        """)

        def limpar_data(valor):
            if pd.isna(valor) or str(valor) == 'NaT':
                return None
            return valor

        # 4. Execução
        for _, row in df_limpo.iterrows():
            session.execute(query, {
                "conv": row.get('Convênio'),
                "sis": row.get('Sistema'),
                "resp": row.get('Responsavel'),
                "val": row.get('Validação'),
                "ref": row.get('Referência'),
                "dt_c": limpar_data(row.get('Data de Corte')),
                "dt_l": limpar_data(row.get('Data de Lançamento')),
                "alt": agora
            })

        session.commit()
        st.success(f"✅ Sincronização concluída! {len(df_limpo)} convênios processados.")
        st.cache_data.clear()
        return True

    except Exception as e:
        session.rollback()
        st.error(f"❌ Erro na sincronização: {e}")
        return False
    finally:
        session.close()


def salvar_edicoes_cirurgicas(df_editado, df_original, df_filtrado_antes_da_edicao):
    engine = init_db_engine()
    agora = get_hora_brasilia()

    with engine.connect() as conn:
        with conn.begin():
            # 1. DELEÇÃO (Mantida)
            ids_que_estavam_na_tela = set(df_filtrado_antes_da_edicao['id'].dropna().astype(int).tolist())
            ids_que_ficaram_apos_edicao = set(df_editado['id'].dropna().astype(int).tolist())
            ids_para_deletar = ids_que_estavam_na_tela - ids_que_ficaram_apos_edicao

            if ids_para_deletar:
                format_ids = ", ".join(map(str, ids_para_deletar))
                conn.execute(text(f"DELETE FROM tabela_corte WHERE id IN ({format_ids})"))

            # 2. UPDATE E INSERT
            for i, row in df_editado.iterrows():
                # --- TRATAMENTO SEGURO DE DATAS ---
                # Forçamos a data a virar uma string no formato ISO (AAAA-MM-DD)
                # Isso impede o MySQL de inverter dia com mês
                def formatar_data_sql(valor):
                    if pd.isna(valor): return None
                    try:
                        return pd.to_datetime(valor).strftime('%Y-%m-%d')
                    except:
                        return None

                dt_corte = formatar_data_sql(row.get('Data de Corte'))
                dt_lanca = formatar_data_sql(row.get('Data de Lançamento'))

                params = {
                    "conv": None if pd.isna(row.get('Convênio')) else row.get('Convênio'),
                    "sis": None if pd.isna(row.get('Sistema')) else row.get('Sistema'),
                    "resp": None if pd.isna(row.get('Responsavel')) else row.get('Responsavel'),
                    "val": None if pd.isna(row.get('Validação')) else row.get('Validação'),
                    "ref": None if pd.isna(row.get('Referência')) else row.get('Referência'),
                    "dt_c": dt_corte,
                    "dt_l": dt_lanca,
                    "alt": agora
                }

                # CASO A: INSERT
                if pd.isna(row.get('id')):
                    query_insert = text("""
                        INSERT INTO tabela_corte (
                            Convênio, Sistema, Responsavel, Validação, Referência, 
                            `Data de Corte`, `Data de Lançamento`, `Alterado em`
                        ) VALUES (
                            :conv, :sis, :resp, :val, :ref, :dt_c, :dt_l, :alt
                        )
                    """)
                    conn.execute(query_insert, params)

                # CASO B: UPDATE
                else:
                    id_atual = int(row['id'])
                    linha_original = df_original[df_original['id'] == id_atual]

                    if not linha_original.empty:
                        # Comparamos apenas as colunas relevantes para ver se mudou
                        if not row.equals(linha_original.iloc[0]):
                            params["id"] = id_atual
                            query_update = text("""
                                UPDATE tabela_corte SET 
                                Convênio=:conv, Sistema=:sis, Responsavel=:resp,
                                Validação=:val, Referência=:ref, `Data de Corte`=:dt_c, 
                                `Data de Lançamento`=:dt_l, `Alterado em`=:alt
                                WHERE id=:id
                            """)
                            conn.execute(query_update, params)

    st.cache_data.clear()
    st.success("✅ Alterações salvas com sucesso!")
    # sleep(1)
    st.rerun()

def tratar_planilha(uploaded_file):
    """
    Função que lê o Excel e aplica a lógica de limpeza das células mescladas.
    """
    # Lê o arquivo. O header=None ajuda a detectar as linhas mescladas antes do cabeçalho real,
    # mas assumindo que a estrutura é padrão, vamos ler normal e tratar depois.
    # DICA: Dependendo de como a planilha começa, pode ser necessário ajustar o 'header'.
    # Aqui vou assumir que a primeira linha já tem dados ou o título.
    df = pd.read_excel(uploaded_file)

    # Lógica para tratar as categorias (FEDERAL, ESTADUAL, MUNICIPAL)
    # 1. Criamos uma coluna nova chamada 'Esfera'
    # 2. Identificamos as linhas separadoras.
    # Geralmente, nessas linhas, a coluna 'Convênio' tem o texto (ex: FEDERAL)
    # e as outras colunas (como Validador) estão vazias (NaN).

    # Lista de palavras-chave para identificar os separadores
    palavras_chave = ['FEDERAL', 'ESTADUAL', 'MUNICIPAL', 'Governos']

    # Vamos iterar para identificar onde estão esses cabeçalhos
    # Nota: Se a planilha for muito grande, existem métodos vetoriais mais rápidos,
    # mas este é mais fácil de entender e manter.

    current_esfera = "Indefinido"

    # Lista para marcar quais linhas vamos deletar (as linhas de cabeçalho mesclado)
    indices_para_remover = []

    for index, row in df.iterrows():
        valor_coluna_conv = row['Convênio']

        # --- MUDANÇA AQUI ---
        # Agora verificamos DUAS coisas:
        # 1. Se tem a palavra chave
        # Só verifica se for texto, senão considera Falso
        if isinstance(valor_coluna_conv, str):
            tem_palavra_chave = any(p in valor_coluna_conv for p in palavras_chave)
        else:
            tem_palavra_chave = False

        # 2. Se as outras colunas importantes estão vazias (NaN ou NaT ou string vazia)
        # Vamos checar a coluna "Validador" e "Data de corte" como exemplo.
        # pd.isna() retorna True se for vazio/NaN
        outras_colunas_vazias = row['Validação'] in palavras_chave

        # A linha só é um SEPARADOR se tiver a palavra E o resto for vazio
        eh_separador = tem_palavra_chave and outras_colunas_vazias
        # --------------------

        if eh_separador:
            indices_para_remover.append(index)

    # 3. Removemos as linhas que eram apenas separadores
    df_clean = df.drop(indices_para_remover)

    # 4. Removemos linhas vazias se houver
    df_clean = df_clean.dropna(subset=['Convênio'])

    # 5. Garantir que as colunas de data sejam datetime para permitir ordenação correta
    col_origem_corte = next((c for c in df_clean.columns if 'Data corte' in c), None)
    col_origem_lanc = next((c for c in df_clean.columns if 'Data lançamento' in c), None)

    col_atualiza_corte = next((c for c in df_clean.columns if 'Data de Corte' in c), None)
    col_atualiza_lanc = next((c for c in df_clean.columns if 'Data de Lançamento' in c), None)

    # 2. Verifica se encontrou as duas colunas
    if col_origem_corte and col_origem_lanc:
        # 3. Faz o rename usando os nomes que encontramos
        df_clean = df_clean.rename(columns={
            col_origem_corte: 'Data de Corte',  # Padronizado
            col_origem_lanc: 'Data de Lançamento'  # Padronizado
        })
    elif col_atualiza_corte and col_atualiza_lanc:
        # 3. Faz o rename usando os nomes que encontramos
        df_clean = df_clean.rename(columns={
            col_origem_corte: 'Data de Corte',  # Padronizado
            col_origem_lanc: 'Datade Lançamento'  # Padronizado
        })
    else:
        print('Alguma das colunas ("Data de corte" ou "Data de lançamento") não se encontra na planilha')
        print(f'colunas de datas de corte\n{df_clean.columns}')
        return False  # ou return apenas

    cols_data = ['Data de Lançamento', 'Data de Corte']
    for col in cols_data:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce', dayfirst=True)

    return df_clean


def to_excel(df):
    """Função auxiliar para converter DF para Excel em memória para download"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Tratada')
    processed_data = output.getvalue()
    return processed_data


# --- INTERFACE DO STREAMLIT ---

st.title("📂 Sistema Compartilhado de Convênios")

# --- FUNÇÃO PARA LIMPAR (Coloque isso antes do sidebar ou no topo do script) ---
def limpar_tudo():
    st.session_state['f_convenio'] = []
    st.session_state['f_sistema'] = []
    st.session_state['f_resp'] = []
    st.session_state['f_validacao'] = []
    st.session_state['f_data_lanc'] = None
    st.session_state['f_data_corte'] = None

# --- BARRA LATERAL ---
with st.sidebar:
    # --- BOTÃO DE TEMA ---
    st.header("⚙️ Administração")
    uploaded_file = st.file_uploader("Subir nova planilha", type=['xlsx', 'xls'])

    if uploaded_file is not None:
        # O botão de ação
        if st.button("Processar e Salvar"):

            with st.spinner("Lendo arquivo e enviando para o TiDB..."):
                try:
                    # 1. SEGURANÇA: Reseta o ponteiro do arquivo para o início
                    uploaded_file.seek(0)

                    # 2. Processamento
                    df_tratado = tratar_planilha(uploaded_file)

                    # 3. Salvamento com verificação real
                    # A função salvar_no_banco retorna True ou False, vamos usar isso!
                    sucesso = salvar_no_banco(df_tratado)

                    if sucesso:
                        st.success("✅ Dados atualizados com sucesso!")
                        # Espera 2 segundinhos para você ver a mensagem verde antes de sumir
                        sleep(2)
                        # Limpa o cache para o gráfico novo aparecer
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("❌ Ocorreu um erro ao salvar no banco. Verifique os logs.")

                except Exception as e:
                    st.error(f"Erro crítico no processamento: {e}")

    st.divider()

    # --- AQUI ENTRAM OS SEUS FILTROS ---
    st.header("🔍 Filtros de Visualização")

    # Dica de Performance: Carregue os dados uma vez só numa variável
    df_banco = carregar_dados_do_banco()

    # --- TRAVA DE SEGURANÇA ---
    # Se o banco estiver vazio, interrompemos a construção dos filtros para não dar erro
    if df_banco.empty:
        st.info("ℹ️ Nenhuma base de dados carregada no momento.")
        # O st.stop() faz o Streamlit parar de ler o código daqui pra baixo (na sidebar)
        # Isso evita que ele tente ler colunas que não existem.
        st.stop()

        # --- SE PASSOU DA TRAVA, SEGUE O BAILE ---

    convenios_filtro = st.multiselect(
        "Filtrar Convênios:",
        options=df_banco['Convênio'].unique(),
        key='f_convenio'
    )

    sistema_filtro = st.multiselect(
        "Filtra Sistemas:",
        options=df_banco['Sistema'].unique(),
        key='f_sistema'
    )

    responsavel_filtro = st.multiselect(
        "Responsável:",
        options=df_banco['Responsavel'].unique(),
        key='f_resp'
    )

    validacao_filtro = st.multiselect(
        "Validador:",
        options=df_banco['Validação'].unique(),
        key='f_validacao'
    )

    # 2. Seus filtros de Data
    data_filtro_lancamento = st.date_input(
        "Data de Lançamento exata:",
        value=None,
        format="DD/MM/YYYY",
        key='f_data_lanc'
    )

    data_filtro_corte = st.date_input(
        "Data de Corte exata:",
        value=None,
        format="DD/MM/YYYY",
        key='f_data_corte'
    )

    # O botão chama a função ANTES de rodar o app de novo
    st.button("Limpar Filtros", on_click=limpar_tudo)

# --- ÁREA PRINCIPAL ---
st.subheader("Visualização da Base de Dados")

# 1. Carrega do Banco
df_base_original = carregar_dados_do_banco()



if not df_base_original.empty:

    # --- SEUS FILTROS DE DATA AQUI ---
    df_visualizacao = df_base_original.copy()

    # --- NOVIDADE: TABELA DE "HOJE" ---
    # Pegamos a data atual do sistema
    hoje = datetime.now().date()

    # Filtramos: Mostra se a data de corte OU a data de lançamento for HOJE
    # Usamos .dt.date para garantir que estamos comparando apenas dia/mês/ano (ignorando horas)
    print(f'df_visualizacao:\n{df_visualizacao.columns}')

    df_visualizacao['Data de Lançamento'] = pd.to_datetime(
        df_visualizacao['Data de Lançamento'], errors='coerce', dayfirst=True
    )
    df_visualizacao['Data de Corte'] = pd.to_datetime(
        df_visualizacao['Data de Corte'], errors='coerce', dayfirst=True
    )

    df_alertas_corte = df_visualizacao.loc[
        df_visualizacao['Data de Lançamento'].notna()
        & df_visualizacao['Data de Corte'].notna()
        & (df_visualizacao['Data de Lançamento'] > df_visualizacao['Data de Corte'])
        ].copy()

    # ALERTA 2: lançamento no fim de semana
    df_alertas_fds = df_visualizacao.loc[
        df_visualizacao['Data de Lançamento'].notna()
        & (df_visualizacao['Data de Lançamento'].dt.dayofweek >= 5)
        ].copy()

    total_alertas = len(df_alertas_corte) + len(df_alertas_fds)

    col_esq, col_dir = st.columns([8, 2])

    with col_dir:
        if total_alertas > 0:
            st.toast(
                f"Há {total_alertas} alerta(s) de data para verificar.",
                icon="🔔"
            )

            with st.popover(f"🔔 Alertas ({total_alertas})", use_container_width=True):
                if not df_alertas_corte.empty:
                    st.warning("Convênios com Data de Lançamento após a Data de Corte")
                    for _, row in df_alertas_corte.iterrows():
                        st.write(
                            f"**{row['Convênio']}**: "
                            f"{row['Data de Lançamento'].strftime('%d/%m/%Y')} > "
                            f"{row['Data de Corte'].strftime('%d/%m/%Y')}"
                        )

                if not df_alertas_fds.empty:
                    st.warning("⚠️ Convênios com Data de Lançamento em fim de semana")

                    # Dicionário de tradução (muito mais rápido que vários ifs)
                    dias_traduzidos = {
                        "Monday": "Segunda-feira", "Tuesday": "Terça-feira",
                        "Wednesday": "Quarta-feira", "Thursday": "Quinta-feira",
                        "Friday": "Sexta-feira", "Saturday": "Sábado", "Sunday": "Domingo"
                    }

                    # Criamos uma lista de strings para mostrar tudo de uma vez
                    linhas_alerta = []
                    for _, row in df_alertas_fds.iterrows():
                        nome_ingles = row['Data de Lançamento'].day_name()
                        dia_pt = dias_traduzidos.get(nome_ingles, nome_ingles)
                        data_fmt = row['Data de Lançamento'].strftime('%d/%m/%Y')

                        linhas_alerta.append(f"* **{row['Convênio']}**: {data_fmt} ({dia_pt})")

                    # Mostra tudo em um bloco só (melhor performance)
                    st.markdown("\n".join(linhas_alerta))
        else:
            st.caption("🔔 Sem alertas")

    filtro_lancamento_hoje = (
            df_visualizacao['Data de Lançamento'].dt.date == hoje
    )

    filtro_corte_hoje = (df_visualizacao['Data de Corte'].dt.date == hoje)

    filtro_lancando_ainda = (
            (df_visualizacao['Data de Lançamento'].dt.date <= hoje) &
            (df_visualizacao['Data de Corte'].dt.date >= hoje)
    )

    df_lancamento_hoje = df_visualizacao[filtro_lancamento_hoje]

    df_corte_hoje = df_visualizacao[filtro_corte_hoje]

    df_lancando_ainda = df_visualizacao[filtro_lancando_ainda]

    # --- INTERFACE POR ABAS ---
    st.subheader(f"📅 Pendências de Hoje ({hoje.strftime('%d/%m/%Y')})")

    # Criamos as duas abas
    tab_lancamentos, tab_cortes, tab_lancando = st.tabs(["🚀 Lançamentos de Hoje", "✂️ Cortes de Hoje", "⚠️ Em Período de Lançamento"])

    # Botão de abrir página do Doug
    st.markdown("""
    <a href="https://lembrete-lancamentos.netlify.app/" target="_blank">
        <button>🔗 Confirme seus lançamentos</button>
    </a>
    """, unsafe_allow_html=True)

    # Selecionamos apenas as colunas que você pediu
    # Nota: Certifique-se que o nome da coluna é "Convênios" (plural) ou "Convênio" (singular) conforme sua planilha
    colunas_resumo = ['Convênio', 'Data de Corte', 'Data de Lançamento', 'Responsavel', 'Validação']

    # Verifica se as colunas existem antes de tentar mostrar (pra evitar erro se a planilha mudar)
    cols_existentes = [c for c in colunas_resumo if c in df_lancamento_hoje.columns]
    df_hoje_resumo = df_lancamento_hoje[cols_existentes]
    df_corte_resumo = df_corte_hoje[cols_existentes]
    df_lancando_resumo = df_lancando_ainda[cols_existentes]

    with tab_lancamentos:
        # Exibe o alerta
        if not df_hoje_resumo.empty:
            st.success(
                f"📅 **Atenção: Existem {len(df_hoje_resumo)} convênios para tratar hoje ({hoje.strftime('%d/%m/%Y')})!**")
            st.dataframe(
                df_hoje_resumo,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Data de Corte": st.column_config.DateColumn("Data de Corte", format="DD/MM/YYYY"),
                    "Data de Lançamento": st.column_config.DateColumn("Data de Lançamento", format="DD/MM/YYYY"),
                }
            )
        else:
            st.info(f"✅ Nenhuma pendência de lançamento para hoje ({hoje.strftime('%d/%m/%Y')}).")
    with tab_cortes:
        # Exibe o alerta
        if not df_corte_resumo.empty:
            st.success(
                f"📅 **Atenção: Existem {len(df_corte_resumo)} convênios que cortam hoje ({hoje.strftime('%d/%m/%Y')})!**")
            st.dataframe(
                df_corte_resumo,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Data de Corte": st.column_config.DateColumn("Data de Corte", format="DD/MM/YYYY"),
                    "Data de Lançamento": st.column_config.DateColumn("Data de Lançamento", format="DD/MM/YYYY"),
                }
            )
        else:
            st.info(f"✅ Nenhuma pendência de corte para hoje ({hoje.strftime('%d/%m/%Y')}).")

    with tab_lancando:
        # Exibe o alerta
        if not df_lancando_resumo.empty:
            st.success(
                f"📅 **Atenção: Existem {len(df_lancando_resumo)} convênios em período de lançamento ({hoje.strftime('%d/%m/%Y')})!**")
            st.dataframe(
                df_lancando_resumo,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Data de Corte": st.column_config.DateColumn("Data de Corte", format="DD/MM/YYYY"),
                    "Data de Lançamento": st.column_config.DateColumn("Data de Lançamento", format="DD/MM/YYYY"),
                }
            )


    st.divider()  # Uma linha para separar o resumo da tabela completa



    # --- TABELA COMPLETA E FILTROS (CÓDIGO ANTERIOR) ---
    st.subheader("Base Geral Completa")

    # 2. Aplica a Lógica dos Filtros

    # Filtro de convênios
    if convenios_filtro:
        df_visualizacao = df_visualizacao[df_visualizacao['Convênio'].isin(convenios_filtro)]

    # Filtro de sistemas
    if sistema_filtro:
        df_visualizacao = df_visualizacao[df_visualizacao['Sistema'].isin(sistema_filtro)]

    # Filtro dos responsáveis
    if responsavel_filtro:
        df_visualizacao = df_visualizacao[df_visualizacao['Responsavel'].isin(responsavel_filtro)]

    # Filtro dos validadores
    if validacao_filtro:
        df_visualizacao = df_visualizacao[df_visualizacao['Validação'].isin(validacao_filtro)]

    # Filtro de Data de Lançamento
    if data_filtro_lancamento:
        # Precisamos usar .dt.date para comparar Data (input) com Timestamp (pandas)
        df_visualizacao = df_visualizacao[df_visualizacao['Data de Lançamento'].dt.date == data_filtro_lancamento]

    # Filtro de Data de Corte
    if data_filtro_corte:
        df_visualizacao = df_visualizacao[df_visualizacao['Data de Corte'].dt.date == data_filtro_corte]

    # No seu código principal:
    df_antes_de_editar = df_visualizacao.copy()  # Salva o estado do filtro

    df_editado = st.data_editor(
        df_visualizacao,
        hide_index=True,
        column_config={
            "id": None,
            "Data de Corte": st.column_config.DateColumn("Data de Corte", format="DD/MM/YYYY"),
            "Data de Lançamento": st.column_config.DateColumn("Data de Lançamento", format="DD/MM/YYYY"),
            "Alterado em": st.column_config.DatetimeColumn("Alterado em",format="DD/MM/YYYY HH:mm:ss")
        },
        use_container_width=True,
        num_rows="dynamic"
    )

    # 1. Criar um buffer na memória
    buffer = io.BytesIO()

    df_sem_id = df_visualizacao.copy()
    df_sem_id['Data de Corte'] = df_sem_id['Data de Corte'].dt.strftime('%d/%m/%Y')
    df_sem_id['Data de Lançamento'] = df_sem_id['Data de Lançamento'].dt.strftime('%d/%m/%Y')
    if 'id' in df_sem_id.columns:
        df_sem_id = df_sem_id.drop(columns=['id'])
    if 'Alterado em' in df_sem_id.columns:
        df_sem_id = df_sem_id.drop(columns=['Alterado em'])
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df_sem_id.to_excel(writer, index=False, sheet_name='Acessos')

    st.caption(f"Mostrando {len(df_visualizacao)} registros encontrados.")

    # Botão de Download
    st.download_button(
        label="📥 Baixar Dados Filtrados",
        data=buffer.getvalue(),
        file_name="relatorio_filtrado.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    # --- PARTE FINAL DO CÓDIGO ---
    if st.button("💾 Salvar Alterações", type="primary"):
        # Chamamos a função passando o que está na tela (editado)
        # e o que veio do banco (original) para comparação
        salvar_edicoes_cirurgicas(df_editado, df_base_original, df_antes_de_editar)
        st.success("Alterações salvas com sucesso!")

else:
    st.info("O banco de dados está vazio. Use a barra lateral para fazer o primeiro upload.")