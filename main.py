import streamlit as st
import pandas as pd
from datetime import datetime
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
import io
import mysql.connector
import openpyxl
import pytz
from time import sleep

# Configuração da página para ocupar mais espaço na tela
st.set_page_config(page_title="Datas de Corte e Lançamento", layout="wide")


@st.cache_data(ttl=600)
def init_db_engine():
    # Pega os dados
    user = st.secrets["mysql"]["user"]
    password = st.secrets["mysql"]["password"]
    host = st.secrets["mysql"]["host"]
    port = st.secrets["mysql"]["port"]
    database = st.secrets["mysql"]["database"]

    # Cria a Engine com Pool de conexões
    # pool_size=5: Mantém 5 conexões abertas prontas pra uso
    # max_overflow=10: Pode abrir mais 10 se tiver muito tráfego
    return create_engine(
        f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}",
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,  # Evita erro de conexão perdida
        pool_recycle=3600
    )


# Atualize a função de leitura para usar a Engine
def carregar_dados_do_banco():
    """Lê os dados usando a Engine (Thread-safe)"""

    # Pega a engine do cache (seguro compartilhar)
    engine = init_db_engine()

    try:
        # Pandas adora Engine! Ele gerencia a conexão sozinho (abre, lê, fecha)
        # Isso resolve o Warning e o Segmentation Fault
        df = pd.read_sql('SELECT * FROM tabela_corte', engine)

        # Seus tratamentos continuam iguais...
        cols_datas = ['Data de corte', 'Data de lançamento']

        # Padronização de nomes (caso precise)
        mapa_colunas = {
            'Data_Corte': 'Data de corte',
            'Data_Lancamento': 'Data de lançamento',
            'Data de Lancamento': 'Data de lançamento'
        }
        df = df.rename(columns=mapa_colunas)

        for col in cols_datas:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

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
    return datetime.now(fuso).strftime('%Y-%m-%d %H:%M:%S')

def salvar_no_banco(df, nome_tabela='tabela_corte'):
    st.write("🕵️‍♂️ Iniciando processo de salvamento...")
    # 1. Inicializa a Engine
    engine = init_db_engine()

    # 2. Cria uma fábrica de sessões
    Session = sessionmaker(bind=engine)
    session = Session()
    st.write("⚙️ Engine criada. Tentando enviar dados...")

    try:
        # 1. Conferindo as credenciais (sem mostrar a senha)
        user = st.secrets["mysql"]["user"]
        host = st.secrets["mysql"]["host"]
        port = st.secrets["mysql"]["port"]
        database = st.secrets["mysql"]["database"]

        st.write(f"📡 Tentando conectar em: {host} (Banco: {database})")

        # 2. Montando a string
        password = st.secrets["mysql"]["password"]
        conexao_str = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"

        # 3. Criando Engine


        # AQUI MUDOU TUDO:
        # Abrimos uma conexão explícita gerenciada
        # 3. Limpa a tabela atual
        # O SQLAlchemy exige que comandos textuais sejam envolvidos em text()
        session.execute(text("TRUNCATE TABLE tabela_corte;"))

        # Horário de Brasília
        agora = get_hora_brasilia()

        # 4. Remove duplicatas
        df_limpo = df.drop_duplicates(subset=['Convênio'])

        # --- A CORREÇÃO ESTÁ AQUI ---
        # Transformamos o que é "vazio do Excel" em "vazio do Banco"
        df_limpo = df_limpo.where(pd.notnull(df_limpo), None)
        # ----------------------------

        # 5. Loop de Inserção
        for _, row in df_limpo.iterrows():
            query = text("""
                        INSERT INTO tabela_corte (
                            Convênio, Sistema, Responsavel, Validação, 
                            Referência, `Data de Corte`, `Data de Lançamento`, `Alterado em`
                        )
                        VALUES (:conv, :sis, :resp, :val, :ref, :dt_c, :dt_l, :alt)
                    """)

            # Função auxiliar rápida para limpar NaT
            def limpar_data(valor):
                if pd.isna(valor) or str(valor) == 'NaT':
                    return None
                return valor

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

        # 6. Salva as alterações definitivamente
        session.commit()

        st.success(f"✅ Base atualizada com {len(df_limpo)} registros!")
        st.cache_data.clear()
        return True

    except Exception as e:
        # Se der erro em qualquer parte, desfaz tudo (rollback)
        session.rollback()
        st.error(f"❌ Erro ao salvar no banco: {e}")
        return False

    finally:
        # Fecha a sessão para liberar a conexão de volta para o Pool
        session.close()


def salvar_edicoes_cirurgicas(df_editado, df_original, df_filtrado_antes_da_edicao):
    """Atualiza apenas as células modificadas comparando os DataFrames"""
    engine = init_db_engine()
    # Horário de Brasília
    agora = get_hora_brasilia()

    with engine.connect() as conn:
        with conn.begin():
            # --- CORREÇÃO DO BUG DE DELEÇÃO ---
            # Só podemos deletar o que estava VISÍVEL e o usuário removeu.
            # Se algo não estava na tela por causa do filtro, não pode ser deletado!

            ids_que_estavam_na_tela = set(df_filtrado_antes_da_edicao['id'].tolist())
            ids_que_ficaram_apos_edicao = set(df_editado['id'].dropna().tolist())

            # Deletamos apenas o que estava na tela e "sumiu"
            ids_para_deletar = ids_que_estavam_na_tela - ids_que_ficaram_apos_edicao

            if ids_para_deletar:
                format_ids = ", ".join(map(str, ids_para_deletar))
                conn.execute(text(f"DELETE FROM tabela_corte WHERE id IN ({format_ids})"))

            # --- PARTE DE UPDATE (Continua igual) ---
            for i, row in df_editado.iterrows():
                if pd.isna(row.get('id')): continue

                # Buscamos a linha original para comparar se houve mudança
                linha_orig = df_original[df_original['id'] == row['id']].iloc[0]

                if not row.equals(linha_orig):
                    query = text("""
                        UPDATE tabela_corte SET 
                        Convênio=:conv, Sistema=:sis, Responsavel=:resp,
                        Validação=:val, Referência=:ref, `Data de Corte`=:dt_c, `Data de Lançamento`=:dt_l, `Alterado em`=:alt
                        WHERE id=:id
                    """)
                    conn.execute(query, {
                        "conv": row['Convênio'], "sis": row['Sistema'],
                        "resp": row['Responsavel'], "val": row['Validação'], "ref": row['Referência'],
                        "dt_c": row['Data de Corte'], "dt_l": row['Data de Lançamento'], "alt": agora,
                        "id": row['id']
                    })

    st.cache_data.clear()
    st.success("✅ Alterações salvas com sucesso!")
    sleep(1)
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
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')

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

    df_visualizacao['Data de Lançamento'] = pd.to_datetime(df_visualizacao['Data de Lançamento'], errors='coerce')
    df_visualizacao['Data de Corte'] = pd.to_datetime(df_visualizacao['Data de Corte'], errors='coerce')

    filtro_lancamento_hoje = (
            df_visualizacao['Data de Lançamento'].dt.date == hoje
    )

    filtro_corte_hoje = (df_visualizacao['Data de Corte'].dt.date == hoje)

    df_lancamento_hoje = df_visualizacao[filtro_lancamento_hoje]

    df_corte_hoje = df_visualizacao[filtro_corte_hoje]

    # --- INTERFACE POR ABAS ---
    st.subheader(f"📅 Pendências de Hoje ({hoje.strftime('%d/%m/%Y')})")

    # Criamos as duas abas
    tab_lancamentos, tab_cortes = st.tabs(["🚀 Lançamentos de Hoje", "✂️ Cortes de Hoje"])

    # Selecionamos apenas as colunas que você pediu
    # Nota: Certifique-se que o nome da coluna é "Convênios" (plural) ou "Convênio" (singular) conforme sua planilha
    colunas_resumo = ['Convênio', 'Data de Corte', 'Data de Lançamento', 'Responsavel', 'Validação']

    # Verifica se as colunas existem antes de tentar mostrar (pra evitar erro se a planilha mudar)
    cols_existentes = [c for c in colunas_resumo if c in df_lancamento_hoje.columns]
    df_hoje_resumo = df_lancamento_hoje[cols_existentes]
    df_corte_resumo = df_corte_hoje[cols_existentes]

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
                f"📅 **Atenção: Existem {len(df_corte_resumo)} convênios para tratar hoje ({hoje.strftime('%d/%m/%Y')})!**")
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
        },
        use_container_width=True,
        num_rows="dynamic"
    )
    # 1. Criar um buffer na memória
    buffer = io.BytesIO()

    df_sem_id = df_visualizacao.copy()
    if 'id' in df_sem_id.columns:
        df_sem_id = df_sem_id.drop(columns=['id'])
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