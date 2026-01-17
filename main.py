import streamlit as st
import pandas as pd
from datetime import datetime
import sqlite3
from sqlalchemy import create_engine
import io
import mysql.connector
import openpyxl
from time import sleep

# Configuração da página para ocupar mais espaço na tela
st.set_page_config(page_title="Datas de Corte e Lançamento", layout="wide")

@st.cache_resource(ttl=600)
def init_connection():
    # Pega os dados do secrets (tanto local quanto na nuvem)
    return mysql.connector.connect(
        host=st.secrets["mysql"]["host"],
        user=st.secrets["mysql"]["user"],
        password=st.secrets["mysql"]["password"],
        database=st.secrets["mysql"]["database"],
        port=st.secrets["mysql"]["port"]
    )

def run_query(query, params=None):
    conn = init_connection()
    # O cursor(dictionary=True) é útil para acessar colunas pelo nome
    with conn.cursor(dictionary=True) as cursor:
        cursor.execute(query, params)
        if query.strip().upper().startswith("SELECT"):
            return cursor.fetchall()
        else:
            conn.commit()
            return None


def carregar_dados_do_banco():
    """Lê os dados salvos no banco para mostrar na tela"""

    # 1. Usa a nova função de conexão que pega os dados do secrets.toml
    # (Certifique-se de usar o mesmo nome que definiu antes: init_connection ou criar_conexao)
    conn = init_connection()

    # --- NOVIDADE: O CHECK-UP DA CONEXÃO ---
    try:
        # Verifica se o servidor responde. Se não, reconecta.
        if not conn.is_connected():
            conn.reconnect(attempts=3, delay=2)
        # O ping garante que o socket está ativo
        conn.ping(reconnect=True, attempts=3, delay=2)
    except Exception:
        # Se deu ruim mesmo, limpa o cache e cria uma do zero
        st.cache_resource.clear()
        conn = init_connection()
    # -----------------------------------------

    try:
        # Lê a tabela.
        # IMPORTANTE: Confirme se o nome da tabela no TiDB é 'lancamentos' ou 'tabela_corte'
        df = pd.read_sql('SELECT * FROM tabela_corte', conn)

        # Converte as colunas de data (ajuste os nomes conforme suas colunas reais)
        cols_data = ['Data de Lancamento', 'Data de Corte']  # Exemplo de nomes sem espaço, padrão SQL

        for col in cols_data:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        return df


    except Exception as e:

        # Se o erro for "Table doesn't exist" (código 1146), a gente finge que não viu

        # e retorna uma tabela vazia, pois é apenas o primeiro acesso.

        if "1146" in str(e):

            return pd.DataFrame()

        else:

            # Se for outro erro (senha, conexão), aí sim mostramos na tela

            st.error(f"Erro ao carregar dados: {e}")

            return pd.DataFrame()


def salvar_no_banco(df, nome_tabela='tabela_corte'):
    st.write("🕵️‍♂️ Iniciando processo de salvamento...")

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
        engine = create_engine(conexao_str)
        st.write("⚙️ Engine criada. Tentando enviar dados...")

        # AQUI MUDOU TUDO:
        # Abrimos uma conexão explícita gerenciada
        with engine.connect() as conn:

            # Tentativa de limpeza preventiva (opcional, mas ajuda no seu caso)
            # Tenta dar um rollback caso tenha algo pendente dessa sessão
            try:
                conn.rollback()
            except:
                pass

            # Iniciamos a transação blindada
            with conn.begin():
                # method='multi' -> Acelera muito o upload (envia várias linhas num comando só)
                # con=conn -> Passamos a conexão aberta, não a engine!
                # 4. Enviando
                df.to_sql(name=nome_tabela, con=conn, if_exists='replace', index=False, chunksize=1000, method='multi')

        st.write("✅ Comando to_sql finalizado!")
        return True


    except Exception as e:
        st.error(f"❌ Erro ao salvar: {e}")
        print(e)
        return False
    finally:
        engine.dispose()


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

    col_atualiza_corte = next((c for c in df_clean.columns if 'Data_Corte' in c), None)
    col_atualiza_lanc = next((c for c in df_clean.columns if 'Data_Lancamento' in c), None)

    # 2. Verifica se encontrou as duas colunas
    if col_origem_corte and col_origem_lanc:
        # 3. Faz o rename usando os nomes que encontramos
        df_clean = df_clean.rename(columns={
            col_origem_corte: 'Data_Corte',  # Padronizado
            col_origem_lanc: 'Data_Lancamento',  # Padronizado
            'Convênio': 'Convenio',
            'Validação': 'Validacao'
        })
    elif col_atualiza_corte and col_atualiza_lanc:
        # 3. Faz o rename usando os nomes que encontramos
        df_clean = df_clean.rename(columns={
            col_origem_corte: 'Data_Corte',  # Padronizado
            col_origem_lanc: 'Data_Lancamento'  # Padronizado
        })
    else:
        print('Alguma das colunas ("Data de corte" ou "Data de lançamento") não se encontra na planilha')
        print(f'colunas de datas de corte\n{df_clean.columns}')
        return False  # ou return apenas

    cols_data = ['Data_Lancamento', 'Data_Corte']
    for col in cols_data:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')

    # Subsitituir letras que podem conter ~, ´, ç
    for colu in df_clean.columns:
        df_clean[colu] = df_clean[colu].str.replace(r'[^a-zA-Z0-9 ]', '', regex=True)

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
        options=df_banco['Convenio'].unique(),
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
        options=df_banco['Validacao'].unique(),
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
df_visualizacao = carregar_dados_do_banco()

if not df_visualizacao.empty:

    # --- SEUS FILTROS DE DATA AQUI ---

    # --- NOVIDADE: TABELA DE "HOJE" ---
    # Pegamos a data atual do sistema
    hoje = datetime.now().date()

    # Filtramos: Mostra se a data de corte OU a data de lançamento for HOJE
    # Usamos .dt.date para garantir que estamos comparando apenas dia/mês/ano (ignorando horas)
    filtro_hoje = (
            df_visualizacao['Data_Lancamento'].dt.date == hoje
    )

    df_hoje = df_visualizacao[filtro_hoje]

    # Selecionamos apenas as colunas que você pediu
    # Nota: Certifique-se que o nome da coluna é "Convênios" (plural) ou "Convênio" (singular) conforme sua planilha
    colunas_resumo = ['Convenio', 'Data_Corte', 'Data_Lancamento', 'Responsavel', 'Validacao']

    # Verifica se as colunas existem antes de tentar mostrar (pra evitar erro se a planilha mudar)
    cols_existentes = [c for c in colunas_resumo if c in df_hoje.columns]
    df_hoje_resumo = df_hoje[cols_existentes]

    # Exibe o alerta
    if not df_hoje_resumo.empty:
        st.success(
            f"📅 **Atenção: Existem {len(df_hoje_resumo)} convênios para tratar hoje ({hoje.strftime('%d/%m/%Y')})!**")
        st.dataframe(
            df_hoje_resumo,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Data_Corte": st.column_config.DateColumn("Data_Corte", format="DD/MM/YYYY"),
                "Data_Lancamento": st.column_config.DateColumn("Data_Lancamento", format="DD/MM/YYYY"),
            }
        )
    else:
        st.info(f"✅ Nenhuma pendência de corte ou lançamento para hoje ({hoje.strftime('%d/%m/%Y')}).")

    st.divider()  # Uma linha para separar o resumo da tabela completa



    # --- TABELA COMPLETA E FILTROS (CÓDIGO ANTERIOR) ---
    st.subheader("Base Geral Completa")

    # 2. Aplica a Lógica dos Filtros

    # Filtro de convênios
    if convenios_filtro:
        df_visualizacao = df_visualizacao[df_visualizacao['Convenio'].isin(convenios_filtro)]

    # Filtro de sistemas
    if sistema_filtro:
        df_visualizacao = df_visualizacao[df_visualizacao['Sistema'].isin(sistema_filtro)]

    # Filtro dos responsáveis
    if responsavel_filtro:
        df_visualizacao = df_visualizacao[df_visualizacao['Responsavel'].isin(responsavel_filtro)]

    # Filtro dos validadores
    if validacao_filtro:
        df_visualizacao = df_visualizacao[df_visualizacao['Validacao'].isin(validacao_filtro)]

    # Filtro de Data de Lançamento
    if data_filtro_lancamento:
        # Precisamos usar .dt.date para comparar Data (input) com Timestamp (pandas)
        df_visualizacao = df_visualizacao[df_visualizacao['Data_Lancamento'].dt.date == data_filtro_lancamento]

    # Filtro de Data de Corte
    if data_filtro_corte:
        df_visualizacao = df_visualizacao[df_visualizacao['Data_Corte'].dt.date == data_filtro_corte]

    # 3. Mostra o Resultado
    st.dataframe(
        df_visualizacao,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Data_Corte": st.column_config.DateColumn("Data_Corte", format="DD/MM/YYYY"),
            "Data_Lancamento": st.column_config.DateColumn("Data_Lancamento", format="DD/MM/YYYY"),
        }
    )

    st.caption(f"Mostrando {len(df_visualizacao)} registros encontrados.")

    # Botão de Download
    st.download_button(
        label="📥 Baixar Dados Filtrados",
        data=to_excel(df_visualizacao),
        file_name="relatorio_filtrado.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

else:
    st.info("O banco de dados está vazio. Use a barra lateral para fazer o primeiro upload.")