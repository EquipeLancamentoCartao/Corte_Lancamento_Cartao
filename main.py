import streamlit as st
import pandas as pd
from datetime import datetime
import sqlite3
import io

# Configuração da página para ocupar mais espaço na tela
st.set_page_config(page_title="Gestor de Convênio", layout="wide")

# CSS para ocultar elementos indesejados
hide_streamlit_style = """
<style>
    /* Ocultar o rodapé padrão "Made with Streamlit" */
    footer {visibility: hidden;}

    /* Ocultar o Menu Hambúrguer no canto superior direito (opcional, remova se quiser manter) */
    #MainMenu {visibility: hidden;}

    /* Ocultar especificamente o botão com sua foto/perfil no rodapé (Viewer Badge) */
    div[class^='viewerBadge'] {display: none;}

    /* Caso a classe mude, esta é uma abordagem mais agressiva para o container do rodapé da cloud */
    .stApp > header {visibility: hidden;}
</style>
"""

st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# --- CONEXÃO COM BANCO DE DADOS (SQLITE) ---
def get_database_connection():
    # Cria (ou conecta) a um arquivo local chamado 'dados_convenios.db'
    conn = sqlite3.connect('dados_convenios.db', check_same_thread=False)
    return conn


def carregar_dados_do_banco():
    """Lê os dados salvos no banco para mostrar na tela"""
    conn = get_database_connection()
    try:
        # Lê a tabela 'lancamentos'. Se não existir (banco novo), retorna DataFrame vazio.
        df = pd.read_sql('SELECT * FROM lancamentos', conn)

        # Converte as colunas de data que vêm do SQL como texto de volta para datetime
        cols_data = ['Data de corte', 'Data de lançamento']
        for col in cols_data:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    except:
        return pd.DataFrame()  # Retorna vazio se der erro ou tabela não existir
    finally:
        conn.close()


def salvar_no_banco(df_novo, modo='append'):
    """
    Salva os dados tratados no banco.
    modo='append': Adiciona ao que já existe.
    modo='replace': Apaga tudo e coloca o novo no lugar.
    """
    conn = get_database_connection()
    # index=False para não criar uma coluna de índice extra no banco
    df_novo.to_sql('lancamentos', conn, if_exists=modo, index=False)
    conn.close()


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
        tem_palavra_chave = any(p in valor_coluna_conv for p in palavras_chave)

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

    # 2. Verifica se encontrou as duas colunas
    if col_origem_corte and col_origem_lanc:
        # 3. Faz o rename usando os nomes que encontramos
        df_clean = df_clean.rename(columns={
            col_origem_corte: 'Data de corte',
            col_origem_lanc: 'Data de lançamento'
        })
    else:
        print('Alguma das colunas ("Data de corte" ou "Data de lançamento") não se encontra na planilha')
        print(f'colunas de datas de corte\n{df_clean.columns}')
        return False  # ou return apenas

    cols_data = ['Data de corte', 'Data de lançamento']
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

# --- BARRA LATERAL ---
with st.sidebar:
    st.header("⚙️ Administração")
    uploaded_file = st.file_uploader("Subir nova planilha", type=['xlsx', 'xls'])

    if uploaded_file is not None:
        if st.button("Processar e Salvar"):
            with st.spinner("Salvando no banco..."):
                df_tratado = tratar_planilha(uploaded_file)
                modo_sql = 'replace'
                salvar_no_banco(df_tratado, modo=modo_sql)
            st.success("Salvo!")
            st.rerun()

    st.divider()

    # --- AQUI ENTRAM OS SEUS FILTROS ---
    st.header("🔍 Filtros de Visualização")

    # 2. Seus filtros de Data
    data_filtro_lancamento = st.date_input(
        "Data de Lançamento exata:",
        value=None,
        format="DD/MM/YYYY"
    )

    data_filtro_corte = st.date_input(
        "Data de Corte exata:",
        value=None,
        format="DD/MM/YYYY"
    )

    if st.button("Limpar Filtros"):
        st.rerun()

    if st.button("🗑️ Limpar todo o Banco de Dados"):
        conn = get_database_connection()
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS lancamentos")
        conn.commit()
        conn.close()
        st.warning("Banco de dados limpo!")
        st.rerun()

# --- ÁREA PRINCIPAL ---
st.subheader("Visualização da Base de Dados")

# 1. Carrega do Banco
df_visualizacao = carregar_dados_do_banco()

if not df_visualizacao.empty:

    # --- NOVIDADE: TABELA DE "HOJE" ---
    # Pegamos a data atual do sistema
    hoje = datetime.now().date()

    # Filtramos: Mostra se a data de corte OU a data de lançamento for HOJE
    # Usamos .dt.date para garantir que estamos comparando apenas dia/mês/ano (ignorando horas)
    filtro_hoje = (
            df_visualizacao['Data de lançamento'].dt.date == hoje
    )

    df_hoje = df_visualizacao[filtro_hoje]

    # Selecionamos apenas as colunas que você pediu
    # Nota: Certifique-se que o nome da coluna é "Convênios" (plural) ou "Convênio" (singular) conforme sua planilha
    colunas_resumo = ['Convênio', 'Data de corte', 'Data de lançamento', 'Responsavel', 'Validação']

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
                "Data de corte": st.column_config.DateColumn("Data de corte", format="DD/MM/YYYY"),
                "Data de lançamento": st.column_config.DateColumn("Data de lançamento", format="DD/MM/YYYY"),
            }
        )
    else:
        st.info(f"✅ Nenhuma pendência de corte ou lançamento para hoje ({hoje.strftime('%d/%m/%Y')}).")

    st.divider()  # Uma linha para separar o resumo da tabela completa

    # --- TABELA COMPLETA E FILTROS (CÓDIGO ANTERIOR) ---
    st.subheader("Base Geral Completa")

    # 2. Aplica a Lógica dos Filtros

    # Filtro de Data de Lançamento
    if data_filtro_lancamento:
        # Precisamos usar .dt.date para comparar Data (input) com Timestamp (pandas)
        df_visualizacao = df_visualizacao[df_visualizacao['Data de lançamento'].dt.date == data_filtro_lancamento]

    # Filtro de Data de Corte
    if data_filtro_corte:
        df_visualizacao = df_visualizacao[df_visualizacao['Data de corte'].dt.date == data_filtro_corte]

    # 3. Mostra o Resultado
    st.dataframe(
        df_visualizacao,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Data de corte": st.column_config.DateColumn("Data de corte", format="DD/MM/YYYY"),
            "Data de lançamento": st.column_config.DateColumn("Data de lançamento", format="DD/MM/YYYY"),
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