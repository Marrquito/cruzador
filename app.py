import io
import os

import pandas as pd
import streamlit as st
import yaml
import streamlit_authenticator as stauth
from yaml.loader import SafeLoader

from core.loader import load_csv, merge_files, get_products, get_status_options
from core.analyzer import crossref, filter_by_status
from core.charts import funnel_chart, sequence_pie, timeline_scatter, products_bar

# ── Configuração da página ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Cruzador de Dados",
    page_icon="🔀",
    layout="wide",
)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")


# ── Autenticação ──────────────────────────────────────────────────────────────
@st.cache_resource
def load_config():
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.load(f, Loader=SafeLoader)


config = load_config()

authenticator = stauth.Authenticate(
    config["credentials"],
    config["cookie"]["name"],
    config["cookie"]["key"],
    config["cookie"]["expiry_days"],
    auto_hash=True,
)

authenticator.login(fields={"Form name": "Cruzador de Dados — Login"})

auth_status = st.session_state.get("authentication_status")

if auth_status is False:
    st.error("Usuário ou senha incorretos.")
    st.stop()

if auth_status is None:
    st.info("Informe suas credenciais para acessar a plataforma.")
    st.stop()

# ── Usuário autenticado — App principal ───────────────────────────────────────
user_name = st.session_state.get("name", "Usuário")

with st.sidebar:
    st.title("🔀 Cruzador de Dados")
    st.caption(f"Olá, **{user_name}**")
    authenticator.logout("Sair", location="sidebar")
    st.divider()

    # Upload de arquivos
    st.subheader("📂 Arquivos")
    uploaded_files = st.file_uploader(
        "Carregar planilhas CSV",
        type=["csv"],
        accept_multiple_files=True,
        help="Envie um ou mais arquivos CSV exportados do Hotmart.",
    )

    st.divider()
    st.caption("v1.0 · Cruzador")


# ── Carregamento dos dados ─────────────────────────────────────────────────────
@st.cache_data(show_spinner="Processando arquivo...")
def process_files(files_data: list[tuple[str, bytes]]) -> pd.DataFrame:
    dfs = []
    for _name, content in files_data:
        try:
            dfs.append(load_csv(content))
        except Exception as e:
            st.warning(f"Erro ao carregar '{_name}': {e}")
    if not dfs:
        return pd.DataFrame()
    return merge_files(dfs)


if not uploaded_files:
    st.title("🔀 Cruzador de Dados")
    st.info("Carregue ao menos um arquivo CSV no painel lateral para começar.")
    st.stop()

files_data = [(f.name, f.read()) for f in uploaded_files]
df_raw = process_files(files_data)

if df_raw.empty:
    st.error("Nenhum dado válido encontrado nos arquivos carregados.")
    st.stop()

# ── Layout principal ──────────────────────────────────────────────────────────
st.title("🔀 Cruzador de Dados")
st.caption(
    f"{len(uploaded_files)} arquivo(s) · **{len(df_raw):,}** transações carregadas"
)

tab_overview, tab_cross = st.tabs(["📊 Visão Geral", "🔗 Cruzamento de Produtos"])


# ──────────────────────────────────────────────────────────────────────────────
# TAB 1 — VISÃO GERAL
# ──────────────────────────────────────────────────────────────────────────────
with tab_overview:
    all_status = get_status_options(df_raw)
    selected_status = st.multiselect(
        "Filtrar por Status",
        options=all_status,
        default=[s for s in ["COMPLETO"] if s in all_status],
        key="status_overview",
    )

    df_view = filter_by_status(df_raw, selected_status) if selected_status else df_raw

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total de Transações", f"{len(df_view):,}")
    col2.metric(
        "Compradores Únicos",
        f"{df_view['_id_comprador'].nunique():,}" if "_id_comprador" in df_view.columns else "—",
    )
    col3.metric(
        "Produtos Distintos",
        f"{df_view['Nome do Produto'].nunique():,}" if "Nome do Produto" in df_view.columns else "—",
    )

    if "Data do Pedido" in df_view.columns:
        data_min = df_view["Data do Pedido"].min()
        data_max = df_view["Data do Pedido"].max()
        periodo = f"{data_min.strftime('%d/%m/%Y')} – {data_max.strftime('%d/%m/%Y')}" if pd.notna(data_min) else "—"
        col4.metric("Período", periodo)

    st.plotly_chart(products_bar(df_view), use_container_width=True)


# ──────────────────────────────────────────────────────────────────────────────
# TAB 2 — CRUZAMENTO
# ──────────────────────────────────────────────────────────────────────────────
with tab_cross:
    all_products = get_products(df_raw)

    if not all_products:
        st.warning("Nenhum produto encontrado nos dados.")
        st.stop()

    col_cfg1, col_cfg2, col_cfg3 = st.columns([2, 2, 1])

    with col_cfg1:
        group_a = st.multiselect(
            "Grupo A — Produto(s) de origem",
            options=all_products,
            help="Selecione um ou mais produtos. Compradores de qualquer um deles entrarão no Grupo A.",
        )

    with col_cfg2:
        product_b = st.selectbox(
            "Produto B — Produto de destino",
            options=[""] + all_products,
            help="Produto que queremos verificar se os compradores do Grupo A também adquiriram.",
        )

    with col_cfg3:
        status_cross = st.multiselect(
            "Status",
            options=get_status_options(df_raw),
            default=[s for s in ["COMPLETO"] if s in get_status_options(df_raw)],
            key="status_cross",
        )

    run = st.button("Analisar", type="primary", disabled=not (group_a and product_b))

    if not run:
        if not group_a or not product_b:
            st.info("Selecione o Grupo A e o Produto B para iniciar a análise.")
        st.stop()

    # ── Executar análise ───────────────────────────────────────────────────────
    df_filtered = filter_by_status(df_raw, status_cross) if status_cross else df_raw

    with st.spinner("Cruzando dados..."):
        try:
            result = crossref(df_filtered, group_a, product_b)
        except Exception as e:
            st.error(f"Erro na análise: {e}")
            st.stop()

    summary = result["summary"]
    intersection = result["intersection"]
    only_a = result["only_a"]
    only_b = result["only_b"]

    # ── Métricas ───────────────────────────────────────────────────────────────
    st.subheader("Resumo")
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Compradores Grupo A", f"{summary['total_grupo_a']:,}")
    m2.metric("Compradores Produto B", f"{summary['total_produto_b']:,}")
    m3.metric("Compraram ambos", f"{summary['compraram_ambos']:,}")
    m4.metric(
        "Taxa de conversão A→B",
        f"{summary['taxa_conversao_pct']}%",
        help="% dos compradores do Grupo A que também compraram o Produto B",
    )
    media = summary["media_dias_entre_compras"]
    m5.metric(
        "Média entre compras",
        f"{media} dias" if media is not None else "—",
    )

    # ── Gráficos ───────────────────────────────────────────────────────────────
    if summary["compraram_ambos"] > 0:
        gc1, gc2 = st.columns(2)
        with gc1:
            st.plotly_chart(
                funnel_chart(summary, "Grupo A", product_b),
                use_container_width=True,
            )
        with gc2:
            st.plotly_chart(
                sequence_pie(summary["sequencia"]),
                use_container_width=True,
            )

    # ── Tabela de compradores em ambos ────────────────────────────────────────
    st.subheader(f"Compradores em ambos os grupos ({len(intersection)})")

    if not intersection.empty:
        # Exportar
        csv_bytes = intersection.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "⬇ Exportar resultado (CSV)",
            data=csv_bytes,
            file_name="cruzamento_resultado.csv",
            mime="text/csv",
        )

        st.dataframe(
            intersection,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Dias entre compras": st.column_config.NumberColumn(format="%d dias"),
            },
        )

        # Timeline — mostrar apenas se não for muito grande
        if len(intersection) <= 100:
            group_a_col = [c for c in intersection.columns if c.startswith("Data ") and "Produto B" not in c]
            product_b_col = [c for c in intersection.columns if c.startswith("Data ") and c not in group_a_col]
            if group_a_col and product_b_col:
                with st.expander("Ver timeline de compras"):
                    st.plotly_chart(
                        timeline_scatter(intersection, group_a_col[0], product_b_col[0]),
                        use_container_width=True,
                    )
        else:
            st.caption("Timeline desabilitada para mais de 100 registros.")
    else:
        st.info("Nenhum comprador encontrado nos dois grupos com os filtros selecionados.")

    # ── Só Grupo A ────────────────────────────────────────────────────────────
    with st.expander(f"Só compraram Grupo A (não foram para B) — {len(only_a)}"):
        if not only_a.empty:
            csv_a = only_a.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "⬇ Exportar (CSV)",
                data=csv_a,
                file_name="so_grupo_a.csv",
                mime="text/csv",
                key="dl_only_a",
            )
            st.dataframe(only_a, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum registro.")

    # ── Só Produto B ──────────────────────────────────────────────────────────
    with st.expander(f"Só compraram Produto B (não estavam no Grupo A) — {len(only_b)}"):
        if not only_b.empty:
            csv_b = only_b.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "⬇ Exportar (CSV)",
                data=csv_b,
                file_name="so_produto_b.csv",
                mime="text/csv",
                key="dl_only_b",
            )
            st.dataframe(only_b, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum registro.")
