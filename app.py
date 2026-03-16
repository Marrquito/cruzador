import os

import pandas as pd
import streamlit as st
import yaml
import streamlit_authenticator as stauth
from yaml.loader import SafeLoader

from core.loader import (
    detect_file_type,
    load_vendas,
    load_leads,
    merge_vendas,
    merge_leads,
    get_products,
    get_status_options,
    get_states,
    get_payment_methods,
    get_tags,
    get_forms,
    get_utm_options,
)
from core.analyzer import crossref, filter_by_status
from core.charts import (
    funnel_chart, sequence_pie, timeline_scatter, products_bar,
    days_histogram, tags_distribution_bar, utm_content_bar,
    first_entry_bar, utm_funnel_bar,
)
from core.cross_analyzer import (
    analysis_lead_to_purchase,
    analysis_avg_tags_per_buyer,
    analysis_leads_by_utm_content,
    analysis_first_entry_to_sales,
    analysis_utm_funnel,
    get_utm_values,
)

# ── Configuração da página ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Cruzador de Dados",
    page_icon="🔀",
    layout="wide",
)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")


# ── Autenticação ──────────────────────────────────────────────────────────────
def _deep_dict(obj) -> dict:
    """Converte recursivamente AttrDict do st.secrets em dict puro."""
    if hasattr(obj, "items"):
        return {k: _deep_dict(v) for k, v in obj.items()}
    return obj


@st.cache_resource
def load_config():
    # 1. Tenta st.secrets (Streamlit Cloud ou .streamlit/secrets.toml local)
    try:
        if "credentials" in st.secrets and "cookie" in st.secrets:
            return {
                "credentials": _deep_dict(st.secrets["credentials"]),
                "cookie": _deep_dict(st.secrets["cookie"]),
            }
    except Exception:
        pass

    # 2. Fallback: config.yaml (desenvolvimento local sem secrets.toml)
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, encoding="utf-8") as f:
            return yaml.load(f, Loader=SafeLoader)

    st.error(
        "Credenciais não encontradas. Configure `st.secrets` ou crie um `config.yaml`. "
        "Consulte o `secrets.toml.example` na raiz do projeto."
    )
    st.stop()


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

    st.subheader("📂 Arquivos")
    uploaded_files = st.file_uploader(
        "Carregar planilhas CSV",
        type=["csv"],
        accept_multiple_files=True,
        help="Envie arquivos de vendas (Hotmart) e/ou leads (UTMs/Tags). O tipo é detectado automaticamente.",
    )

    st.divider()
    st.caption("v1.0 · Cruzador")


# ── Carregamento e separação dos dados ───────────────────────────────────────
@st.cache_data(show_spinner="Processando arquivos...")
def process_files(files_data: list[tuple[str, bytes]]) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """
    Retorna (df_vendas, df_leads, alertas).
    Detecta automaticamente o tipo de cada arquivo.
    """
    vendas_dfs, leads_dfs, alertas = [], [], []

    for name, content in files_data:
        try:
            import io
            import pandas as _pd
            from core.loader import ENCODINGS

            for enc in ENCODINGS:
                try:
                    df_peek = _pd.read_csv(io.BytesIO(content), encoding=enc, nrows=1)
                    break
                except UnicodeDecodeError:
                    continue

            file_type = detect_file_type(df_peek)

            if file_type == "vendas":
                vendas_dfs.append(load_vendas(content))
            elif file_type == "leads":
                leads_dfs.append(load_leads(content))
            else:
                alertas.append(f"'{name}': tipo não reconhecido — ignorado.")
        except Exception as e:
            alertas.append(f"Erro ao carregar '{name}': {e}")

    df_vendas = merge_vendas(vendas_dfs) if vendas_dfs else pd.DataFrame()
    df_leads = merge_leads(leads_dfs) if leads_dfs else pd.DataFrame()
    return df_vendas, df_leads, alertas


if not uploaded_files:
    st.title("🔀 Cruzador de Dados")
    st.info("Carregue ao menos um arquivo CSV no painel lateral para começar.")
    st.stop()

files_data = [(f.name, f.read()) for f in uploaded_files]
df_vendas, df_leads, alertas = process_files(files_data)

# Exibe alertas no sidebar
with st.sidebar:
    n_vendas = len([f for f in files_data if True])  # contagem via dados
    if not df_vendas.empty:
        st.success(f"Vendas: **{len(df_vendas):,}** transações")
    if not df_leads.empty:
        st.success(f"Leads: **{len(df_leads):,}** registros")
    for alerta in alertas:
        st.warning(alerta)

if df_vendas.empty and df_leads.empty:
    st.error("Nenhum dado válido encontrado nos arquivos carregados.")
    st.stop()

# ── Layout principal ──────────────────────────────────────────────────────────
st.title("🔀 Cruzador de Dados")

tab_overview, tab_cross, tab_vendas, tab_leads, tab_analises = st.tabs([
    "📊 Visão Geral",
    "🔗 Cruzamento",
    "💰 Tabela de Vendas",
    "👤 Tabela de Leads",
    "📈 Análises",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — VISÃO GERAL
# ══════════════════════════════════════════════════════════════════════════════
with tab_overview:
    if df_vendas.empty:
        st.info("Nenhum arquivo de vendas carregado.")
    else:
        all_status = get_status_options(df_vendas)
        selected_status = st.multiselect(
            "Filtrar por Status",
            options=all_status,
            # default=[s for s in ["COMPLETO"] if s in all_status],
            key="status_overview",
        )

        df_view = filter_by_status(df_vendas, selected_status) if selected_status else df_vendas

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
            periodo = (
                f"{data_min.strftime('%d/%m/%Y')} – {data_max.strftime('%d/%m/%Y')}"
                if pd.notna(data_min)
                else "—"
            )
            col4.metric("Período", periodo)

        st.plotly_chart(products_bar(df_view), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — CRUZAMENTO
# ══════════════════════════════════════════════════════════════════════════════
with tab_cross:
    if df_vendas.empty:
        st.info("Nenhum arquivo de vendas carregado.")
    else:
        all_products = get_products(df_vendas)

        if not all_products:
            st.warning("Nenhum produto encontrado nos dados.")
        else:
            col_cfg1, col_cfg2, col_cfg3 = st.columns([2, 2, 1])

            with col_cfg1:
                group_a = st.multiselect(
                    "Grupo A — Produto(s) de origem",
                    options=all_products,
                    help="Compradores de qualquer produto selecionado entram no Grupo A.",
                )

            with col_cfg2:
                product_b = st.selectbox(
                    "Produto B — Produto de destino",
                    options=[""] + all_products,
                )

            with col_cfg3:
                status_cross = st.multiselect(
                    "Status",
                    options=get_status_options(df_vendas),
                    # default=[s for s in ["COMPLETO"] if s in get_status_options(df_vendas)],
                    key="status_cross",
                )

            run = st.button("Analisar", type="primary", disabled=not (group_a and product_b))

            if not run:
                st.info("Selecione o Grupo A e o Produto B para iniciar a análise.")
            else:
                df_filtered = filter_by_status(df_vendas, status_cross) if status_cross else df_vendas

                with st.spinner("Cruzando dados..."):
                    try:
                        result = crossref(df_filtered, group_a, product_b)
                    except Exception as e:
                        st.error(f"Erro na análise: {e}")
                        result = None

                if result is not None:
                    summary = result["summary"]
                    intersection = result["intersection"]
                    only_a = result["only_a"]
                    only_b = result["only_b"]

                    st.subheader("Resumo")
                    m1, m2, m3, m4, m5 = st.columns(5)
                    m1.metric("Compradores Grupo A", f"{summary['total_grupo_a']:,}")
                    m2.metric("Compradores Produto B", f"{summary['total_produto_b']:,}")
                    m3.metric("Compraram ambos", f"{summary['compraram_ambos']:,}")
                    m4.metric("Taxa de conversão A→B", f"{summary['taxa_conversao_pct']}%")
                    media = summary["media_dias_entre_compras"]
                    m5.metric("Média entre compras", f"{media} dias" if media is not None else "—")

                    if summary["compraram_ambos"] > 0:
                        gc1, gc2 = st.columns(2)
                        with gc1:
                            st.plotly_chart(funnel_chart(summary, "Grupo A", product_b), use_container_width=True)
                        with gc2:
                            st.plotly_chart(sequence_pie(summary["sequencia"]), use_container_width=True)

                    st.subheader(f"Compradores em ambos os grupos ({len(intersection)})")

                    if not intersection.empty:
                        st.download_button(
                            "⬇ Exportar resultado (CSV)",
                            data=intersection.to_csv(index=False).encode("utf-8-sig"),
                            file_name="cruzamento_resultado.csv",
                            mime="text/csv",
                        )
                        st.dataframe(
                            intersection,
                            use_container_width=True,
                            hide_index=True,
                            column_config={"Dias entre compras": st.column_config.NumberColumn(format="%d dias")},
                        )

                        if len(intersection) <= 100:
                            group_a_col = [c for c in intersection.columns if c.startswith("Data ") and "Produto B" not in c]
                            product_b_col = [c for c in intersection.columns if c.startswith("Data ") and c not in group_a_col]
                            if group_a_col and product_b_col:
                                with st.expander("Ver timeline de compras"):
                                    st.plotly_chart(
                                        timeline_scatter(intersection, group_a_col[0], product_b_col[0]),
                                        use_container_width=True,
                                    )

                    with st.expander(f"Só compraram Grupo A (não foram para B) — {len(only_a)}"):
                        if not only_a.empty:
                            st.download_button(
                                "⬇ Exportar (CSV)", data=only_a.to_csv(index=False).encode("utf-8-sig"),
                                file_name="so_grupo_a.csv", mime="text/csv", key="dl_only_a",
                            )
                            st.dataframe(only_a, use_container_width=True, hide_index=True)

                    with st.expander(f"Só compraram Produto B (não estavam no Grupo A) — {len(only_b)}"):
                        if not only_b.empty:
                            st.download_button(
                                "⬇ Exportar (CSV)", data=only_b.to_csv(index=False).encode("utf-8-sig"),
                                file_name="so_produto_b.csv", mime="text/csv", key="dl_only_b",
                            )
                            st.dataframe(only_b, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — TABELA DE VENDAS
# ══════════════════════════════════════════════════════════════════════════════
with tab_vendas:
    if df_vendas.empty:
        st.info("Nenhum arquivo de vendas carregado.")
    else:
        # ── Filtros ────────────────────────────────────────────────────────────
        with st.expander("Filtros", expanded=True):
            fc1, fc2, fc3 = st.columns(3)

            with fc1:
                status_v = st.multiselect(
                    "Status",
                    options=get_status_options(df_vendas),
                    # default=[s for s in ["COMPLETO"] if s in get_status_options(df_vendas)],
                    key="status_vendas",
                )
                produtos_v = st.multiselect(
                    "Produto",
                    options=get_products(df_vendas),
                    key="produtos_vendas",
                )

            with fc2:
                estados_v = st.multiselect(
                    "Estado",
                    options=get_states(df_vendas),
                    key="estados_vendas",
                )
                metodos_v = st.multiselect(
                    "Método de Pagamento",
                    options=get_payment_methods(df_vendas),
                    key="metodos_vendas",
                )

            with fc3:
                date_min_v = df_vendas["Data do Pedido"].min() if "Data do Pedido" in df_vendas.columns else None
                date_max_v = df_vendas["Data do Pedido"].max() if "Data do Pedido" in df_vendas.columns else None

                if date_min_v and pd.notna(date_min_v):
                    data_inicio_v = st.date_input(
                        "Data inicial", value=date_min_v.date(), key="data_inicio_v"
                    )
                    data_fim_v = st.date_input(
                        "Data final", value=date_max_v.date(), key="data_fim_v"
                    )
                else:
                    data_inicio_v = data_fim_v = None

        # ── Aplicar filtros ────────────────────────────────────────────────────
        dv = df_vendas.copy()

        if status_v:
            dv = dv[dv["Status"].isin(status_v)]
        if produtos_v:
            dv = dv[dv["Nome do Produto"].isin(produtos_v)]
        if estados_v and "Estado do Comprador" in dv.columns:
            dv = dv[dv["Estado do Comprador"].isin(estados_v)]
        if metodos_v and "Método de Pagamento" in dv.columns:
            dv = dv[dv["Método de Pagamento"].isin(metodos_v)]
        if data_inicio_v and data_fim_v and "Data do Pedido" in dv.columns:
            dv = dv[
                (dv["Data do Pedido"].dt.date >= data_inicio_v)
                & (dv["Data do Pedido"].dt.date <= data_fim_v)
            ]

        # ── Métricas ───────────────────────────────────────────────────────────
        mv1, mv2, mv3, mv4 = st.columns(4)
        mv1.metric("Transações", f"{len(dv):,}")
        mv2.metric(
            "Compradores únicos",
            f"{dv['_id_comprador'].nunique():,}" if "_id_comprador" in dv.columns else "—",
        )

        val_col = "Valor Pago pelo Comprador Sem Taxas e Impostos"
        if val_col in dv.columns:
            receita = dv[val_col].sum()
            mv3.metric("Receita total (sem taxas)", f"R$ {receita:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        else:
            mv3.metric("Receita total", "—")

        mv4.metric(
            "Produtos distintos",
            f"{dv['Nome do Produto'].nunique():,}" if "Nome do Produto" in dv.columns else "—",
        )

        # ── Tabela ─────────────────────────────────────────────────────────────
        COLS_VENDAS = [c for c in [
            "Data do Pedido",
            "Data de Aprovação",
            "Nome do Comprador",
            "E-mail do Comprador",
            "Telefone do Comprador",
            "CPF/CNPJ Comprador",
            "Cidade do Comprador",
            "Estado do Comprador",
            "Nome do Produto",
            "Valor do Produto",
            val_col,
            "Status",
            "Método de Pagamento",
            "Número de Parcelas",
            "Fonte de Rastreamento",
            "Código de Rastreamento",
        ] if c in dv.columns]

        dv_display = dv[COLS_VENDAS].sort_values("Data do Pedido", ascending=False) if "Data do Pedido" in dv.columns else dv[COLS_VENDAS]

        st.download_button(
            "⬇ Exportar tabela (CSV)",
            data=dv_display.to_csv(index=False).encode("utf-8-sig"),
            file_name="vendas_filtrado.csv",
            mime="text/csv",
            key="dl_vendas",
        )

        st.dataframe(
            dv_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Data do Pedido": st.column_config.DatetimeColumn(format="DD/MM/YYYY"),
                "Data de Aprovação": st.column_config.DatetimeColumn(format="DD/MM/YYYY"),
                "Valor do Produto": st.column_config.NumberColumn(format="R$ %.2f"),
                val_col: st.column_config.NumberColumn(format="R$ %.2f"),
            },
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — TABELA DE LEADS
# ══════════════════════════════════════════════════════════════════════════════
with tab_leads:
    if df_leads.empty:
        st.info("Nenhum arquivo de leads carregado.")
    else:
        # ── Filtros ────────────────────────────────────────────────────────────
        with st.expander("Filtros", expanded=True):
            fl1, fl2, fl3 = st.columns(3)

            with fl1:
                tags_l = st.multiselect(
                    "Tag",
                    options=get_tags(df_leads),
                    key="tags_leads",
                )
                forms_l = st.multiselect(
                    "Formulário de origem",
                    options=get_forms(df_leads),
                    key="forms_leads",
                )

            with fl2:
                utm_sources = st.multiselect(
                    "UTM Source",
                    options=get_utm_options(df_leads, "utm_source"),
                    key="utm_source_leads",
                )
                utm_campaigns = st.multiselect(
                    "UTM Campaign",
                    options=get_utm_options(df_leads, "utm_campaign"),
                    key="utm_campaign_leads",
                )

            with fl3:
                utm_mediums = st.multiselect(
                    "UTM Medium",
                    options=get_utm_options(df_leads, "utm_medium"),
                    key="utm_medium_leads",
                )

                date_min_l = df_leads["lead_register"].min() if "lead_register" in df_leads.columns else None
                date_max_l = df_leads["lead_register"].max() if "lead_register" in df_leads.columns else None

                if date_min_l and pd.notna(date_min_l):
                    data_inicio_l = st.date_input(
                        "Data inicial", value=date_min_l.date(), key="data_inicio_l"
                    )
                    data_fim_l = st.date_input(
                        "Data final", value=date_max_l.date(), key="data_fim_l"
                    )
                else:
                    data_inicio_l = data_fim_l = None

        # ── Aplicar filtros ────────────────────────────────────────────────────
        dl = df_leads.copy()

        if tags_l and "tag_name" in dl.columns:
            dl = dl[dl["tag_name"].isin(tags_l)]
        if forms_l and "lead_register_form" in dl.columns:
            dl = dl[dl["lead_register_form"].astype(str).isin(forms_l)]
        if utm_sources and "utm_source" in dl.columns:
            dl = dl[dl["utm_source"].isin(utm_sources)]
        if utm_campaigns and "utm_campaign" in dl.columns:
            dl = dl[dl["utm_campaign"].isin(utm_campaigns)]
        if utm_mediums and "utm_medium" in dl.columns:
            dl = dl[dl["utm_medium"].isin(utm_mediums)]
        if data_inicio_l and data_fim_l and "lead_register" in dl.columns:
            dl = dl[
                (dl["lead_register"].dt.date >= data_inicio_l)
                & (dl["lead_register"].dt.date <= data_fim_l)
            ]

        # ── Métricas ───────────────────────────────────────────────────────────
        ml1, ml2, ml3, ml4 = st.columns(4)
        ml1.metric("Total de Leads", f"{len(dl):,}")
        ml2.metric(
            "E-mails únicos",
            f"{dl['lead_email'].nunique():,}" if "lead_email" in dl.columns else "—",
        )
        ml3.metric(
            "Fontes (utm_source)",
            f"{dl['utm_source'].nunique():,}" if "utm_source" in dl.columns else "—",
        )
        ml4.metric(
            "Campanhas (utm_campaign)",
            f"{dl['utm_campaign'].nunique():,}" if "utm_campaign" in dl.columns else "—",
        )

        # ── Tabela ─────────────────────────────────────────────────────────────
        COLS_LEADS = [c for c in [
            "lead_register",
            "lead_name",
            "lead_email",
            "lead_phone",
            "tag_name",
            "lead_register_form",
            "event",
            "utm_source",
            "utm_campaign",
            "utm_medium",
            "utm_content",
            "utm_term_campaign",
            "utm_term_medium",
            "utm_term_content",
            "utm_id",
        ] if c in dl.columns]

        dl_display = dl[COLS_LEADS].sort_values("lead_register", ascending=False) if "lead_register" in dl.columns else dl[COLS_LEADS]

        st.download_button(
            "⬇ Exportar tabela (CSV)",
            data=dl_display.to_csv(index=False).encode("utf-8-sig"),
            file_name="leads_filtrado.csv",
            mime="text/csv",
            key="dl_leads",
        )

        st.dataframe(
            dl_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "lead_register": st.column_config.DatetimeColumn(
                    "Data de Cadastro", format="DD/MM/YYYY HH:mm"
                ),
                "lead_name": st.column_config.TextColumn("Nome"),
                "lead_email": st.column_config.TextColumn("E-mail"),
                "lead_phone": st.column_config.TextColumn("Telefone"),
                "tag_name": st.column_config.TextColumn("Tag"),
                "lead_register_form": st.column_config.TextColumn("Formulário"),
                "utm_source": st.column_config.TextColumn("Source"),
                "utm_campaign": st.column_config.TextColumn("Campaign"),
                "utm_medium": st.column_config.TextColumn("Medium"),
                "utm_content": st.column_config.TextColumn("Content"),
            },
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — ANÁLISES CRUZADAS
# ══════════════════════════════════════════════════════════════════════════════
with tab_analises:
    _sem_leads = df_leads.empty
    _sem_vendas = df_vendas.empty

    if _sem_leads and _sem_vendas:
        st.info("Carregue arquivos de leads e vendas para usar as análises cruzadas.")
    else:
        # ── Configurações globais da aba ──────────────────────────────────────
        with st.container():
            ga1, ga2, ga3 = st.columns([3, 2, 1])
            with ga1:
                produto_analise = st.selectbox(
                    "Produto de referência",
                    options=get_products(df_vendas) if not _sem_vendas else [],
                    key="produto_analise",
                    help="Usado nas análises que cruzam leads com vendas.",
                )
            with ga2:
                status_analise = st.multiselect(
                    "Status das vendas",
                    options=get_status_options(df_vendas) if not _sem_vendas else [],
                    # default=[s for s in ["COMPLETO"] if not _sem_vendas and s in get_status_options(df_vendas)],
                    key="status_analise",
                )
            with ga3:
                st.write("")  # espaço vertical
                rodar = st.button("Rodar análises", type="primary", key="rodar_analises")

        if not rodar:
            st.info("Selecione o produto de referência e clique em **Rodar análises**.")
        else:
            if not produto_analise:
                st.warning("Selecione um produto de referência.")
            else:
                _status = status_analise or None

                an1, an2, an3, an4, an5 = st.tabs([
                    "⏱ Lead → Compra",
                    "🏷 Tags por comprador",
                    "📣 Anúncios (utm_content)",
                    "🚪 Primeira entrada → Venda",
                    "📡 Funil por UTM",
                ])

                # ── Análise 1 — Tempo médio lead → compra ────────────────────
                with an1:
                    if _sem_leads or _sem_vendas:
                        st.info("Necessário ter leads e vendas carregados.")
                    else:
                        r1 = analysis_lead_to_purchase(df_leads, df_vendas, produto_analise, _status)

                        if "error" in r1:
                            st.error(r1["error"])
                        elif r1["count"] == 0:
                            st.warning("Nenhum lead encontrado que tenha comprado este produto.")
                        else:
                            st.caption(f"Compradores de **{produto_analise}** encontrados também na base de leads: **{r1['count']}**")
                            c1, c2, c3, c4 = st.columns(4)
                            c1.metric("Média", f"{r1['media']} dias")
                            c2.metric("Mediana", f"{r1['mediana']} dias")
                            c3.metric("Mínimo", f"{r1['min']} dias")
                            c4.metric("Máximo", f"{r1['max']} dias")

                            st.plotly_chart(
                                days_histogram(r1["df"], title=f"Dias entre entrada na base e compra de {produto_analise}"),
                                use_container_width=True,
                            )

                            with st.expander("Ver tabela detalhada"):
                                cols_show = [c for c in ["lead_name", "lead_email", "data_lead", "data_compra", "dias"] if c in r1["df"].columns]
                                st.dataframe(r1["df"][cols_show], use_container_width=True, hide_index=True)

                # ── Análise 2 — Média de tags por comprador ──────────────────
                with an2:
                    if _sem_leads or _sem_vendas:
                        st.info("Necessário ter leads e vendas carregados.")
                    else:
                        r2 = analysis_avg_tags_per_buyer(df_leads, df_vendas, produto_analise, _status)

                        if "error" in r2:
                            st.error(r2["error"])
                        elif r2["count"] == 0:
                            st.warning("Nenhum comprador deste produto encontrado na base de leads.")
                        else:
                            st.caption(f"Compradores de **{produto_analise}** com dados de tag: **{r2['count']}**")
                            t1, t2, t3 = st.columns(3)
                            t1.metric("Média de tags", f"{r2['media']}")
                            t2.metric("Mediana", f"{r2['mediana']}")
                            t3.metric("Máximo", f"{r2['max']}")

                            st.plotly_chart(
                                tags_distribution_bar(r2["dist"]),
                                use_container_width=True,
                            )

                            with st.expander("Ver tabela detalhada"):
                                cols_t = [c for c in ["lead_name", "lead_email", "num_tags"] if c in r2["df"].columns]
                                st.dataframe(r2["df"][cols_t], use_container_width=True, hide_index=True)

                # ── Análise 3 — Anúncios que trouxeram mais leads ────────────
                with an3:
                    if _sem_leads:
                        st.info("Necessário ter arquivo de leads carregado.")
                    else:
                        r3 = analysis_leads_by_utm_content(df_leads)

                        if r3.empty:
                            st.warning("Coluna utm_content não encontrada ou sem dados.")
                        else:
                            st.caption(f"**{len(r3)}** valores distintos de utm_content na base de leads.")
                            top_n_3 = st.slider("Exibir top N anúncios", 5, min(50, len(r3)), min(25, len(r3)), key="top_n_3")
                            st.plotly_chart(utm_content_bar(r3, top_n=top_n_3), use_container_width=True)

                            with st.expander("Ver tabela completa"):
                                st.dataframe(r3, use_container_width=True, hide_index=True)

                # ── Análise 4 — Primeira entrada → vendas ────────────────────
                with an4:
                    if _sem_leads or _sem_vendas:
                        st.info("Necessário ter leads e vendas carregados.")
                    else:
                        r4 = analysis_first_entry_to_sales(df_leads, df_vendas, produto_analise, _status)

                        if "error" in r4:
                            st.error(r4["error"])
                        elif not r4:
                            st.warning("Nenhum dado encontrado.")
                        else:
                            st.caption(
                                "Análise baseada na **primeira entrada** de cada lead na base "
                                "(menor data de lead_register). Mostra qual tag ou formulário de origem "
                                "gerou mais compradores."
                            )
                            sub4a, sub4b = st.tabs(["Por primeira tag", "Por formulário de origem"])

                            with sub4a:
                                by_tag = r4.get("by_tag", pd.DataFrame())
                                if by_tag.empty:
                                    st.info("Sem dados de tag.")
                                else:
                                    top_n_4t = st.slider("Top N tags", 5, min(30, len(by_tag)), min(20, len(by_tag)), key="top_n_4t")
                                    st.plotly_chart(
                                        first_entry_bar(by_tag, "tag_name", f"Primeira tag → compra de {produto_analise}", top_n=top_n_4t),
                                        use_container_width=True,
                                    )
                                    st.dataframe(by_tag, use_container_width=True, hide_index=True)

                            with sub4b:
                                by_form = r4.get("by_form", pd.DataFrame())
                                if by_form.empty:
                                    st.info("Sem dados de formulário.")
                                else:
                                    top_n_4f = st.slider("Top N formulários", 5, min(30, len(by_form)), min(20, len(by_form)), key="top_n_4f")
                                    st.plotly_chart(
                                        first_entry_bar(by_form, "lead_register_form", f"Formulário de origem → compra de {produto_analise}", top_n=top_n_4f),
                                        use_container_width=True,
                                    )
                                    st.dataframe(by_form, use_container_width=True, hide_index=True)

                # ── Análise 5 — Funil por UTM ────────────────────────────────
                with an5:
                    if _sem_leads:
                        st.info("Necessário ter arquivo de leads carregado.")
                    else:
                        st.caption(
                            "Selecione a dimensão UTM e filtre os valores para ver leads e vendas por canal. "
                            "A atribuição usa a **primeira entrada** de cada lead."
                        )

                        u1, u2 = st.columns([1, 3])
                        with u1:
                            utm_dim = st.selectbox(
                                "Dimensão UTM",
                                options=["utm_content", "utm_campaign", "utm_medium"],
                                key="utm_dim",
                            )
                        with u2:
                            utm_vals_disponíveis = get_utm_values(df_leads, utm_dim)
                            utm_filter = st.multiselect(
                                f"Filtrar por {utm_dim} (deixe vazio para ver todos)",
                                options=utm_vals_disponíveis,
                                key="utm_filter",
                                placeholder="Buscar...",
                            )

                        r5 = analysis_utm_funnel(
                            df_leads,
                            df_vendas,
                            utm_col=utm_dim,
                            product=produto_analise if not _sem_vendas else None,
                            status=_status,
                            filter_values=utm_filter or None,
                        )

                        if r5.empty:
                            st.warning("Nenhum resultado com os filtros selecionados.")
                        else:
                            top_n_5 = st.slider("Exibir top N", 5, min(50, len(r5)), min(20, len(r5)), key="top_n_5")
                            st.plotly_chart(
                                utm_funnel_bar(r5, utm_col=utm_dim, top_n=top_n_5),
                                use_container_width=True,
                            )

                            with st.expander("Ver tabela completa"):
                                st.dataframe(r5, use_container_width=True, hide_index=True)
