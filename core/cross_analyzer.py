"""
Análises cruzadas entre tabela de leads (TAGS-E-UTMS) e tabela de vendas (VENDAS-BA).
Chave de cruzamento: email (normalizado).
"""
import pandas as pd

UTM_COLS = ["utm_source", "utm_campaign", "utm_medium", "utm_content"]


# ── Helpers internos ──────────────────────────────────────────────────────────

def _norm_email(val) -> str:
    if pd.isna(val):
        return ""
    return str(val).strip().lower()


def _prep_leads(df: pd.DataFrame) -> pd.DataFrame:
    dl = df.copy()
    dl["_email_norm"] = dl["lead_email"].apply(_norm_email) if "lead_email" in dl.columns else ""
    for col in UTM_COLS:
        if col in dl.columns:
            dl[col] = dl[col].apply(lambda x: str(x).strip() if pd.notna(x) else x)
    return dl


def _prep_sales(df: pd.DataFrame) -> pd.DataFrame:
    dv = df.copy()
    dv["_email_norm"] = (
        dv["E-mail do Comprador"].apply(_norm_email)
        if "E-mail do Comprador" in dv.columns
        else ""
    )
    return dv


def _first_entry_per_email(dl: pd.DataFrame) -> pd.DataFrame:
    """Retorna a primeira linha por email (menor lead_register)."""
    dl = dl[dl["_email_norm"] != ""]
    if "lead_register" in dl.columns:
        return (
            dl.sort_values("lead_register")
            .drop_duplicates(subset=["_email_norm"], keep="first")
        )
    return dl.drop_duplicates(subset=["_email_norm"], keep="first")


def _buyer_emails(dv: pd.DataFrame, product: str, status: list[str] | None) -> set[str]:
    mask = dv["Nome do Produto"] == product
    if status:
        mask &= dv["Status"].isin(status)
    return set(dv[mask & (dv["_email_norm"] != "")]["_email_norm"].unique())


def _display_names(dl: pd.DataFrame) -> pd.DataFrame:
    """Retorna mapeamento email → nome/email para exibição."""
    cols = ["_email_norm"]
    if "lead_name" in dl.columns:
        cols.append("lead_name")
    if "lead_email" in dl.columns:
        cols.append("lead_email")
    return dl[cols].drop_duplicates(subset=["_email_norm"])


# ── Análise 1 — Tempo médio lead → compra ────────────────────────────────────

def analysis_lead_to_purchase(
    df_leads: pd.DataFrame,
    df_vendas: pd.DataFrame,
    product: str,
    status: list[str] | None = None,
) -> dict:
    """
    Calcula o tempo (em dias) entre a primeira entrada do lead na base
    e a data de aprovação da compra do produto selecionado.
    """
    dl = _prep_leads(df_leads)
    dv = _prep_sales(df_vendas)

    if "lead_register" not in dl.columns:
        return {"error": "Coluna 'lead_register' não encontrada na tabela de leads."}

    date_col = "Data de Aprovação" if "Data de Aprovação" in dv.columns else "Data do Pedido"

    mask = dv["Nome do Produto"] == product
    if status:
        mask &= dv["Status"].isin(status)

    sales = (
        dv[mask & (dv["_email_norm"] != "")]
        .dropna(subset=[date_col])
        [["_email_norm", date_col]]
        .groupby("_email_norm")[date_col].min()
        .reset_index()
        .rename(columns={date_col: "data_compra"})
    )

    first_lead = (
        dl[dl["_email_norm"] != ""]
        .dropna(subset=["lead_register"])
        .groupby("_email_norm")["lead_register"].min()
        .reset_index()
        .rename(columns={"lead_register": "data_lead"})
    )

    merged = sales.merge(first_lead, on="_email_norm", how="inner")
    merged["dias"] = (merged["data_compra"] - merged["data_lead"]).dt.days
    merged = merged[merged["dias"] >= 0]  # Ignora inconsistências de data

    if merged.empty:
        return {"count": 0, "media": None, "mediana": None, "df": merged}

    merged = merged.merge(_display_names(dl), on="_email_norm", how="left")

    return {
        "count": len(merged),
        "media": round(merged["dias"].mean(), 1),
        "mediana": int(merged["dias"].median()),
        "min": int(merged["dias"].min()),
        "max": int(merged["dias"].max()),
        "df": merged.sort_values("dias"),
    }


# ── Análise 1b — Visão geral: tempo lead→compra para todos os produtos ─────────

def analysis_lead_to_purchase_all(
    df_leads: pd.DataFrame,
    df_vendas: pd.DataFrame,
    status: list[str] | None = None,
) -> pd.DataFrame:
    """
    Para cada produto, calcula contagem, mediana, min e max de dias entre
    a primeira entrada na base de leads e a primeira compra do produto.
    Retorna um DataFrame ordenado por mediana (menor primeiro).
    """
    dl = _prep_leads(df_leads)
    dv = _prep_sales(df_vendas)

    if "lead_register" not in dl.columns or "Nome do Produto" not in dv.columns:
        return pd.DataFrame()

    date_col = "Data de Aprovação" if "Data de Aprovação" in dv.columns else "Data do Pedido"

    dv_f = dv.copy()
    if status:
        dv_f = dv_f[dv_f["Status"].isin(status)]

    # Primeira data de compra por email × produto
    sales = (
        dv_f[dv_f["_email_norm"] != ""]
        .dropna(subset=[date_col])
        .groupby(["_email_norm", "Nome do Produto"])[date_col]
        .min()
        .reset_index()
        .rename(columns={date_col: "data_compra"})
    )

    # Primeira entrada na base de leads por email
    first_lead = (
        dl[dl["_email_norm"] != ""]
        .dropna(subset=["lead_register"])
        .groupby("_email_norm")["lead_register"]
        .min()
        .reset_index()
        .rename(columns={"lead_register": "data_lead"})
    )

    merged = sales.merge(first_lead, on="_email_norm", how="inner")
    merged["dias"] = (merged["data_compra"] - merged["data_lead"]).dt.days
    merged = merged[merged["dias"] >= 0]

    if merged.empty:
        return pd.DataFrame()

    result = (
        merged.groupby("Nome do Produto")["dias"]
        .agg(
            leads_que_compraram="count",
            mediana=lambda s: int(s.median()),
            minimo=lambda s: int(s.min()),
            maximo=lambda s: int(s.max()),
            media=lambda s: round(s.mean(), 1),
        )
        .reset_index()
        .sort_values("mediana")
        .reset_index(drop=True)
    )

    return result


# ── Análise 2 — Média de tags por comprador ───────────────────────────────────

def analysis_avg_tags_per_buyer(
    df_leads: pd.DataFrame,
    df_vendas: pd.DataFrame,
    product: str,
    status: list[str] | None = None,
) -> dict:
    """
    Para compradores do produto selecionado, conta quantas tags distintas
    cada um possui na tabela de leads.
    """
    dl = _prep_leads(df_leads)
    dv = _prep_sales(df_vendas)

    if "tag_name" not in dl.columns:
        return {"error": "Coluna 'tag_name' não encontrada na tabela de leads."}

    buyers = _buyer_emails(dv, product, status)
    if not buyers:
        return {"count": 0, "media": None, "df": pd.DataFrame()}

    dl_buyers = dl[dl["_email_norm"].isin(buyers)]

    tags_per_buyer = (
        dl_buyers.dropna(subset=["tag_name"])
        .groupby("_email_norm")["tag_name"]
        .nunique()
        .reset_index()
        .rename(columns={"tag_name": "num_tags"})
    )
    tags_per_buyer = tags_per_buyer.merge(_display_names(dl), on="_email_norm", how="left")

    if tags_per_buyer.empty:
        return {"count": 0}

    dist = (
        tags_per_buyer["num_tags"]
        .value_counts()
        .sort_index()
        .reset_index()
        .rename(columns={"num_tags": "qtde_tags", "count": "compradores"})
    )

    return {
        "count": len(tags_per_buyer),
        "media": round(tags_per_buyer["num_tags"].mean(), 2),
        "mediana": int(tags_per_buyer["num_tags"].median()) if pd.notna(tags_per_buyer["num_tags"].median()) else 0,
        "max": int(tags_per_buyer["num_tags"].max()) if pd.notna(tags_per_buyer["num_tags"].max()) else 0,
        "dist": dist,
        "df": tags_per_buyer.sort_values("num_tags", ascending=False),
    }


# ── Análise 3 — Anúncios (utm_content) que trouxeram mais leads ───────────────

def analysis_leads_by_utm_content(df_leads: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa leads por utm_content e conta emails únicos.
    """
    dl = _prep_leads(df_leads)

    if "utm_content" not in dl.columns:
        return pd.DataFrame()

    id_col = "lead_id" if "lead_id" in dl.columns else "_email_norm"

    return (
        dl.assign(
            utm_content=lambda d: d["utm_content"]
            .fillna("(não rastreado)")
            .apply(lambda x: str(x).strip())
        )
        .groupby("utm_content")
        .agg(
            entradas=pd.NamedAgg(column=id_col, aggfunc="count"),
            emails_unicos=pd.NamedAgg(column="_email_norm", aggfunc="nunique"),
        )
        .reset_index()
        .sort_values("emails_unicos", ascending=False)
        .reset_index(drop=True)
    )


# ── Análise 4 — Primeira entrada (tag/form) → vendas ─────────────────────────

def analysis_first_entry_to_sales(
    df_leads: pd.DataFrame,
    df_vendas: pd.DataFrame,
    product: str,
    status: list[str] | None = None,
) -> dict:
    """
    Para cada lead, pega sua primeira entrada na base (menor lead_register).
    Verifica se esse lead comprou o produto APÓS essa data de entrada e
    agrupa por tag e formulário.

    Só conta como conversão se data da compra >= data da primeira entrada,
    evitando inflar tags com compradores que já haviam comprado antes de entrar.
    """
    dl = _prep_leads(df_leads)
    dv = _prep_sales(df_vendas)

    if "lead_register" not in dl.columns:
        return {"error": "Coluna 'lead_register' não encontrada."}

    first = _first_entry_per_email(dl).copy()

    if first.empty:
        return {}

    # Primeira compra do produto por email (respeitando filtro de status)
    date_col = "Data de Aprovação" if "Data de Aprovação" in dv.columns else "Data do Pedido"
    mask = dv["Nome do Produto"] == product
    if status:
        mask &= dv["Status"].isin(status)

    first_purchase = (
        dv[mask & (dv["_email_norm"] != "")]
        .dropna(subset=[date_col])
        .groupby("_email_norm")[date_col]
        .min()
        .reset_index()
        .rename(columns={date_col: "data_compra"})
    )

    # Join e restrição temporal: só conta se comprou DEPOIS (ou no mesmo dia) da entrada
    first = first.merge(first_purchase, on="_email_norm", how="left")
    first["comprou"] = (
        first["data_compra"].notna()
        & (first["data_compra"] >= first["lead_register"])
    ).astype(int)

    result = {}

    # Por primeira tag
    if "tag_name" in first.columns:
        by_tag = (
            first.fillna({"tag_name": "(sem tag)"})
            .groupby("tag_name")
            .agg(total_leads=("_email_norm", "count"), compradores=("comprou", "sum"))
            .reset_index()
        )
        by_tag["compradores"] = by_tag["compradores"].astype(int)
        by_tag["taxa_conversao_pct"] = (
            by_tag["compradores"] / by_tag["total_leads"] * 100
        ).round(1)
        result["by_tag"] = by_tag.sort_values("compradores", ascending=False).reset_index(drop=True)

    # Por formulário de origem
    if "lead_register_form" in first.columns:
        by_form = (
            first.fillna({"lead_register_form": "(sem formulário)"})
            .assign(lead_register_form=lambda d: "Form " + d["lead_register_form"].astype(str))
            .groupby("lead_register_form")
            .agg(total_leads=("_email_norm", "count"), compradores=("comprou", "sum"))
            .reset_index()
        )
        by_form["compradores"] = by_form["compradores"].astype(int)
        by_form["taxa_conversao_pct"] = (
            by_form["compradores"] / by_form["total_leads"] * 100
        ).round(1)
        result["by_form"] = by_form.sort_values("compradores", ascending=False).reset_index(drop=True)

    return result


# ── Análise 5 — Funil por UTM (leads + vendas por dimensão) ──────────────────

def analysis_utm_funnel(
    df_leads: pd.DataFrame,
    df_vendas: pd.DataFrame,
    utm_col: str,
    product: str | None = None,
    status: list[str] | None = None,
    filter_values: list[str] | None = None,
) -> pd.DataFrame:
    """
    Para a dimensão UTM selecionada (utm_content, utm_campaign ou utm_medium),
    retorna: emails únicos de leads, entradas totais, compradores do produto
    e taxa de conversão.
    Usa a PRIMEIRA entrada por email para atribuição.
    """
    dl = _prep_leads(df_leads)
    dv = _prep_sales(df_vendas)

    if utm_col not in dl.columns:
        return pd.DataFrame()

    dl[utm_col] = dl[utm_col].fillna("(não rastreado)").apply(lambda x: str(x).strip())

    # Atribui pelo primeiro registro por email
    dl_first = _first_entry_per_email(dl).copy()

    if filter_values:
        dl_first = dl_first[dl_first[utm_col].isin(filter_values)]

    leads_agg = (
        dl_first.groupby(utm_col)
        .agg(
            emails_unicos=("_email_norm", "nunique"),
            entradas=("_email_norm", "count"),
        )
        .reset_index()
    )

    if product:
        date_col = "Data de Aprovação" if "Data de Aprovação" in dv.columns else "Data do Pedido"
        mask = dv["Nome do Produto"] == product
        if status:
            mask &= dv["Status"].isin(status)

        first_purchase = (
            dv[mask & (dv["_email_norm"] != "")]
            .dropna(subset=[date_col])
            .groupby("_email_norm")[date_col]
            .min()
            .reset_index()
            .rename(columns={date_col: "data_compra"})
        )

        dl_first = dl_first.copy().merge(first_purchase, on="_email_norm", how="left")
        dl_first["comprou"] = (
            dl_first["data_compra"].notna()
            & (dl_first["data_compra"] >= dl_first["lead_register"])
        ).astype(int)

        buyers_agg = (
            dl_first.groupby(utm_col)["comprou"]
            .sum()
            .astype(int)
            .reset_index()
            .rename(columns={"comprou": "compradores"})
        )

        result = leads_agg.merge(buyers_agg, on=utm_col, how="left")
        result["compradores"] = result["compradores"].fillna(0).astype(int)
        result["taxa_conversao_pct"] = (
            result["compradores"] / result["emails_unicos"] * 100
        ).round(1)
    else:
        result = leads_agg

    return result.sort_values("emails_unicos", ascending=False).reset_index(drop=True)


# ── Análise 6 — Comportamento antes/depois de uma tag ────────────────────────

def analysis_behavior_around_tag(
    df_leads: pd.DataFrame,
    df_vendas: pd.DataFrame,
    tag: str,
    status: list[str] | None = None,
) -> dict:
    """
    Para cada lead que possui a tag selecionada, compara suas compras ANTES e
    DEPOIS da data em que a tag foi atribuída pela primeira vez.

    Retorna um dict com:
      - count: total de leads com a tag
      - total_com_compra_antes / total_com_compra_depois
      - media_compras_antes / media_compras_depois
      - behavior_counts: DataFrame com breakdown comportamental
      - products_after / products_before: top produtos por categoria
      - summary_per_person: DataFrame detalhado por lead
    """
    dl = _prep_leads(df_leads)
    dv = _prep_sales(df_vendas)

    if "tag_name" not in dl.columns or "lead_register" not in dl.columns:
        return {"error": "Colunas 'tag_name' e 'lead_register' necessárias na tabela de leads."}

    if "Nome do Produto" not in dv.columns:
        return {"error": "Coluna 'Nome do Produto' não encontrada na tabela de vendas."}

    # Leads com a tag selecionada
    dl_tag = dl[dl["tag_name"] == tag].copy()
    if dl_tag.empty:
        return {"count": 0}

    # Primeira vez que a tag foi atribuída por email
    first_tag = (
        dl_tag[dl_tag["_email_norm"] != ""]
        .dropna(subset=["lead_register"])
        .groupby("_email_norm")["lead_register"]
        .min()
        .reset_index()
        .rename(columns={"lead_register": "data_tag"})
    )

    if first_tag.empty:
        return {"count": 0}

    # Filtra vendas por status e emails presentes
    dv_f = dv.copy()
    if status:
        dv_f = dv_f[dv_f["Status"].isin(status)]

    date_col = "Data de Aprovação" if "Data de Aprovação" in dv_f.columns else "Data do Pedido"

    sales = (
        dv_f[
            dv_f["_email_norm"].isin(first_tag["_email_norm"])
            & (dv_f["_email_norm"] != "")
        ]
        .dropna(subset=[date_col])
        .merge(first_tag, on="_email_norm", how="left")
    )

    sales_before = sales[sales[date_col] < sales["data_tag"]]
    sales_after = sales[sales[date_col] >= sales["data_tag"]]

    # Contagem de compras por pessoa
    def _count_per_person(df_slice: pd.DataFrame, col_name: str) -> pd.DataFrame:
        if df_slice.empty:
            return pd.DataFrame(columns=["_email_norm", col_name])
        return (
            df_slice.groupby("_email_norm")["Nome do Produto"]
            .count()
            .reset_index()
            .rename(columns={"Nome do Produto": col_name})
        )

    before_counts = _count_per_person(sales_before, "compras_antes")
    after_counts = _count_per_person(sales_after, "compras_depois")

    per_person = (
        first_tag
        .merge(before_counts, on="_email_norm", how="left")
        .merge(after_counts, on="_email_norm", how="left")
    )
    per_person["compras_antes"] = per_person["compras_antes"].fillna(0).astype(int)
    per_person["compras_depois"] = per_person["compras_depois"].fillna(0).astype(int)

    per_person = per_person.merge(_display_names(dl), on="_email_norm", how="left")

    # Classificação comportamental
    def _classify(row) -> str:
        b, a = row["compras_antes"], row["compras_depois"]
        if b == 0 and a == 0:
            return "Nunca comprou"
        if b > 0 and a == 0:
            return "Comprou apenas antes da tag"
        if b == 0 and a > 0:
            return "Comprou apenas depois da tag"
        return "Comprou antes e depois da tag"

    per_person["comportamento"] = per_person.apply(_classify, axis=1)

    behavior_counts = (
        per_person["comportamento"]
        .value_counts()
        .reset_index()
    )
    behavior_counts.columns = ["comportamento", "leads"]

    # Top produtos comprados antes e depois
    def _top_products(df_slice: pd.DataFrame) -> pd.DataFrame:
        if df_slice.empty:
            return pd.DataFrame(columns=["Nome do Produto", "compradores", "transacoes"])
        return (
            df_slice.groupby("Nome do Produto")
            .agg(compradores=("_email_norm", "nunique"), transacoes=("_email_norm", "count"))
            .reset_index()
            .sort_values("compradores", ascending=False)
            .reset_index(drop=True)
        )

    return {
        "count": len(first_tag),
        "total_com_compra_antes": int((per_person["compras_antes"] > 0).sum()),
        "total_com_compra_depois": int((per_person["compras_depois"] > 0).sum()),
        "media_compras_antes": round(per_person["compras_antes"].mean(), 2),
        "media_compras_depois": round(per_person["compras_depois"].mean(), 2),
        "behavior_counts": behavior_counts,
        "products_after": _top_products(sales_after),
        "products_before": _top_products(sales_before),
        "per_person": per_person.sort_values("compras_depois", ascending=False),
    }


# ── Análise 7 — Comportamento antes/depois de um filtro de leads ─────────────

def analysis_behavior_around_filter(
    dl_filtered: pd.DataFrame,
    df_vendas: pd.DataFrame,
    status: list[str] | None = None,
) -> dict:
    """
    Recebe um DataFrame de leads já filtrado (qualquer combinação de filtros:
    tag, UTM, formulário, data…) e, para cada email único nesse conjunto,
    usa a primeira data de lead_register como "data de entrada no filtro".

    Compara as compras desses leads ANTES e DEPOIS dessa data de entrada,
    mostrando quais produtos foram adquiridos em cada período.
    """
    dl = _prep_leads(dl_filtered)
    dv = _prep_sales(df_vendas)

    if "lead_register" not in dl.columns:
        return {"error": "Coluna 'lead_register' não encontrada na tabela de leads."}
    if "Nome do Produto" not in dv.columns:
        return {"error": "Coluna 'Nome do Produto' não encontrada na tabela de vendas."}

    first_entry = (
        dl[dl["_email_norm"] != ""]
        .dropna(subset=["lead_register"])
        .groupby("_email_norm")["lead_register"]
        .min()
        .reset_index()
        .rename(columns={"lead_register": "data_entrada"})
    )

    if first_entry.empty:
        return {"count": 0}

    dv_f = dv.copy()
    if status:
        dv_f = dv_f[dv_f["Status"].isin(status)]

    date_col = "Data de Aprovação" if "Data de Aprovação" in dv_f.columns else "Data do Pedido"

    sales = (
        dv_f[
            dv_f["_email_norm"].isin(first_entry["_email_norm"])
            & (dv_f["_email_norm"] != "")
        ]
        .dropna(subset=[date_col])
        .merge(first_entry, on="_email_norm", how="left")
    )

    sales_before = sales[sales[date_col] < sales["data_entrada"]]
    sales_after  = sales[sales[date_col] >= sales["data_entrada"]]

    def _count_per_person(df_slice: pd.DataFrame, col_name: str) -> pd.DataFrame:
        if df_slice.empty:
            return pd.DataFrame(columns=["_email_norm", col_name])
        return (
            df_slice.groupby("_email_norm")["Nome do Produto"]
            .count()
            .reset_index()
            .rename(columns={"Nome do Produto": col_name})
        )

    per_person = (
        first_entry
        .merge(_count_per_person(sales_before, "compras_antes"), on="_email_norm", how="left")
        .merge(_count_per_person(sales_after,  "compras_depois"), on="_email_norm", how="left")
    )
    per_person["compras_antes"]  = per_person["compras_antes"].fillna(0).astype(int)
    per_person["compras_depois"] = per_person["compras_depois"].fillna(0).astype(int)

    def _classify(row) -> str:
        b, a = row["compras_antes"], row["compras_depois"]
        if b == 0 and a == 0: return "Nunca comprou"
        if b > 0 and a == 0:  return "Comprou apenas antes da entrada"
        if b == 0 and a > 0:  return "Comprou apenas depois da entrada"
        return "Comprou antes e depois da entrada"

    per_person["comportamento"] = per_person.apply(_classify, axis=1)

    behavior_counts = per_person["comportamento"].value_counts().reset_index()
    behavior_counts.columns = ["comportamento", "leads"]

    def _top_products(df_slice: pd.DataFrame) -> pd.DataFrame:
        if df_slice.empty:
            return pd.DataFrame(columns=["Nome do Produto", "compradores", "transacoes"])
        return (
            df_slice.groupby("Nome do Produto")
            .agg(compradores=("_email_norm", "nunique"), transacoes=("_email_norm", "count"))
            .reset_index()
            .sort_values("compradores", ascending=False)
            .reset_index(drop=True)
        )

    return {
        "count": len(first_entry),
        "total_com_compra_antes":  int((per_person["compras_antes"]  > 0).sum()),
        "total_com_compra_depois": int((per_person["compras_depois"] > 0).sum()),
        "media_compras_antes":  round(per_person["compras_antes"].mean(),  2),
        "media_compras_depois": round(per_person["compras_depois"].mean(), 2),
        "behavior_counts":  behavior_counts,
        "products_before":  _top_products(sales_before),
        "products_after":   _top_products(sales_after),
        "per_person": per_person.sort_values("compras_depois", ascending=False),
    }


# ── Helper de listagem de valores UTM disponíveis ────────────────────────────

def get_utm_values(df_leads: pd.DataFrame, utm_col: str) -> list[str]:
    """Retorna lista ordenada de valores únicos de uma coluna UTM (sem nulos)."""
    if utm_col not in df_leads.columns:
        return []
    return sorted(
        df_leads[utm_col]
        .dropna()
        .apply(lambda x: str(x).strip())
        .unique()
        .tolist()
    )


# ── Análise 6 — Tags mais comuns entre compradores ────────────────────────────

def analysis_buyer_tags(
    df_leads: pd.DataFrame,
    df_vendas: pd.DataFrame,
    product: str,
    status: list[str] | None = None,
) -> dict:
    """
    Para os compradores de `product`, calcula quais tags eles possuem
    e com que frequência, retornando a contagem e % sobre o total de compradores.
    """
    if "tag_name" not in df_leads.columns:
        return {"error": "Coluna 'tag_name' não encontrada nos leads."}
    if "Nome do Produto" not in df_vendas.columns:
        return {"error": "Coluna 'Nome do Produto' não encontrada nas vendas."}

    dl = _prep_leads(df_leads)
    dv = _prep_sales(df_vendas)

    buyer_emails = _buyer_emails(dv, product, status)
    if not buyer_emails:
        return {"count": 0, "df": pd.DataFrame()}

    # Leads que são compradores do produto
    buyers_leads = dl[dl["_email_norm"].isin(buyer_emails) & (dl["_email_norm"] != "")]

    total_buyers = buyers_leads["_email_norm"].nunique()
    if total_buyers == 0:
        return {"count": 0, "df": pd.DataFrame()}

    # Conta compradores únicos por tag (um comprador pode ter a tag em múltiplas linhas)
    tag_counts = (
        buyers_leads[buyers_leads["tag_name"].notna()]
        .groupby("tag_name")["_email_norm"]
        .nunique()
        .reset_index()
        .rename(columns={"_email_norm": "compradores"})
    )
    tag_counts["pct"] = (tag_counts["compradores"] / total_buyers * 100).round(1)
    tag_counts = tag_counts.sort_values("compradores", ascending=False).reset_index(drop=True)

    return {
        "count": total_buyers,
        "df": tag_counts,
    }
