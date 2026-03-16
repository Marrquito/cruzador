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
        "mediana": int(tags_per_buyer["num_tags"].median()),
        "max": int(tags_per_buyer["num_tags"].max()),
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
    Verifica se esse lead comprou o produto e agrupa por tag e formulário.
    """
    dl = _prep_leads(df_leads)
    dv = _prep_sales(df_vendas)

    if "lead_register" not in dl.columns:
        return {"error": "Coluna 'lead_register' não encontrada."}

    buyers = _buyer_emails(dv, product, status)
    first = _first_entry_per_email(dl).copy()

    if first.empty:
        return {}

    first["comprou"] = first["_email_norm"].isin(buyers).astype(int)
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
        buyers = _buyer_emails(dv, product, status)
        dl_first = dl_first.copy()
        dl_first["comprou"] = dl_first["_email_norm"].isin(buyers).astype(int)

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
