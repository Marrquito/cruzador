import pandas as pd


def _effective_date(row: pd.Series) -> pd.Timestamp | None:
    """Usa data de aprovação quando disponível, senão data do pedido."""
    aprov = row.get("Data de Aprovação")
    pedido = row.get("Data do Pedido")
    if pd.notna(aprov):
        return aprov
    if pd.notna(pedido):
        return pedido
    return None


def filter_by_status(df: pd.DataFrame, status_list: list[str]) -> pd.DataFrame:
    if not status_list or "Status" not in df.columns:
        return df
    return df[df["Status"].isin(status_list)]


def crossref(
    df: pd.DataFrame,
    group_a: list[str],
    product_b: str,
) -> dict:
    """
    Cruza compradores do Grupo A com compradores do Produto B.

    Retorna um dict com:
      - summary: métricas gerais
      - intersection: DataFrame dos compradores que compram nos dois grupos
      - only_a: compradores que só compraram Grupo A
      - only_b: compradores que só compraram Produto B
    """
    if "_id_comprador" not in df.columns:
        raise ValueError("Identificador de comprador não encontrado. Verifique o CSV.")

    df = df.copy()
    df["_data_efetiva"] = df.apply(_effective_date, axis=1)

    # Compras do Grupo A
    mask_a = df["Nome do Produto"].isin(group_a)
    df_a = df[mask_a]

    # Compras do Produto B
    mask_b = df["Nome do Produto"] == product_b
    df_b = df[mask_b]

    buyers_a = set(df_a["_id_comprador"].dropna().unique())
    buyers_b = set(df_b["_id_comprador"].dropna().unique())

    buyers_both = buyers_a & buyers_b
    only_a = buyers_a - buyers_b
    only_b = buyers_b - buyers_a

    # Para cada comprador em ambos, calcular datas e sequência
    rows = []
    for buyer_id in buyers_both:
        compras_a = df_a[df_a["_id_comprador"] == buyer_id]
        compras_b = df_b[df_b["_id_comprador"] == buyer_id]

        data_a = compras_a["_data_efetiva"].dropna().min()
        data_b = compras_b["_data_efetiva"].dropna().min()

        if pd.notna(data_a) and pd.notna(data_b):
            if data_a < data_b:
                sequencia = "Comprou Grupo A primeiro"
                dias_entre = (data_b - data_a).days
            elif data_b < data_a:
                sequencia = "Comprou Produto B primeiro"
                dias_entre = (data_a - data_b).days
            else:
                sequencia = "Mesma data"
                dias_entre = 0
        else:
            sequencia = "Data indisponível"
            dias_entre = None

        # Pega nome e email do primeiro registro
        ref = compras_a.iloc[0]
        rows.append(
            {
                "Nome": ref.get("Nome do Comprador", ""),
                "CPF/CNPJ": ref.get("CPF/CNPJ Comprador", ""),
                "E-mail": ref.get("E-mail do Comprador", ""),
                "Produto Grupo A comprado": compras_a["Nome do Produto"].iloc[0],
                f"Data {_short_label(group_a)}": data_a,
                f"Data {product_b}": data_b,
                "Sequência": sequencia,
                "Dias entre compras": dias_entre,
            }
        )

    intersection_df = pd.DataFrame(rows)

    # Separa quem comprou B antes de A — esses são desconsiderados do funil A→B
    if not intersection_df.empty:
        b_first_df = intersection_df[
            intersection_df["Sequência"] == "Comprou Produto B primeiro"
        ].reset_index(drop=True)
        intersection_df = intersection_df[
            intersection_df["Sequência"] != "Comprou Produto B primeiro"
        ].reset_index(drop=True)
    else:
        b_first_df = pd.DataFrame()

    # Detalhes de só A (sem B)
    only_a_df = (
        df_a[df_a["_id_comprador"].isin(only_a)][
            ["Nome do Comprador", "CPF/CNPJ Comprador", "E-mail do Comprador",
             "Nome do Produto", "_data_efetiva"]
        ]
        .rename(columns={"_data_efetiva": "Data"})
        .drop_duplicates(subset=["E-mail do Comprador", "Nome do Produto"])
        .reset_index(drop=True)
    )

    # Detalhes de só B (sem A)
    only_b_df = (
        df_b[df_b["_id_comprador"].isin(only_b)][
            ["Nome do Comprador", "CPF/CNPJ Comprador", "E-mail do Comprador",
             "Nome do Produto", "_data_efetiva"]
        ]
        .rename(columns={"_data_efetiva": "Data"})
        .drop_duplicates(subset=["E-mail do Comprador"])
        .reset_index(drop=True)
    )

    # Sequência summary — apenas dos válidos (A→B)
    seq_counts = (
        intersection_df["Sequência"].value_counts().to_dict()
        if not intersection_df.empty
        else {}
    )

    # Tempo médio entre compras — apenas conversões A→B com dias disponíveis
    dias_validos = (
        intersection_df["Dias entre compras"].dropna()
        if not intersection_df.empty
        else pd.Series(dtype=float)
    )
    media_dias = round(dias_validos.mean(), 1) if not dias_validos.empty else None

    convertidos = len(intersection_df)

    summary = {
        "total_grupo_a": len(buyers_a),
        "total_produto_b": len(buyers_b),
        "compraram_ambos": convertidos,
        "b_comprou_primeiro": len(b_first_df),
        "so_grupo_a": len(only_a),
        "so_produto_b": len(only_b),
        "taxa_conversao_pct": (
            round(convertidos / len(buyers_a) * 100, 1) if buyers_a else 0
        ),
        "sequencia": seq_counts,
        "media_dias_entre_compras": media_dias,
    }

    return {
        "summary": summary,
        "intersection": intersection_df,
        "b_first": b_first_df,
        "only_a": only_a_df,
        "only_b": only_b_df,
    }


def _short_label(products: list[str]) -> str:
    if len(products) == 1:
        return products[0][:30]
    return "Grupo A"
