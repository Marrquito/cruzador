import pandas as pd
import plotly.graph_objects as go
import plotly.express as px


def funnel_chart(summary: dict, group_a_label: str, product_b_label: str) -> go.Figure:
    labels = [
        f"Compradores do {group_a_label}",
        f"Também compraram {product_b_label}",
    ]
    values = [summary["total_grupo_a"], summary["compraram_ambos"]]

    fig = go.Figure(
        go.Funnel(
            y=labels,
            x=values,
            textinfo="value+percent initial",
            marker_color=["#4C6EF5", "#37B24D"],
        )
    )
    fig.update_layout(
        title="Funil de Conversão",
        margin=dict(l=20, r=20, t=50, b=20),
        height=300,
    )
    return fig


def sequence_pie(seq_counts: dict) -> go.Figure:
    if not seq_counts:
        return go.Figure()

    labels = list(seq_counts.keys())
    values = list(seq_counts.values())

    colors = {
        "Comprou Grupo A primeiro": "#4C6EF5",
        "Comprou Produto B primeiro": "#F59F00",
        "Mesma data": "#74C0FC",
        "Data indisponível": "#CED4DA",
    }
    marker_colors = [colors.get(l, "#ADB5BD") for l in labels]

    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=values,
            hole=0.45,
            marker_colors=marker_colors,
        )
    )
    fig.update_layout(
        title="Sequência de Compra",
        margin=dict(l=20, r=20, t=50, b=20),
        height=320,
        legend=dict(orientation="h", yanchor="bottom", y=-0.3),
    )
    return fig


def timeline_scatter(intersection_df: pd.DataFrame, group_a_col: str, product_b_col: str) -> go.Figure:
    if intersection_df.empty:
        return go.Figure()

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=intersection_df[group_a_col],
            y=intersection_df["Nome"],
            mode="markers",
            name="Grupo A",
            marker=dict(color="#4C6EF5", size=8),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=intersection_df[product_b_col],
            y=intersection_df["Nome"],
            mode="markers",
            name="Produto B",
            marker=dict(color="#F03E3E", size=8, symbol="diamond"),
        )
    )

    # Linhas conectando A e B para cada comprador
    for _, row in intersection_df.iterrows():
        da = row[group_a_col]
        db = row[product_b_col]
        if pd.notna(da) and pd.notna(db):
            fig.add_trace(
                go.Scatter(
                    x=[da, db],
                    y=[row["Nome"], row["Nome"]],
                    mode="lines",
                    line=dict(color="#CED4DA", width=1),
                    showlegend=False,
                )
            )

    fig.update_layout(
        title="Timeline de Compras (compradores em ambos os grupos)",
        xaxis_title="Data",
        yaxis_title="Comprador",
        height=max(400, len(intersection_df) * 28 + 100),
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


def products_bar(df: pd.DataFrame) -> go.Figure:
    if "Nome do Produto" not in df.columns:
        return go.Figure()

    counts = df["Nome do Produto"].value_counts().head(20).reset_index()
    counts.columns = ["Produto", "Qtde"]

    fig = px.bar(
        counts,
        x="Qtde",
        y="Produto",
        orientation="h",
        color="Qtde",
        color_continuous_scale="Blues",
        title="Top 20 Produtos por Nº de Transações",
    )
    fig.update_layout(
        yaxis=dict(autorange="reversed"),
        showlegend=False,
        margin=dict(l=20, r=20, t=50, b=20),
        height=500,
        coloraxis_showscale=False,
        bargap=0.15,
    )
    return fig


# ── Charts de análise cruzada ─────────────────────────────────────────────────

def lead_to_purchase_all_bar(df: pd.DataFrame) -> go.Figure:
    """
    Barras horizontais com a mediana de dias lead→compra por produto.
    Inclui anotações de mínimo e máximo.
    """
    if df.empty:
        return go.Figure()

    fig = go.Figure()

    fig.add_trace(go.Bar(
        name="Mediana (dias)",
        y=df["Nome do Produto"],
        x=df["mediana"],
        orientation="h",
        marker_color="#4C6EF5",
        text=df.apply(lambda r: f"med {r['mediana']}d  |  min {r['minimo']}d  |  max {r['maximo']}d", axis=1),
        textposition="outside",
    ))

    fig.update_layout(
        title="Mediana de dias lead → compra por produto",
        xaxis_title="Dias",
        yaxis=dict(autorange="reversed"),
        margin=dict(l=20, r=20, t=60, b=20),
        height=max(400, len(df) * 38 + 100),
        bargap=0.15,
        showlegend=False,
    )
    return fig


def days_histogram(df: pd.DataFrame, days_col: str = "dias", title: str = "Distribuição de dias") -> go.Figure:
    """Histograma de dias entre lead entry e compra."""
    if df.empty or days_col not in df.columns:
        return go.Figure()

    fig = px.histogram(
        df,
        x=days_col,
        nbins=min(40, max(10, len(df) // 5)),
        color_discrete_sequence=["#4C6EF5"],
        title=title,
        labels={days_col: "Dias"},
    )
    fig.update_layout(
        xaxis_title="Dias entre entrada na base e compra",
        yaxis_title="Nº de compradores",
        margin=dict(l=20, r=20, t=50, b=20),
        height=350,
        bargap=0.05,
    )
    return fig


def tags_distribution_bar(dist_df: pd.DataFrame) -> go.Figure:
    """Gráfico de barras: quantos compradores têm X tags."""
    if dist_df.empty:
        return go.Figure()

    fig = px.bar(
        dist_df,
        x="qtde_tags",
        y="compradores",
        color_discrete_sequence=["#37B24D"],
        title="Distribuição de tags por comprador",
        labels={"qtde_tags": "Nº de tags", "compradores": "Nº de compradores"},
        text="compradores",
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(
        xaxis=dict(tickmode="linear"),
        margin=dict(l=20, r=20, t=50, b=20),
        height=350,
        bargap=0.15,
    )
    return fig


def utm_content_bar(df: pd.DataFrame, top_n: int = 25) -> go.Figure:
    """Top anúncios (utm_content) por emails únicos de leads."""
    if df.empty:
        return go.Figure()

    df_plot = df.head(top_n).copy()

    fig = px.bar(
        df_plot,
        x="emails_unicos",
        y="utm_content",
        orientation="h",
        color="emails_unicos",
        color_continuous_scale="Purples",
        title=f"Top {top_n} anúncios por leads únicos (utm_content)",
        text="emails_unicos",
        labels={"emails_unicos": "Leads únicos", "utm_content": "Anúncio"},
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(
        yaxis=dict(autorange="reversed"),
        margin=dict(l=20, r=20, t=50, b=40),
        height=max(400, top_n * 32 + 100),
        coloraxis_showscale=False,
        bargap=0.15,
    )
    return fig


def first_entry_bar(df: pd.DataFrame, label_col: str, title: str, top_n: int = 20) -> go.Figure:
    """Barras agrupadas: leads totais vs compradores por tag ou formulário."""
    if df.empty:
        return go.Figure()

    df_plot = df.head(top_n).copy()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Total leads",
        y=df_plot[label_col],
        x=df_plot["total_leads"],
        orientation="h",
        marker_color="#74C0FC",
    ))
    fig.add_trace(go.Bar(
        name="Compradores",
        y=df_plot[label_col],
        x=df_plot["compradores"],
        orientation="h",
        marker_color="#37B24D",
        text=df_plot["taxa_conversao_pct"].apply(lambda v: f"{v}%"),
        textposition="outside",
    ))
    fig.update_layout(
        title=title,
        barmode="group",
        yaxis=dict(autorange="reversed"),
        xaxis_title="Quantidade",
        margin=dict(l=20, r=20, t=80, b=20),
        height=max(400, top_n * 42 + 100),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        bargap=0.2,
        bargroupgap=0.05,
    )
    return fig


def utm_funnel_bar(df: pd.DataFrame, utm_col: str, top_n: int = 20) -> go.Figure:
    """Barras agrupadas: leads únicos vs compradores por valor de UTM."""
    if df.empty:
        return go.Figure()

    df_plot = df.head(top_n).copy()
    has_buyers = "compradores" in df_plot.columns

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Leads únicos",
        y=df_plot[utm_col],
        x=df_plot["emails_unicos"],
        orientation="h",
        marker_color="#74C0FC",
    ))

    if has_buyers:
        fig.add_trace(go.Bar(
            name="Compradores",
            y=df_plot[utm_col],
            x=df_plot["compradores"],
            orientation="h",
            marker_color="#F03E3E",
            text=df_plot["taxa_conversao_pct"].apply(lambda v: f"{v}%"),
            textposition="outside",
        ))

    fig.update_layout(
        title=f"Leads e vendas por {utm_col}",
        barmode="group",
        yaxis=dict(autorange="reversed"),
        xaxis_title="Quantidade",
        margin=dict(l=20, r=20, t=80, b=20),
        height=max(400, top_n * 42 + 100),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        bargap=0.2,
        bargroupgap=0.05,
    )
    return fig


# ── Charts de comportamento antes/depois da tag ───────────────────────────────

def behavior_pie(behavior_counts: pd.DataFrame) -> go.Figure:
    """Donut chart com breakdown comportamental antes/depois da tag."""
    if behavior_counts.empty:
        return go.Figure()

    colors = {
        "Nunca comprou": "#CED4DA",
        "Comprou apenas antes da tag": "#F59F00",
        "Comprou apenas depois da tag": "#37B24D",
        "Comprou antes e depois da tag": "#4C6EF5",
    }
    marker_colors = [colors.get(l, "#ADB5BD") for l in behavior_counts["comportamento"]]

    fig = go.Figure(
        go.Pie(
            labels=behavior_counts["comportamento"],
            values=behavior_counts["leads"],
            hole=0.45,
            marker_colors=marker_colors,
            textinfo="label+percent",
        )
    )
    fig.update_layout(
        title="Comportamento de compra em relação à tag",
        margin=dict(l=20, r=20, t=50, b=20),
        height=380,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.25),
    )
    return fig


def products_before_after_bar(
    products_before: pd.DataFrame,
    products_after: pd.DataFrame,
    top_n: int = 15,
) -> go.Figure:
    """
    Barras agrupadas mostrando os top produtos comprados antes vs depois da tag.
    Ordenados pelo total (antes + depois).
    """
    if products_before.empty and products_after.empty:
        return go.Figure()

    pb = products_before[["Nome do Produto", "compradores"]].rename(columns={"compradores": "antes"}) if not products_before.empty else pd.DataFrame(columns=["Nome do Produto", "antes"])
    pa = products_after[["Nome do Produto", "compradores"]].rename(columns={"compradores": "depois"}) if not products_after.empty else pd.DataFrame(columns=["Nome do Produto", "depois"])

    merged = pb.merge(pa, on="Nome do Produto", how="outer").fillna(0)
    merged["antes"] = merged["antes"].astype(int)
    merged["depois"] = merged["depois"].astype(int)
    merged["total"] = merged["antes"] + merged["depois"]
    merged = merged.nlargest(top_n, "total")

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Antes da tag",
        y=merged["Nome do Produto"],
        x=merged["antes"],
        orientation="h",
        marker_color="#F59F00",
    ))
    fig.add_trace(go.Bar(
        name="Depois da tag",
        y=merged["Nome do Produto"],
        x=merged["depois"],
        orientation="h",
        marker_color="#37B24D",
    ))

    fig.update_layout(
        title=f"Top {top_n} produtos: compradores antes vs depois da tag",
        barmode="group",
        yaxis=dict(autorange="reversed"),
        xaxis_title="Nº de compradores únicos",
        margin=dict(l=20, r=20, t=80, b=20),
        height=max(400, top_n * 48 + 100),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        bargap=0.2,
        bargroupgap=0.05,
    )
    return fig
