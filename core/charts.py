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
    )
    return fig
