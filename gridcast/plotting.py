import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

COLORS = {
    'Hídrica': '#1f77b4',  # Blue
    'Eólica': '#2ca02c',   # Green
    'Solar': '#ff7f0e',    # Orange
    'Biomassa': '#d62728', # Red
    'Ondas': '#9467bd',    # Purple
    'Gás Natural - Ciclo Combinado': '#8c564b',  # Brown
    'Gás natural - Cogeração': '#e377c2',        # Pink
    'Gás Natural': '#8c564b',        # Pink
    'Carvão': '#7f7f7f',   # Gray
    'Outra Térmica': '#bcbd22',  # Olive
    'Importação': '#17becf',      # Cyan
    'Exportação': '#ff9896',      # Light red
    'Bombagem': '#98df8a',        # Light green
    'Injeção de Baterias': '#ffbb78',  # Light orange
    'Consumo Baterias': '#f7b6d2'      # Light pink
}


def plot_columns(df: pd.DataFrame, columns: list[str]) -> go.Figure:
    """
    Plot a list of columns in a stacked area chart.
    """
    fig = make_subplots(rows=1, cols=1, subplot_titles=columns)
    for col in columns:
        fig.add_trace(
            go.Scatter(x=df['Data e Hora'], y=df[col], name=col, mode='lines'),
            row=1, col=1
        )
    return fig


def plot_daily_energy_stacked(df: pd.DataFrame, day: str) -> go.Figure:
    """
    Create a daily energy breakdown plot with stacked area charts
    showing how production sources meet consumption.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to plot.
    day : str
        Day date in YYYY-MM-DD format

    Returns
    -------
    plotly.graph_objects.Figure
        The Plotly figure with stacked energy breakdown
    """
    if isinstance(day, str):
        day = pd.to_datetime(day)

    mask = (df['Data e Hora'] >= day) & (df['Data e Hora'] < day + pd.Timedelta(days=1))
    daily_df = df.loc[mask].copy()

    if daily_df.empty:
        raise ValueError(f"No data found for the specified date: {day}")

    # Create subplots for stacked production and individual lines
    fig = make_subplots(
        rows=2,
        cols=1,
        subplot_titles=('Energy Production Stacked', 'Individual Production Sources'),
        vertical_spacing=0.1,
        shared_xaxes=True,
        row_heights=[0.6, 0.4]
    )

    daily_df["Gás Natural"] = (
        daily_df["Gás Natural - Ciclo Combinado"] +
        daily_df["Gás natural - Cogeração"]
    )
    daily_df["Consumo e Armazenamento"] = (
        daily_df["Consumo"] +  daily_df["Bombagem"] + daily_df["Injeção de Baterias"]
    )

    # Define production columns for stacking (positive contributions)
    production_cols = [
        "Outra Térmica", "Ondas", "Carvão", "Biomassa",
        "Gás Natural", "Importação",
        "Eólica", "Solar", "Hídrica"
    ]

    available_cols = [col for col in production_cols if col in daily_df.columns]

    if available_cols:
        # Create stacked area chart
        fig.add_trace(
            go.Scatter(
                x=daily_df['Data e Hora'],
                y=daily_df[available_cols[0]],
                mode='lines',
                name=available_cols[0],
                line={"color": COLORS.get(available_cols[0], '#000000')},
                fill='tonexty',
                showlegend=True,
                hovertemplate='<b>%{y:.1f} MW<br>'
            ),
            row=1, col=1
        )

        for i, col in enumerate(available_cols[1:], 1):
            cumulative = daily_df[available_cols[:i+1]].sum(axis=1)
            hover_text = [f'{daily_df[col].iloc[j]:.1f} MW' for j in range(len(daily_df))]
            fig.add_trace(
                go.Scatter(
                    x=daily_df['Data e Hora'],
                    y=cumulative,
                    mode='lines',
                    name=col,
                    line={"color": COLORS.get(col, '#000000')},
                    fill='tonexty',
                    showlegend=True,
                    hovertemplate='<b>%{text}<br>',
                    text=hover_text
                ),
                row=1, col=1
            )

    # Add consumption totals lines
    fig.add_trace(
        go.Scatter(
        x=daily_df['Data e Hora'],
        y=daily_df["Consumo"],
        mode='lines',
        name="Consumo",
        line={"color": '#d62728', "width": 3, "dash": 'dash'},
        showlegend=True,
        hovertemplate='<b>%{y:.1f} MW<br>'
    ),
    row=1, col=1
    )

    fig.add_trace(
        go.Scatter(
        x=daily_df['Data e Hora'],
        y=daily_df["Consumo e Armazenamento"],
        mode='lines',
        name="Consumo e Armazenamento",
        line={"color": 'black', "width": 3},
        showlegend=True,
        hovertemplate='<b>%{y:.1f} MW<br>'
    ),
    row=1, col=1
    )

    # Plot individual production types as lines (row 2)
    for col in production_cols:
        if col in daily_df.columns:
            fig.add_trace(
                go.Scatter(
                    x=daily_df['Data e Hora'],
                    y=daily_df[col],
                    mode='lines',
                    name=col,
                    line={"color": COLORS.get(col, '#000000')},
                    showlegend=False,
                    hovertemplate='<b>%{y:.1f} MW<br>'
                ),
                row=2, col=1
            )

    # Update layout
    fig.update_layout(
        title=f'Daily Energy Breakdown (Stacked): {day.strftime("%Y-%m-%d")}',
        height=900,
        hovermode='x unified',
        margin={"b": 150},  # Add bottom margin for legend
        legend={
            "orientation": "h",
            "yanchor": "top",
            "y": -0.15,  # Position below the plot
            "xanchor": "center",
            "x": 0.5,  # Center horizontally
            "bgcolor": 'rgba(255,255,255,0.8)',
            "bordercolor": 'rgba(0,0,0,0.2)',
            "borderwidth": 1
        }
    )

    fig.update_xaxes(title_text="Time", row=2, col=1)
    fig.update_yaxes(title_text="Power (MW)", row=1, col=1)
    fig.update_yaxes(title_text="Power (MW)", row=2, col=1)
    return fig
