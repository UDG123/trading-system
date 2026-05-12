"""Generate lightweight HTML dashboard from Railway-exported logs/CSV."""
from __future__ import annotations
import pandas as pd
import plotly.express as px
from pathlib import Path


def build_dashboard(log_csv: str, output_html: str = "dashboard.html") -> str:
    df = pd.read_csv(log_csv)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    figs = []
    if {"timestamp", "trade_count"}.issubset(df.columns):
        figs.append(px.line(df, x="timestamp", y="trade_count", title="Daily Trade Count"))
    if {"timestamp", "confluence_score"}.issubset(df.columns):
        figs.append(px.line(df, x="timestamp", y="confluence_score", title="Confluence Scores"))
    if {"timestamp", "hmm_state"}.issubset(df.columns):
        figs.append(px.histogram(df, x="hmm_state", title="HMM States"))
    if {"timestamp", "pnl"}.issubset(df.columns):
        figs.append(px.line(df, x="timestamp", y="pnl", title="PnL"))

    html = ["<html><body><h1>Trading Dashboard</h1>"]
    for fig in figs:
        html.append(fig.to_html(full_html=False, include_plotlyjs="cdn"))
    html.append("</body></html>")
    out = Path(output_html)
    out.write_text("\n".join(html), encoding="utf-8")
    return str(out)
