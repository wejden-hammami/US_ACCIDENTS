# -*- coding: utf-8 -*-
"""Dashboard US Accidents (thème clair beige).

3 onglets :
  1. Tableau de bord : statistiques descriptives calculées par Hive (CSV exportés dans data/)
                       + grande carte interactive (États / Points / Hexagones, lecture dans le temps)
  2. Requêtes Hive   : on écrit une requête SQL, Hive l'exécute sur HDFS et on voit le résultat
  3. Flux en direct  : Kafka (temps réel), Flume et Sqoop (données arrivées dans HDFS)

Lancer en local (données d'EXEMPLE) : DATA_DIR=sample_data python app.py
"""
import csv
import io
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import deque

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State, no_update

DATA_DIR = os.environ.get("DATA_DIR", "data")
IS_SAMPLE = os.path.basename(os.path.normpath(DATA_DIR)) == "sample_data"

HIVE_HOST = os.environ.get("HIVE_HOST", "hiveserver2")
HIVE_PORT = int(os.environ.get("HIVE_PORT", "10000"))
HIVE_DB = os.environ.get("HIVE_DB", "accidents")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "us_accidents")
WEBHDFS_URL = os.environ.get("WEBHDFS_URL", "http://namenode:9870")
HDFS_FLUME = "/data/us_accidents/raw/flume"
HDFS_SQOOP = "/data/us_accidents/raw/sqoop"
COLUMNS = ["id", "severity", "start_time", "end_time", "start_lat", "start_lng", "street", "city", "state",
           "weather_condition", "temperature_f", "visibility_mi", "junction", "traffic_signal", "sunrise_sunset"]

# ── Palette claire (beige) : bleu = principal, orange = mise en évidence, vert = 3e série
BG, PANEL, PANEL2 = "#f4eee2", "#fffdf8", "#f7f1e4"
INK, MUTED, GRID, LINE = "#2d2a26", "#6f685c", "#ebe3d2", "#e3d9c4"
BLUE, ORANGE, AQUA, RED = "#2a78d6", "#e2622a", "#16996d", "#c0262e"
NAVY = "#0b3a75"
SEV_COLORS = {"1": "#8fb7ea", "2": "#2a78d6", "3": "#e2622a", "4": "#c0262e"}
HEX_SCALE = [[0, "#c9dcf6"], [0.5, "#2a78d6"], [1, "#0b2d5e"]]
STATE_SCALE = [[0, "#dce8f8"], [0.5, "#6aa6ec"], [1, "#0b3a75"]]
FONT = "Inter, Segoe UI, system-ui, -apple-system, Arial, sans-serif"


# ══════════════════════════════════════════════════════════════════════════════
#  Données (CSV produits par Hive)
# ══════════════════════════════════════════════════════════════════════════════
def load(name):
    path = os.path.join(DATA_DIR, name + ".csv")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path)
        df.columns = [c.split(".")[-1] for c in df.columns]   # Hive préfixe parfois ("_u1.source")
        return df if len(df) else None
    except Exception:
        return None


def fmt(n):
    return f"{int(n):,}".replace(",", " ")


# ══════════════════════════════════════════════════════════════════════════════
#  Mise en forme des graphiques
# ══════════════════════════════════════════════════════════════════════════════
def style(fig, height=300, legend=False):
    fig.update_layout(
        height=height, margin=dict(l=6, r=6, t=6, b=6),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONT, color=MUTED, size=12), showlegend=legend, bargap=0.28,
        hoverlabel=dict(bgcolor="white", bordercolor=LINE, font=dict(family=FONT, color=INK, size=13)),
        legend=dict(orientation="h", y=-0.18, x=0, title_text="", font=dict(color=MUTED)),
    )
    fig.update_xaxes(showgrid=False, linecolor=LINE, zeroline=False, ticks="", title_font=dict(size=12))
    fig.update_yaxes(gridcolor=GRID, zeroline=False, linecolor="rgba(0,0,0,0)", ticks="", title_font=dict(size=12))
    return fig


def empty(name, height=300):
    fig = go.Figure()
    fig.add_annotation(text=f"Pas de données ({name}.csv absent)", showarrow=False, font=dict(size=13, color=MUTED))
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return style(fig, height)


def card(title, subtitle, fig, cls="c3"):
    return html.Div([
        html.Div([html.Div(title, className="ct"), html.Div(subtitle, className="cs")], className="chead"),
        dcc.Graph(figure=fig, config={"displayModeBar": False}),
    ], className=f"card {cls}")


# ══════════════════════════════════════════════════════════════════════════════
#  ONGLET 1 : tableau de bord
# ══════════════════════════════════════════════════════════════════════════════
def kpis():
    d = load("q01_kpis")
    if d is None:
        return html.Div()
    r = d.iloc[0]
    items = [
        ("Accidents analysés", fmt(r["accidents"]), "lignes lues par Hive", BLUE),
        ("Identifiants uniques", fmt(r["ids_uniques"]), "aucun doublon" if r["accidents"] == r["ids_uniques"] else "doublons détectés", AQUA),
        ("États couverts", str(int(r["etats"])), "sur 50 États", "#a89b7f"),
        ("Gravité moyenne", f"{float(r['gravite_moyenne']):.2f}", "échelle de 1 à 4", ORANGE),
        ("Période", f"{str(r['premiere_date'])[:4]} – {str(r['derniere_date'])[:4]}",
         f"{str(r['premiere_date'])[:7]} → {str(r['derniere_date'])[:7]}", BLUE),
    ]
    return html.Div([
        html.Div([html.Div(k, className="kl"), html.Div(v, className="kv"), html.Div(s, className="ks")],
                 className="kpi", style={"borderLeftColor": c}) for k, v, s, c in items
    ], className="kpis")


def insights():
    out = []
    k, e = load("q01_kpis"), load("q02_par_etat")
    if k is not None and e is not None:
        top = e.sort_values("accidents", ascending=False).iloc[0]
        out.append(("État le plus touché", f"{top['state']} · {100 * top['accidents'] / k.iloc[0]['accidents']:.0f} % des accidents"))
    h = load("q04_par_heure")
    if h is not None:
        p = h.sort_values("accidents", ascending=False).iloc[0]
        out.append(("Heure de pointe", f"{int(p['heure'])} h · {fmt(p['accidents'])} accidents"))
    g = load("q03_gravite")
    if g is not None:
        out.append(("Gravité dominante", f"Niveau {int(g.sort_values('accidents', ascending=False).iloc[0]['severity'])}"))
    m = load("q06_meteo")
    if m is not None:
        out.append(("Météo la plus fréquente", str(m.sort_values("accidents", ascending=False).iloc[0]["weather_condition"])))
    return html.Div([html.Div([html.Div(a, className="il"), html.Div(b, className="iv")], className="insight") for a, b in out],
                    className="insights")


def months_list():
    d = load("q10_carte_echantillon")
    if d is None or "mois" not in d.columns:
        return []
    return sorted(d["mois"].dropna().astype(str).unique())


def build_map(layer, idx):
    geo = dict(scope="usa", projection_type="albers usa", bgcolor="rgba(0,0,0,0)", landcolor="#efe6d3",
               subunitcolor="#d6c9ac", subunitwidth=0.7, countrycolor="#d6c9ac", coastlinecolor="#d6c9ac",
               showlakes=False, showframe=False, showcoastlines=False)
    fig = go.Figure()
    note = ""
    cbar = dict(title="", thickness=10, len=0.6, outlinewidth=0, tickfont=dict(color=MUTED), x=0.97)

    if layer == "etats":
        d = load("q02_par_etat")
        if d is not None:
            fig.add_trace(go.Choropleth(
                locations=d["state"], z=d["accidents"], locationmode="USA-states", colorscale=STATE_SCALE,
                marker_line_color=BG, marker_line_width=0.8, customdata=d[["gravite_moyenne"]].values, colorbar=cbar,
                hovertemplate="<b>%{location}</b><br>%{z:,} accidents<br>gravité moyenne %{customdata[0]}<extra></extra>"))
        note = "Couche « États » : cumul sur toute la période"
    else:
        d = load("q10_carte_echantillon")
        months = months_list()
        label = ""
        if d is not None and "mois" in d.columns and months:
            idx = max(0, min(int(idx or 0), len(months) - 1))
            d = d[d["mois"].astype(str) <= months[idx]]
            label = months[idx]
        if d is not None and len(d):
            d = d.copy()
            d["severity"] = d["severity"].astype(int).astype(str)
            if layer == "points":
                for sev in ["1", "2", "3", "4"]:
                    s = d[d["severity"] == sev]
                    if len(s):
                        fig.add_trace(go.Scattergeo(
                            lat=s["start_lat"], lon=s["start_lng"], mode="markers", name=f"Gravité {sev}",
                            marker=dict(size=5, color=SEV_COLORS[sev], opacity=0.8, line=dict(width=0)),
                            hovertemplate=f"Gravité {sev}<extra></extra>"))
            else:
                step = 0.6
                g = (d.assign(la=(d["start_lat"] / step).round() * step, lo=(d["start_lng"] / step).round() * step)
                      .groupby(["la", "lo"]).size().reset_index(name="n"))
                size = 7 + 20 * np.sqrt(g["n"] / g["n"].max())
                fig.add_trace(go.Scattergeo(
                    lat=g["la"], lon=g["lo"], mode="markers", showlegend=False, customdata=g[["n"]].values,
                    marker=dict(symbol="hexagon", size=size, color=g["n"], colorscale=HEX_SCALE, opacity=0.9,
                                line=dict(width=0.5, color="white"), colorbar=cbar),
                    hovertemplate="%{customdata[0]} accidents dans cette zone<extra></extra>"))
        note = f"Cumul des accidents jusqu'à {label}" if label else "Échantillon d'accidents"
        if d is None:
            note = "Pas de données (q10_carte_echantillon.csv absent)"

    fig.update_geos(**geo)
    fig.update_layout(
        height=600, margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONT, color=MUTED), showlegend=(layer == "points"),
        legend=dict(x=0.01, y=0.02, bgcolor="rgba(255,253,248,.85)", font=dict(color=INK), bordercolor=LINE, borderwidth=1),
        hoverlabel=dict(bgcolor="white", bordercolor=LINE, font=dict(family=FONT, color=INK, size=13)),
        annotations=[dict(text=note, x=0.01, y=0.99, xref="paper", yref="paper", showarrow=False,
                          font=dict(color=MUTED, size=12), xanchor="left", yanchor="top")],
    )
    return fig


def fig_gravite():
    d = load("q03_gravite")
    if d is None:
        return empty("q03_gravite", 230)
    d = d.copy()
    d["severity"] = d["severity"].astype(int).astype(str)
    d["part"] = (100 * d["accidents"] / d["accidents"].sum()).round(1)
    f = go.Figure(go.Bar(x=d["severity"], y=d["accidents"], marker_color=[SEV_COLORS.get(s, BLUE) for s in d["severity"]],
                         text=d["part"].astype(str) + " %", textposition="outside", cliponaxis=False, customdata=d[["part"]].values,
                         hovertemplate="Gravité %{x}<br>%{y:,} accidents (%{customdata[0]} %)<extra></extra>"))
    f.update_xaxes(title="Gravité (1 = faible, 4 = fort)", categoryorder="category ascending")
    f.update_yaxes(title="", showticklabels=False, showgrid=False)
    return style(f, 230)


def fig_heure():
    d = load("q04_par_heure")
    if d is None:
        return empty("q04_par_heure", 230)
    pic = d["accidents"].idxmax()
    f = go.Figure(go.Bar(x=d["heure"], y=d["accidents"], marker_color=[ORANGE if i == pic else BLUE for i in d.index],
                         hovertemplate="%{x} h<br>%{y:,} accidents<extra></extra>"))
    f.update_xaxes(title="Heure de la journée", dtick=3)
    f.update_yaxes(title="", showticklabels=False)
    return style(f, 230)


def fig_top_etats():
    d = load("q02_par_etat")
    if d is None:
        return empty("q02_par_etat", 230)
    top = d.sort_values("accidents", ascending=False).head(6).sort_values("accidents")
    f = go.Figure(go.Bar(x=top["accidents"], y=top["state"], orientation="h", marker_color=BLUE, customdata=top[["gravite_moyenne"]].values,
                         hovertemplate="<b>%{y}</b><br>%{x:,} accidents<br>gravité moyenne %{customdata[0]}<extra></extra>"))
    f.update_yaxes(title="", gridcolor="rgba(0,0,0,0)")
    f.update_xaxes(title="", showgrid=True, gridcolor=GRID)
    return style(f, 230)


def fig_mois():
    d = load("q05_par_mois")
    if d is None:
        return empty("q05_par_mois", 440)
    f = go.Figure(go.Scatter(x=d["mois"], y=d["accidents"], mode="lines+markers", fill="tozeroy",
                             line=dict(color=BLUE, width=2.5), marker=dict(size=7, color=BLUE, line=dict(width=2, color=PANEL)),
                             fillcolor="rgba(42,120,214,0.12)", hovertemplate="%{x}<br>%{y:,} accidents<extra></extra>"))
    f.update_xaxes(title="", type="category")
    f.update_yaxes(title="")
    return style(f, 440)


def fig_barh(name, label_col, height=340):
    d = load(name)
    if d is None:
        return empty(name, height)
    d = d.sort_values("accidents").copy()
    if name == "q07_villes":
        d["label"] = d["city"] + " (" + d["state"] + ")"
        label_col = "label"
    extra, cd = "", None
    if "gravite_moyenne" in d.columns:
        cd = d[["gravite_moyenne"]].values
        extra = "<br>gravité moyenne %{customdata[0]}"
    f = go.Figure(go.Bar(x=d["accidents"], y=d[label_col], orientation="h", marker_color=BLUE, customdata=cd,
                         hovertemplate="<b>%{y}</b><br>%{x:,} accidents" + extra + "<extra></extra>"))
    f.update_yaxes(title="", gridcolor="rgba(0,0,0,0)")
    f.update_xaxes(title="", showgrid=True, gridcolor=GRID)
    return style(f, height)


def fig_classe(name):
    d = load(name)
    if d is None:
        return empty(name, 340)
    if "ordre" in d.columns:
        d = d.sort_values("ordre")
    pic = d["accidents"].idxmax()
    f = go.Figure(go.Bar(x=d["classe"], y=d["accidents"], marker_color=[ORANGE if i == pic else BLUE for i in d.index],
                         customdata=d[["gravite_moyenne"]].values,
                         hovertemplate="%{x}<br>%{y:,} accidents<br>gravité moyenne %{customdata[0]}<extra></extra>"))
    f.update_xaxes(title="", categoryorder="array", categoryarray=list(d["classe"]))
    f.update_yaxes(title="")
    return style(f, 340)


def sources_block():
    d = load("q11_sources")
    if d is None:
        return empty("q11_sources", 260), html.Div()
    cols = {"sqoop": BLUE, "kafka": ORANGE, "flume": AQUA}
    f = go.Figure()
    f.add_bar(name="Lignes lues", x=d["source"], y=d["lignes"], marker_color="#d9cdb4",
              hovertemplate="%{x}<br>%{y:,} lignes<extra></extra>")
    f.add_bar(name="Identifiants uniques", x=d["source"], y=d["ids_uniques"], marker_color=[cols.get(s, BLUE) for s in d["source"]],
              hovertemplate="%{x}<br>%{y:,} ids uniques<extra></extra>")
    f.update_layout(barmode="group")
    f.update_xaxes(title="")
    f.update_yaxes(title="")
    rows = []
    for _, r in d.iterrows():
        ok = int(r["lignes"]) == int(r["ids_uniques"])
        rows.append(html.Tr([html.Td(str(r["source"]).capitalize(), className="b"), html.Td(fmt(r["lignes"])), html.Td(fmt(r["ids_uniques"])),
                             html.Td(html.Span("Conforme" if ok else "À vérifier", className="pill ok" if ok else "pill warn"))]))
    table = html.Table([html.Thead(html.Tr([html.Th("Source"), html.Th("Lignes"), html.Th("Ids uniques"), html.Th("Contrôle")])),
                        html.Tbody(rows)], className="tbl")
    return style(f, 260, legend=True), table


def tab_dashboard():
    months = months_list()
    n = len(months)
    step = max(1, n // 6) if n else 1
    marks = {i: months[i] for i in range(0, n, step)} if n else {}
    src_fig, src_table = sources_block()
    map_card = html.Div([
        html.Div([
            html.Div([html.Div("Carte interactive des accidents", className="ct"),
                      html.Div("Changez de couche et faites défiler le temps", className="cs")]),
            dcc.RadioItems(id="layer", value="etats", className="pills", inline=True, options=[
                {"label": "États", "value": "etats"}, {"label": "Points", "value": "points"}, {"label": "Hexagones", "value": "hex"}]),
        ], className="maphead"),
        dcc.Graph(id="map", figure=build_map("etats", max(n - 1, 0)), config={"displayModeBar": False, "scrollZoom": False}),
        html.Div([
            html.Button("▶ Lecture", id="play", className="btn", n_clicks=0),
            html.Div(dcc.Slider(id="mois", min=0, max=max(n - 1, 0), step=1, value=max(n - 1, 0), marks=marks, included=True), className="sl"),
            html.Div(months[-1] if n else "", id="tlabel", className="tlabel"),
        ], className="timebar", style={} if n else {"display": "none"}),
        dcc.Interval(id="tick", interval=900, disabled=True),
    ], className="card mapcard")

    return html.Div([
        html.Div([
            html.Div([kpis(), insights()], className="left"),
            map_card,
            html.Div([
                card("Répartition selon la gravité", "Part de chaque niveau", fig_gravite(), ""),
                card("Accidents par heure", "En orange : heure de pointe", fig_heure(), ""),
                card("Top 6 des États", "Nombre d'accidents", fig_top_etats(), ""),
            ], className="right"),
        ], className="hero"),

        html.Div("Évolution et conditions", className="sec"),
        html.Div("Dans le temps, selon la météo, la visibilité et la température", className="sub"),
        html.Div([
            card("Évolution mensuelle", "Nombre d'accidents par mois", fig_mois(), "c8"),
            html.Div([
                html.Div([html.Div("Contrôle des sources", className="ct"),
                          html.Div("Lignes lues et identifiants uniques", className="cs")], className="chead"),
                dcc.Graph(figure=src_fig, config={"displayModeBar": False}), src_table,
                html.Div("Les statistiques s'appuient sur la source de référence (Kafka). « À vérifier » signale un format à corriger.", className="note"),
            ], className="card c4"),
            card("Conditions météo", "Top 15 – accidents", fig_barh("q06_meteo", "weather_condition"), "c3"),
            card("Villes les plus touchées", "Top 15 – accidents", fig_barh("q07_villes", "city"), "c3"),
            card("Visibilité", "Orange = classe la plus fréquente", fig_classe("q08_visibilite"), "c3"),
            card("Température", "Plage en °F · orange = classe la plus fréquente", fig_classe("q09_temperature"), "c3"),
        ], className="grid"),
    ])


# ══════════════════════════════════════════════════════════════════════════════
#  ONGLET 2 : requêtes Hive en direct
# ══════════════════════════════════════════════════════════════════════════════
PRESETS = {
    "Accidents par État (top 10)": "SELECT state, COUNT(*) AS accidents\nFROM accidents_ref\nGROUP BY state\nORDER BY accidents DESC\nLIMIT 10",
    "Gravité moyenne par heure": "SELECT HOUR(start_time) AS heure, ROUND(AVG(severity), 2) AS gravite_moyenne\nFROM accidents_ref\nGROUP BY HOUR(start_time)\nORDER BY heure",
    "Météo la plus fréquente": "SELECT weather_condition, COUNT(*) AS accidents\nFROM accidents_ref\nWHERE weather_condition IS NOT NULL\nGROUP BY weather_condition\nORDER BY accidents DESC\nLIMIT 10",
    "Comparer les 3 sources": "SELECT 'kafka' AS source, COUNT(*) AS lignes FROM accidents_kafka_typed\nUNION ALL SELECT 'flume', COUNT(*) FROM accidents_flume_typed\nUNION ALL SELECT 'sqoop', COUNT(*) FROM accidents_sqoop_typed",
    "Aperçu des données Kafka": "SELECT id, severity, start_time, city, state, weather_condition\nFROM accidents_kafka_typed\nLIMIT 10",
    "Aperçu des données Flume": "SELECT id, severity, start_time, city, state, weather_condition\nFROM accidents_flume_typed\nLIMIT 10",
    "Aperçu des données Sqoop": "SELECT id, severity, start_time, city, state, weather_condition\nFROM accidents_sqoop_typed\nLIMIT 10",
    "Tables disponibles": "SHOW TABLES",
}
ALLOWED_START = ("select", "show", "describe", "desc", "with", "explain")


def run_hive(sql):
    """Exécute une requête de LECTURE sur HiveServer2. Renvoie (colonnes, lignes, secondes)."""
    from pyhive import hive
    t0 = time.perf_counter()
    conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username="hadoop", database=HIVE_DB, auth="NONE")
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [c[0].split(".")[-1] for c in (cur.description or [])]
        rows = cur.fetchmany(200)
    finally:
        conn.close()
    return cols, rows, time.perf_counter() - t0


def html_table(cols, rows, cls="tbl"):
    head = html.Thead(html.Tr([html.Th(c) for c in cols]))
    body = html.Tbody([html.Tr([html.Td("" if v is None else str(v)) for v in r]) for r in rows])
    return html.Table([head, body], className=cls)


def tab_hive():
    return html.Div([
        html.Div([
            html.Div([html.Div("Éditeur de requêtes Hive", className="ct"),
                      html.Div("Écrivez une requête de lecture (SELECT, SHOW, DESCRIBE). Elle est exécutée par Hive sur les données de HDFS.", className="cs")],
                     className="chead"),
            dcc.Dropdown(id="preset", options=[{"label": k, "value": k} for k in PRESETS], placeholder="Choisir une requête d'exemple…",
                         clearable=True, className="dd"),
            dcc.Textarea(id="sql", value=PRESETS["Accidents par État (top 10)"], spellCheck=False, className="sqlbox"),
            html.Div([html.Button("▶ Exécuter dans Hive", id="run", className="btn", n_clicks=0),
                      html.Span("Base utilisée : accidents · chaque requête prend en général 30 à 90 secondes (calcul MapReduce).", className="hint")],
                     className="runbar"),
        ], className="card c12"),
        html.Div([
            html.Div([html.Div("Résultat", className="ct"), html.Div("Les 200 premières lignes sont affichées", className="cs")], className="chead"),
            dcc.Loading(html.Div(id="hive-out", children=html.Div("Aucune requête exécutée pour le moment.", className="empty")),
                        type="circle", color=BLUE),
        ], className="card c12"),
    ], className="grid")


# ══════════════════════════════════════════════════════════════════════════════
#  ONGLET 3 : flux en direct (Kafka, Flume, Sqoop)
# ══════════════════════════════════════════════════════════════════════════════
class KafkaFeed:
    """Lit le topic Kafka en continu (thread d'arrière-plan, groupe de consommation indépendant)."""

    def __init__(self):
        self.msgs = deque(maxlen=60)
        self.times = deque(maxlen=50000)
        self.total = 0
        self.status = "Connexion à Kafka…"
        self.ok = False
        self._started = False
        self._lock = threading.Lock()

    def start(self):
        with self._lock:
            if self._started:
                return
            self._started = True
        threading.Thread(target=self._run, daemon=True).start()

    def _on_error(self, err):
        try:
            name = err.name()
        except Exception:
            name = str(err)
        if "ALL_BROKERS_DOWN" in name or "TRANSPORT" in name:
            self.ok, self.status = False, f"Kafka injoignable ({KAFKA_BOOTSTRAP}) – démarré ? docker ps"

    def _run(self):
        while True:
            try:
                from confluent_kafka import Consumer
                c = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": f"dashboard-live-{uuid.uuid4().hex[:8]}",
                              "auto.offset.reset": "latest", "enable.auto.commit": False,
                              "session.timeout.ms": 10000, "socket.timeout.ms": 5000, "error_cb": self._on_error})
                c.list_topics(timeout=6)          # lève une exception si Kafka est injoignable
                c.subscribe([KAFKA_TOPIC])
                self.ok, self.status = True, f"Connecté à Kafka ({KAFKA_BOOTSTRAP}) · topic « {KAFKA_TOPIC} »"
                while True:
                    m = c.poll(1.0)
                    if m is None:
                        continue
                    if m.error():
                        code = str(m.error())
                        if "UNKNOWN_TOPIC" in code or "PARTITION_EOF" in code:
                            continue
                        self.ok, self.status = False, f"Kafka : {code[:120]}"
                        continue
                    if not self.ok:
                        self.ok, self.status = True, f"Connecté à Kafka ({KAFKA_BOOTSTRAP}) · topic « {KAFKA_TOPIC} »"
                    try:
                        rec = json.loads(m.value().decode("utf-8"))
                    except Exception:
                        continue
                    self.msgs.appendleft(rec)
                    self.times.append(time.time())
                    self.total += 1
            except Exception as e:   # Kafka arrêté, bibliothèque absente, etc. : on réessaie
                self.ok, self.status = False, f"Kafka injoignable ({KAFKA_BOOTSTRAP}) – démarré ? docker ps"
                time.sleep(5)


KAFKA = KafkaFeed()


def webhdfs(path, op, **params):
    q = urllib.parse.urlencode({"op": op, "user.name": "hadoop", **params})
    url = f"{WEBHDFS_URL}/webhdfs/v1{path}?{q}"
    with urllib.request.urlopen(url, timeout=6) as r:
        return r.read()


def hdfs_list(path):
    data = json.loads(webhdfs(path, "LISTSTATUS"))
    return [f for f in data["FileStatuses"]["FileStatus"] if f.get("type") == "FILE"]


def hdfs_tail(path, nbytes=12000, length=None):
    size = length if length is not None else 0
    offset = max(0, size - nbytes)
    return webhdfs(path, "OPEN", offset=offset, length=nbytes).decode("utf-8", errors="ignore"), offset


def parse_flume(line):
    m = re.match(r"^\[(.*?)\]\s+ACCIDENT\s+(.*)$", line.strip())
    if not m:
        return None
    rec = {"start_time": m.group(1)}
    for part in m.group(2).split("|"):
        if "=" in part:
            k, v = part.split("=", 1)
            rec[k] = v
    return rec


def mini_table(rows, cols):
    if not rows:
        return html.Div("Aucune donnée pour le moment.", className="empty")
    head = html.Thead(html.Tr([html.Th(c.replace("_", " ")) for c in cols]))
    body = html.Tbody([html.Tr([html.Td("" if r.get(c) in (None, "") else str(r.get(c))) for c in cols]) for r in rows])
    return html.Table([head, body], className="tbl mini")


def status_pill(ok, text):
    return html.Span(text, className="pill ok" if ok else "pill warn")


def tab_live():
    KAFKA.start()
    return html.Div([
        html.Div([
            html.Div([html.Div([html.Div("Streaming Kafka", className="ct"),
                                html.Div("Accidents reçus en temps réel par le topic Kafka (mise à jour chaque seconde)", className="cs")]),
                      html.Div(id="k-status")], className="maphead2"),
            html.Div([
                html.Div([html.Div("Messages reçus", className="kl"), html.Div("0", id="k-total", className="kv"),
                          html.Div("depuis l'ouverture de cette page", className="ks")], className="kpi", style={"borderLeftColor": ORANGE}),
                html.Div([html.Div("Débit", className="kl"), html.Div("0", id="k-rate", className="kv"),
                          html.Div("messages par seconde (10 dernières s)", className="ks")], className="kpi", style={"borderLeftColor": BLUE}),
            ], className="kpis2"),
            dcc.Graph(id="k-graph", config={"displayModeBar": False}),
            html.Div(id="k-table"),
            html.Div(["Pour voir le flux défiler, lancez le producteur dans un autre terminal : ",
                      html.Code("docker compose --profile kafka-app run --rm -e LIMIT=3000 -e DELAY=0.05 producer")], className="note"),
        ], className="card c12"),
        html.Div([
            html.Div([html.Div([html.Div("Flume", className="ct"),
                                html.Div("Derniers accidents écrits par Flume dans HDFS (/raw/flume)", className="cs")]),
                      html.Div(id="f-status")], className="maphead2"),
            html.Div(id="f-table"),
        ], className="card c6"),
        html.Div([
            html.Div([html.Div([html.Div("Sqoop", className="ct"),
                                html.Div("Aperçu de l'import MySQL → HDFS (/raw/sqoop)", className="cs")]),
                      html.Div(id="s-status")], className="maphead2"),
            html.Div(id="s-table"),
        ], className="card c6"),
        dcc.Interval(id="k-tick", interval=1000),
        dcc.Interval(id="fs-tick", interval=6000),
    ], className="grid")


# ══════════════════════════════════════════════════════════════════════════════
#  Style de la page
# ══════════════════════════════════════════════════════════════════════════════
CSS = f"""
*{{box-sizing:border-box}}
body{{margin:0;font-family:{FONT};background:{BG};color:{INK}}}
.page{{width:100%;padding:18px 24px 30px}}
.top{{display:flex;align-items:center;justify-content:space-between;gap:20px;margin-bottom:10px;flex-wrap:wrap}}
.top h1{{margin:0;font-size:26px;font-weight:700;letter-spacing:-.01em;color:{NAVY}}}
.top p{{margin:4px 0 0;color:{MUTED};font-size:14px}}
.chips{{display:flex;gap:8px;flex-wrap:wrap}}
.chip{{background:{PANEL};border:1px solid {LINE};color:{MUTED};padding:6px 12px;border-radius:20px;font-size:12.5px}}
.chip b{{color:{INK}}}
.hero{{display:grid;grid-template-columns:300px minmax(0,1fr) 340px;gap:16px;align-items:start}}
.card{{background:{PANEL};border:1px solid {LINE};border-radius:14px;padding:16px 18px;box-shadow:0 1px 2px rgba(80,60,20,.05),0 8px 22px rgba(80,60,20,.07)}}
.chead{{margin-bottom:6px}}
.ct{{font-size:15px;font-weight:700;color:{INK}}}
.cs{{font-size:12.5px;color:{MUTED};margin-top:2px}}
.left{{display:flex;flex-direction:column;gap:14px}}
.kpis{{display:grid;gap:10px}}
.kpi{{background:{PANEL};border:1px solid {LINE};border-left:4px solid {BLUE};border-radius:12px;padding:12px 16px}}
.kl{{font-size:11.5px;color:{MUTED};font-weight:600;text-transform:uppercase;letter-spacing:.05em}}
.kv{{font-size:28px;font-weight:700;margin:4px 0 0;letter-spacing:-.01em}}
.ks{{font-size:12px;color:{MUTED}}}
.insights{{display:grid;gap:8px}}
.insight{{background:{PANEL2};border:1px solid {LINE};border-radius:12px;padding:10px 14px}}
.il{{font-size:11.5px;color:{MUTED};font-weight:600;text-transform:uppercase;letter-spacing:.05em}}
.iv{{font-size:15px;font-weight:700;margin-top:2px}}
.mapcard{{padding:0;overflow:hidden}}
.maphead{{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:14px 18px 0;flex-wrap:wrap}}
.maphead2{{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-bottom:10px;flex-wrap:wrap}}
.pills{{display:flex;gap:6px}}
.pills label{{display:inline-block;cursor:pointer;padding:7px 14px;border-radius:20px;font-size:13px;font-weight:600;color:{MUTED};
  background:{PANEL2};border:1px solid {LINE}}}
.pills input{{display:none}}
.pills label:has(input:checked){{background:{BLUE};color:#fff;border-color:{BLUE}}}
.timebar{{display:flex;align-items:center;gap:14px;padding:6px 20px 16px}}
.btn{{background:{BLUE};color:#fff;border:0;border-radius:20px;padding:9px 18px;font-weight:700;font-size:13px;cursor:pointer;white-space:nowrap}}
.btn:hover{{background:{NAVY}}}
.timebar .sl{{flex:1;min-width:0;padding-top:6px}}
.tlabel{{color:{INK};font-weight:700;font-size:14px;min-width:84px;text-align:right}}
.right{{display:flex;flex-direction:column;gap:14px}}
.grid{{display:grid;grid-template-columns:repeat(12,1fr);gap:16px;margin-top:16px}}
.c3{{grid-column:span 3}}.c4{{grid-column:span 4}}.c6{{grid-column:span 6}}.c8{{grid-column:span 8}}.c12{{grid-column:span 12}}
.sec{{margin:26px 0 0;font-size:18px;font-weight:700;display:flex;align-items:center;gap:10px;color:{NAVY}}}
.sec:before{{content:"";width:5px;height:18px;border-radius:3px;background:{BLUE}}}
.sub{{margin:3px 0 0 15px;color:{MUTED};font-size:13.5px}}
.tbl{{width:100%;border-collapse:collapse;margin-top:6px;font-size:14px}}
.tbl th{{text-align:left;color:{MUTED};font-size:11.5px;text-transform:uppercase;letter-spacing:.05em;padding:8px 8px;border-bottom:1px solid {LINE}}}
.tbl td{{padding:9px 8px;border-bottom:1px solid {GRID}}}
.tbl .b{{font-weight:700}}
.tbl.mini td,.tbl.mini th{{font-size:12.5px}}
.pill{{padding:3px 10px;border-radius:12px;font-size:12px;font-weight:700;white-space:nowrap}}
.pill.ok{{background:#dff3e9;color:#0f7a52}}.pill.warn{{background:#fde7d9;color:#b5470f}}
.note{{font-size:12.5px;color:{MUTED};margin-top:10px;line-height:1.5}}
.note code{{background:{PANEL2};border:1px solid {LINE};border-radius:6px;padding:2px 6px;color:{INK};font-size:12px}}
.empty{{color:{MUTED};font-size:13.5px;padding:16px 4px}}
.err{{background:#fde7d9;border:1px solid {ORANGE};border-radius:10px;padding:12px 14px;color:#8a3a0e;font-size:13.5px;white-space:pre-wrap}}
.meta{{color:{MUTED};font-size:13px;margin-bottom:8px}}
.meta b{{color:{INK}}}
.sqlbox{{width:100%;min-height:150px;margin-top:10px;padding:12px 14px;border:1px solid {LINE};border-radius:10px;background:{PANEL2};
  font-family:Consolas,Menlo,monospace;font-size:14px;color:{INK};resize:vertical}}
.runbar{{display:flex;align-items:center;gap:14px;margin-top:12px;flex-wrap:wrap}}
.hint{{color:{MUTED};font-size:12.5px}}
.kpis2{{display:grid;grid-template-columns:repeat(2,minmax(0,260px));gap:12px;margin-bottom:6px}}
.banner{{background:#fde7d9;border:1px solid {ORANGE};border-radius:10px;padding:10px 14px;margin:0 0 14px;font-weight:700;color:#8a3a0e}}
.foot{{text-align:center;color:{MUTED};font-size:12.5px;padding:22px 0 0}}
.Select-control{{border-color:{LINE}!important;background:{PANEL2}!important}}
.rc-slider-rail{{background:{LINE}!important}}
.rc-slider-track{{background:{BLUE}!important}}
.rc-slider-handle{{border-color:{BLUE}!important;background:{BLUE}!important}}
.rc-slider-mark-text{{color:{MUTED}!important;font-size:11px}}
.rc-slider-mark-text-active{{color:{INK}!important}}
.rc-slider-dot{{border-color:{LINE}!important;background:{PANEL}!important}}
@media (max-width:1400px){{.hero{{grid-template-columns:260px minmax(0,1fr)}}.right{{grid-column:1 / -1;flex-direction:row}}.right .card{{flex:1}}}}
@media (max-width:1000px){{.hero{{grid-template-columns:1fr}}.c3,.c4,.c6,.c8{{grid-column:span 12}}.right{{flex-direction:column}}}}
"""

app = Dash(__name__, title="US Accidents · Tableau de bord", suppress_callback_exceptions=True)
app.index_string = app.index_string.replace("</head>", f"<style>{CSS}</style></head>")

TAB = {"padding": "10px 20px", "border": "0", "background": "transparent", "color": MUTED, "fontWeight": "600",
       "borderBottom": "3px solid transparent", "fontFamily": FONT, "flex": "0 0 auto"}
TAB_ON = {**TAB, "color": NAVY, "borderBottom": f"3px solid {BLUE}", "background": "transparent"}


def layout():
    k = load("q01_kpis")
    periode = f"{str(k.iloc[0]['premiere_date'])[:7]} → {str(k.iloc[0]['derniere_date'])[:7]}" if k is not None else "—"
    banner = [html.Div("DONNÉES D'EXEMPLE (synthétiques) – ce ne sont pas les résultats Hive", className="banner")] if IS_SAMPLE else []
    return html.Div(banner + [
        html.Div([
            html.Div([html.H1("US Accidents – Tableau de bord"),
                      html.P("Sqoop · Kafka · Flume → HDFS → Hive : statistiques descriptives, requêtes en direct et flux de données")]),
            html.Div([html.Span(["Source : ", html.B("Hive · HDFS")], className="chip"),
                      html.Span(["Période : ", html.B(periode)], className="chip")], className="chips"),
        ], className="top"),
        dcc.Tabs(id="tabs", value="dash", parent_style={"borderBottom": f"1px solid {LINE}", "marginBottom": "6px"},
                 style={"height": "auto", "border": "0"}, children=[
            dcc.Tab(label="Tableau de bord", value="dash", style=TAB, selected_style=TAB_ON),
            dcc.Tab(label="Requêtes Hive", value="hive", style=TAB, selected_style=TAB_ON),
            dcc.Tab(label="Flux en direct", value="live", style=TAB, selected_style=TAB_ON),
        ]),
        html.Div(id="tab-content", children=tab_dashboard()),
        html.Div("Projet Big Data US_ACCIDENTS · Sqoop / Kafka / Flume → HDFS → Hive → Dashboard", className="foot"),
    ], className="page")


app.layout = layout   # recharge les CSV à chaque rafraîchissement de la page


# ── Callbacks : navigation ──────────────────────────────────────────────────
@app.callback(Output("tab-content", "children"), Input("tabs", "value"))
def render_tab(tab):
    if tab == "hive":
        return tab_hive()
    if tab == "live":
        return tab_live()
    return tab_dashboard()


# ── Callbacks : carte et lecture dans le temps ──────────────────────────────
@app.callback(Output("map", "figure"), Output("tlabel", "children"), Input("layer", "value"), Input("mois", "value"))
def update_map(layer, idx):
    months = months_list()
    label = months[max(0, min(int(idx or 0), len(months) - 1))] if months else ""
    return build_map(layer, idx), label


@app.callback(Output("tick", "disabled"), Output("play", "children"), Input("play", "n_clicks"), prevent_initial_call=True)
def toggle_play(n):
    playing = (n or 0) % 2 == 1
    return (not playing), ("❚❚ Pause" if playing else "▶ Lecture")


@app.callback(Output("mois", "value"), Input("tick", "n_intervals"), State("mois", "value"), State("mois", "max"),
              prevent_initial_call=True)
def advance(_, v, vmax):
    return 0 if (v or 0) >= vmax else (v or 0) + 1


# ── Callbacks : requêtes Hive ───────────────────────────────────────────────
@app.callback(Output("sql", "value"), Input("preset", "value"), prevent_initial_call=True)
def pick_preset(name):
    return PRESETS.get(name, no_update)


@app.callback(Output("hive-out", "children"), Input("run", "n_clicks"), State("sql", "value"), prevent_initial_call=True)
def execute(n, sql):
    sql = (sql or "").strip().rstrip(";").strip()
    if not sql:
        return html.Div("Écrivez d'abord une requête.", className="empty")
    first = sql.split(None, 1)[0].lower()
    if first not in ALLOWED_START or ";" in sql:
        return html.Div("Requête refusée : seules les requêtes de lecture (SELECT, SHOW, DESCRIBE) sont autorisées, une à la fois.", className="err")
    try:
        cols, rows, secs = run_hive(sql)
    except ImportError:
        return html.Div("La bibliothèque PyHive n'est pas installée dans le conteneur du dashboard.\nReconstruisez-le : docker compose up -d --build dashboard", className="err")
    except Exception as e:
        msg = str(e)
        if "Could not connect" in msg or "Connection refused" in msg or "TTransportException" in msg:
            msg = f"HiveServer2 est injoignable ({HIVE_HOST}:{HIVE_PORT}). Vérifiez qu'il est démarré : docker ps\n\n{msg[:300]}"
        return html.Div(msg[:900], className="err")
    meta = html.Div([status_pill(True, "Exécuté"), "  ", html.B(f"{len(rows)} ligne(s)"), f" · {len(cols)} colonne(s) · ",
                     html.B(f"{secs:.1f} s")], className="meta")
    children = [meta, html_table(cols, rows)]
    # petit graphique automatique : 1 colonne texte + 1 colonne numérique
    try:
        if len(cols) >= 2 and 2 <= len(rows) <= 40:
            vals = [float(r[1]) for r in rows]
            f = go.Figure(go.Bar(x=[str(r[0]) for r in rows], y=vals, marker_color=BLUE,
                                 hovertemplate="%{x}<br>%{y:,}<extra></extra>"))
            f.update_xaxes(title=cols[0], type="category")
            f.update_yaxes(title=cols[1])
            children.append(dcc.Graph(figure=style(f, 280), config={"displayModeBar": False}))
    except Exception:
        pass
    return children


# ── Callbacks : flux en direct ──────────────────────────────────────────────
@app.callback(Output("k-status", "children"), Output("k-total", "children"), Output("k-rate", "children"),
              Output("k-graph", "figure"), Output("k-table", "children"), Input("k-tick", "n_intervals"))
def update_kafka(_):
    now = time.time()
    times = list(KAFKA.times)
    rate = sum(1 for t in times if now - t <= 10) / 10.0
    buckets = np.zeros(60)
    for t in times:
        age = int(now - t)
        if 0 <= age < 60:
            buckets[59 - age] += 1
    f = go.Figure(go.Bar(x=list(range(-59, 1)), y=buckets, marker_color=BLUE,
                         hovertemplate="il y a %{x} s : %{y} message(s)<extra></extra>"))
    f.update_xaxes(title="secondes (0 = maintenant)", showgrid=False)
    f.update_yaxes(title="msg/s", rangemode="tozero", range=[0, max(1, float(buckets.max()) * 1.2)])
    f = style(f, 170)
    rows = list(KAFKA.msgs)[:12]
    if rows:
        table = mini_table(rows, ["id", "severity", "start_time", "city", "state", "weather_condition", "temperature_f"])
    else:
        table = html.Div("En attente de messages… lancez le producteur (commande ci-dessous).", className="empty")
    return status_pill(KAFKA.ok, KAFKA.status), fmt(KAFKA.total), f"{rate:.1f}", f, table


@app.callback(Output("f-status", "children"), Output("f-table", "children"),
              Output("s-status", "children"), Output("s-table", "children"), Input("fs-tick", "n_intervals"))
def update_files(_):
    # Flume : dernier fichier terminé (les .tmp sont en cours d'écriture)
    try:
        files = hdfs_list(HDFS_FLUME)
        closed = [f for f in files if not f["pathSuffix"].endswith(".tmp") and f["length"] > 0]
        writing = any(f["pathSuffix"].endswith(".tmp") for f in files)
        if closed:
            last = max(closed, key=lambda f: f["modificationTime"])
            text, offset = hdfs_tail(f"{HDFS_FLUME}/{last['pathSuffix']}", 12000, last["length"])
            lines = text.splitlines()[1:] if offset > 0 else text.splitlines()
            recs = [r for r in (parse_flume(l) for l in lines[-12:]) if r][::-1]
            ftable = mini_table(recs, ["start_time", "id", "severity", "city", "state", "weather_condition"])
            fstat = status_pill(True, f"{len(closed)} fichier(s)" + (" · écriture en cours" if writing else ""))
        else:
            ftable = html.Div("Aucun fichier Flume terminé pour le moment.", className="empty")
            fstat = status_pill(False, "En attente")
    except Exception as e:
        ftable = html.Div(f"HDFS injoignable ({WEBHDFS_URL}) : {str(e)[:150]}", className="err")
        fstat = status_pill(False, "Indisponible")

    # Sqoop : aperçu du premier fichier de l'import
    try:
        files = [f for f in hdfs_list(HDFS_SQOOP) if f["pathSuffix"].startswith("part-") and f["length"] > 0]
        if files:
            first = sorted(files, key=lambda f: f["pathSuffix"])[0]
            text = webhdfs(f"{HDFS_SQOOP}/{first['pathSuffix']}", "OPEN", offset=0, length=6000).decode("utf-8", errors="ignore")
            rows = list(csv.reader(io.StringIO("\n".join(text.splitlines()[:10]))))
            recs = [dict(zip(COLUMNS, r)) for r in rows]
            shifted = sum(1 for r in rows if len(r) != len(COLUMNS))
            stable = mini_table(recs, ["id", "severity", "start_time", "city", "state", "weather_condition"])
            sstat = status_pill(shifted == 0, f"{len(files)} fichier(s)" + ("" if shifted == 0 else f" · {shifted}/10 lignes décalées"))
        else:
            stable = html.Div("Aucun fichier Sqoop dans HDFS pour le moment.", className="empty")
            sstat = status_pill(False, "En attente")
    except Exception as e:
        stable = html.Div(f"HDFS injoignable ({WEBHDFS_URL}) : {str(e)[:150]}", className="err")
        sstat = status_pill(False, "Indisponible")
    return fstat, ftable, sstat, stable


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8050")), debug=False)
