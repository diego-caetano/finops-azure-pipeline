"""
FinOps KMM Dashboard Generator — Plotly Edition
Lê dados do Azure Blob e gera dashboard.html com dados reais
"""
import os, json, io
from datetime import datetime

try:
    import pandas as pd
    from azure.storage.blob import BlobServiceClient
    from dotenv import load_dotenv
except ImportError:
    print("Execute: pip install pandas pyarrow azure-storage-blob python-dotenv")
    exit(1)

load_dotenv()
os.makedirs("output", exist_ok=True)

# ── Configuração ─────────────────────────────────────────────────
CONN_STR  = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER = "finops-v2"
PARQUET   = "gold/fact_allocated_cost_consolidated/fact_allocated_cost_all.parquet"
MANIFEST  = "gold/fact_allocated_cost_consolidated/manifest.csv"
OUTPUT    = "output/dashboard.html"

# ── Carregar dados ───────────────────────────────────────────────
print("Conectando ao Azure Blob Storage...")
client    = BlobServiceClient.from_connection_string(CONN_STR)
container = client.get_container_client(CONTAINER)

print("Baixando parquet...")
fact = pd.read_parquet(io.BytesIO(
    container.get_blob_client(PARQUET).download_blob().readall()
))
fact["Date"] = pd.to_datetime(fact["Date"])

print("Baixando manifest...")
manifest_df = pd.read_csv(io.BytesIO(
    container.get_blob_client(MANIFEST).download_blob().readall()
), sep=";", encoding="latin1")
manifest_df["Date"] = pd.to_datetime(manifest_df["Date"])

print(f"Dados: {len(fact):,} linhas | {fact['Date'].nunique()} datas")

# ── Métricas ─────────────────────────────────────────────────────
total_alocado = fact["AllocatedCost"].sum()
media_diaria  = total_alocado / fact["Date"].nunique()
dias_ok       = int((manifest_df["Status"] == "OK").sum()) if "Status" in manifest_df.columns else fact["Date"].nunique()
n_datas       = fact["Date"].nunique()
cobertura     = (fact["AllocatedCost"].sum() / (fact["Cost"] * fact["Share"]).sum() * 100)
data_min      = fact["Date"].min().strftime("%d/%m/%Y")
data_max      = fact["Date"].max().strftime("%d/%m/%Y")

def fmt(v):
    if v >= 1_000_000: return f"R$ {v/1_000_000:.2f}M"
    if v >= 1_000:     return f"R$ {v/1_000:.1f}K"
    return f"R$ {v:.2f}"

# ── Dados para gráficos ──────────────────────────────────────────
by_date     = fact.groupby("Date", observed=True)["AllocatedCost"].sum().sort_index()
line_labels = [d.strftime("%d/%m") for d in by_date.index]
line_data   = [round(v, 2) for v in by_date.values]

by_method   = fact.groupby("AllocationMethod", observed=True)["AllocatedCost"].sum().sort_values(ascending=False)
donut_labels= by_method.index.tolist()
donut_data  = [round(v, 2) for v in by_method.values]

top10       = fact.groupby("Cliente", observed=True)["AllocatedCost"].sum().nlargest(10).sort_values()
bar_labels  = top10.index.tolist()
bar_data    = [round(v, 2) for v in top10.values]

# Top 3 clientes — série temporal real
top3 = fact.groupby("Cliente", observed=True)["AllocatedCost"].sum().nlargest(3).index.tolist()
area_traces = []
colors_area = ["#F4400A", "#0D0F2B", "#2B7A4B"]
for i, cli in enumerate(top3):
    serie = fact[fact["Cliente"] == cli].groupby("Date", observed=True)["AllocatedCost"].sum().reindex(by_date.index, fill_value=0)
    area_traces.append({
        "x": line_labels,
        "y": [round(v, 2) for v in serie.values],
        "name": cli,
        "color": colors_area[i]
    })

# Manifest
manif_chart  = manifest_df.sort_values("Date").tail(15).copy()
manif_labels = [pd.to_datetime(d).strftime("%d/%m") for d in manif_chart["Date"]]
manif_cost   = [round(float(v), 2) for v in manif_chart.get("TotalCost",   pd.Series([0]*len(manif_chart)))]
manif_alloc  = [round(float(v), 2) for v in manif_chart.get("TotalAllocated", pd.Series([0]*len(manif_chart)))]

# ServiceKeys
top15       = fact.groupby("ServiceKey", observed=True)["AllocatedCost"].sum().nlargest(15).sort_values()
svc_labels  = [s[-40:] if len(s) > 40 else s for s in top15.index.tolist()]
svc_data    = [round(v, 2) for v in top15.values]

# Dedicado vs WP
dedicated   = fact[fact["AllocationMethod"] == "DEDICATED_TAG"].groupby("Cliente", observed=True)["AllocatedCost"].sum()
shared_wp   = fact[fact["AllocationMethod"] == "WEIGHT_PROPORTIONAL"].groupby("Cliente", observed=True)["AllocatedCost"].sum()
total_wp    = shared_wp.sum()
top10_cli   = fact.groupby("Cliente", observed=True)["AllocatedCost"].sum().nlargest(10)
c7_labels   = top10_cli.index.tolist()
c7_ded      = [round(dedicated.get(c, 0), 2) for c in c7_labels]
c7_shr      = [round(shared_wp.get(c, 0), 2) for c in c7_labels]

# Drill RG
by_alloc    = fact.groupby("AllocationMethod", observed=True)["AllocatedCost"].sum().sort_values()
by_rg       = fact.groupby("ResourceGroupName", observed=True)["AllocatedCost"].sum().nlargest(10).sort_values()
by_rec      = fact.groupby("ResourceName", observed=True)["AllocatedCost"].sum().nlargest(10).sort_values()
by_rec.index = [s[-50:] if len(s) > 50 else s for s in by_rec.index]

drill_data = [
    {"labels": by_alloc.index.tolist(), "data": [round(v,2) for v in by_alloc.values], "title": "Custo por AllocationMethod"},
    {"labels": by_rg.index.tolist(),    "data": [round(v,2) for v in by_rg.values],    "title": "Custo por Resource Group"},
    {"labels": by_rec.index.tolist(),   "data": [round(v,2) for v in by_rec.values],   "title": "Custo por Recurso"},
]

# ── Tabelas HTML ─────────────────────────────────────────────────
def badge(s):
    cls = "badge-ok" if str(s) == "OK" else "badge-warn"
    return f'<span class="{cls}">{s}</span>'

# P2 resumo clientes
cli_met = fact.groupby(["Cliente","AllocationMethod"], observed=True)["AllocatedCost"].sum().reset_index()
cli_met = cli_met.loc[cli_met.groupby("Cliente", observed=True)["AllocatedCost"].idxmax()]
cli_res = fact.groupby("Cliente", observed=True)["AllocatedCost"].sum().nlargest(10).reset_index()
cli_res = cli_res.merge(cli_met[["Cliente","AllocationMethod"]], on="Cliente", how="left")
t_clientes = "".join(
    f"<tr><td>{r['Cliente']}</td><td>R$ {r['AllocatedCost']:,.0f}</td>"
    f"<td>{r['AllocatedCost']/total_alocado*100:.1f}%</td><td>{r['AllocationMethod']}</td></tr>"
    for _, r in cli_res.iterrows()
)

# P2 recursos
top_rec = fact.groupby("ResourceName", observed=True)["AllocatedCost"].sum().nlargest(8).reset_index()
t_recursos = "".join(
    f"<tr><td>{r['ResourceName'][:35]}</td><td>R$ {r['AllocatedCost']:,.0f}</td></tr>"
    for _, r in top_rec.iterrows()
)

# P3 manifest
t_manifest = ""
for _, r in manifest_df.sort_values("Date", ascending=False).head(25).iterrows():
    st  = str(r.get("Status", "OK"))
    dat = pd.to_datetime(r["Date"]).strftime("%d/%m/%Y")
    pro = pd.to_datetime(r["ProcessedAt"]).strftime("%d/%m %H:%M") if pd.notna(r.get("ProcessedAt")) else "-"
    t_manifest += f"<tr><td>{dat}</td><td>{badge(st)}</td><td>{int(r.get('Rows',0)):,}</td><td>R$ {float(r.get('TotalAllocated',0)):,.0f}</td><td>R$ {float(r.get('TotalCost',0)):,.0f}</td><td>{pro}</td></tr>"

# P4 serviços
svc_det = fact.groupby(["ServiceKey","Cliente","AllocationMethod"], observed=True).agg(
    AllocatedCost=("AllocatedCost","sum"), ShareMedio=("Share","mean")
).reset_index().nlargest(15, "AllocatedCost")
t_servicos = "".join(
    f"<tr><td>{r['ServiceKey'][-40:]}</td><td>{r['Cliente']}</td><td>R$ {r['AllocatedCost']:,.0f}</td><td>{r['AllocationMethod']}</td><td>{r['ShareMedio']*100:.1f}%</td></tr>"
    for _, r in svc_det.iterrows()
)

# P5 custo por cliente
t_custo_cli = ""
for cli in top10_cli.index:
    ded = dedicated.get(cli, 0)
    shr = shared_wp.get(cli, 0)
    pct = (shr/total_wp*100) if total_wp > 0 else 0
    tot = top10_cli[cli]
    bw  = min(int(pct*2), 100)
    t_custo_cli += (
        f"<tr><td><strong>{cli}</strong></td><td>R$ {ded:,.0f}</td><td>R$ {shr:,.0f}</td>"
        f"<td><div class='pct-wrap'><div class='pct-bg'><div class='pct-fill' style='width:{bw}%'></div></div>"
        f"<span style='font-size:11px;color:#F4400A;font-weight:600'>{pct:.1f}%</span></div></td>"
        f"<td><strong>R$ {tot:,.0f}</strong></td></tr>"
    )

# Drill tables
def drill_table(series, col_name):
    tot = series.sum()
    rows = "".join(
        f"<tr><td>{n}</td><td>R$ {v:,.0f}</td><td>{v/tot*100:.1f}%</td></tr>"
        for n, v in series.sort_values(ascending=False).items()
    )
    return f"<table><thead><tr><th>{col_name}</th><th>Custo Alocado</th><th>% Total</th></tr></thead><tbody>{rows}</tbody></table>"

t_drill = [
    drill_table(by_alloc.sort_values(ascending=False), "Método"),
    drill_table(by_rg.sort_values(ascending=False),    "Resource Group"),
    drill_table(by_rec.sort_values(ascending=False),   "Recurso"),
]

# ── Serializar dados para JS ─────────────────────────────────────
JS_DATA = f"""
var LINE_LABELS  = {json.dumps(line_labels)};
var LINE_DATA    = {json.dumps(line_data)};
var DONUT_LABELS = {json.dumps(donut_labels)};
var DONUT_DATA   = {json.dumps(donut_data)};
var BAR_LABELS   = {json.dumps(bar_labels)};
var BAR_DATA     = {json.dumps(bar_data)};
var AREA_TRACES  = {json.dumps(area_traces)};
var MANIF_LABELS = {json.dumps(manif_labels)};
var MANIF_COST   = {json.dumps(manif_cost)};
var MANIF_ALLOC  = {json.dumps(manif_alloc)};
var SVC_LABELS   = {json.dumps(svc_labels)};
var SVC_DATA     = {json.dumps(svc_data)};
var C7_LABELS    = {json.dumps(c7_labels)};
var C7_DED       = {json.dumps(c7_ded)};
var C7_SHR       = {json.dumps(c7_shr)};
var DRILL        = {json.dumps(drill_data)};
var DRILL_TABLES = {json.dumps(t_drill)};
"""

JS_LOGIC = """
var RED='#F4400A', DARK='#0D0F2B', GREEN='#2B7A4B';
var COLORS=['#F4400A','#2B7A4B','#C8C8C8','#FF6B35','#0D0F2B','#FF9B6B'];
var LB={plot_bgcolor:'white',paper_bgcolor:'white',font:{family:'Barlow,sans-serif',size:11},margin:{l:10,r:20,t:10,b:50}};

function showTab(id,btn){
  var pages=document.querySelectorAll('.page');
  var tabs=document.querySelectorAll('.tab');
  for(var i=0;i<pages.length;i++) pages[i].classList.remove('active');
  for(var i=0;i<tabs.length;i++) tabs[i].classList.remove('active');
  document.getElementById(id).classList.add('active');
  btn.classList.add('active');
}

window.onload=function(){
  // G1 linha
  Plotly.newPlot('g1',[{x:LINE_LABELS,y:LINE_DATA,type:'scatter',mode:'lines+markers',
    line:{color:RED,width:2},marker:{size:4,color:RED},fill:'tozeroy',fillcolor:'rgba(244,64,10,0.07)',
    hovertemplate:'%{x}<br>R$ %{y:,.0f}<extra></extra>'}],
    Object.assign({},LB,{xaxis:{showgrid:false},yaxis:{gridcolor:'#EEE',tickprefix:'R$ ',tickformat:',d'}}),
    {responsive:true,displayModeBar:false});

  // G2 donut
  Plotly.newPlot('g2',[{labels:DONUT_LABELS,values:DONUT_DATA,type:'pie',hole:0.6,
    marker:{colors:COLORS},textinfo:'percent',textfont:{size:10},
    hovertemplate:'%{label}<br>R$ %{value:,.0f}<extra></extra>'}],
    Object.assign({},LB,{showlegend:true,legend:{font:{size:10},orientation:'v'},margin:{l:0,r:0,t:0,b:0}}),
    {responsive:true,displayModeBar:false});

  // G3 top clientes
  Plotly.newPlot('g3',[{x:BAR_DATA,y:BAR_LABELS,type:'bar',orientation:'h',
    marker:{color:RED},hovertemplate:'%{y}<br>R$ %{x:,.0f}<extra></extra>'}],
    Object.assign({},LB,{xaxis:{gridcolor:'#EEE',tickprefix:'R$ ',tickformat:',d'},
    yaxis:{showgrid:false},margin:{l:10,r:80,t:10,b:20}}),
    {responsive:true,displayModeBar:false});

  // G4 area top3
  var traces4=[];
  var areaColors=[RED,DARK,GREEN];
  for(var i=0;i<AREA_TRACES.length;i++){
    traces4.push({x:AREA_TRACES[i].x,y:AREA_TRACES[i].y,name:AREA_TRACES[i].name,
      type:'scatter',mode:'lines',line:{color:AREA_TRACES[i].color,width:2},
      fill:i===0?'tozeroy':'tonexty',fillcolor:areaColors[i]+'22',
      hovertemplate:'%{x}<br>R$ %{y:,.0f}<extra></extra>'});
  }
  Plotly.newPlot('g4',traces4,
    Object.assign({},LB,{xaxis:{showgrid:false},yaxis:{gridcolor:'#EEE',tickprefix:'R$ '},
    legend:{orientation:'h',y:-0.25,font:{size:10}}}),
    {responsive:true,displayModeBar:false});

  // G5 manifest
  Plotly.newPlot('g5',
    [{x:MANIF_LABELS,y:MANIF_COST, type:'bar',name:'Total Cost',     marker:{color:DARK}},
     {x:MANIF_LABELS,y:MANIF_ALLOC,type:'bar',name:'Total Allocated',marker:{color:RED}}],
    Object.assign({},LB,{barmode:'group',xaxis:{showgrid:false},
    yaxis:{gridcolor:'#EEE',tickprefix:'R$ ',tickformat:',d'},
    legend:{orientation:'h',y:-0.3,font:{size:10}}}),
    {responsive:true,displayModeBar:false});

  // G6 servicekeys
  Plotly.newPlot('g6',[{x:SVC_DATA,y:SVC_LABELS,type:'bar',orientation:'h',
    marker:{color:RED},hovertemplate:'%{y}<br>R$ %{x:,.0f}<extra></extra>'}],
    Object.assign({},LB,{xaxis:{gridcolor:'#EEE',tickprefix:'R$ ',tickformat:',d'},
    yaxis:{showgrid:false},height:420,margin:{l:10,r:10,t:10,b:20}}),
    {responsive:true,displayModeBar:false});

  // G7 dedicado vs WP
  Plotly.newPlot('g7',
    [{x:C7_LABELS,y:C7_DED,type:'bar',name:'Dedicado',           marker:{color:DARK}},
     {x:C7_LABELS,y:C7_SHR,type:'bar',name:'Compartilhado (WP)', marker:{color:RED}}],
    Object.assign({},LB,{barmode:'group',xaxis:{showgrid:false,tickangle:-30},
    yaxis:{gridcolor:'#EEE',tickprefix:'R$ ',tickformat:',d'},
    legend:{orientation:'h',y:-0.35,font:{size:10}}}),
    {responsive:true,displayModeBar:false});

  setDrill(0);
};

function setDrill(n){
  var btns=document.querySelectorAll('.dbtn');
  for(var i=0;i<btns.length;i++) btns[i].className=i===n?'dbtn':'dbtn ghost';
  document.getElementById('drill-title').textContent=DRILL[n].title;
  document.getElementById('drill-table').innerHTML=DRILL_TABLES[n];
  Plotly.newPlot('g8',[{x:DRILL[n].data,y:DRILL[n].labels,type:'bar',orientation:'h',
    marker:{color:RED},hovertemplate:'%{y}<br>R$ %{x:,.0f}<extra></extra>'}],
    Object.assign({},{plot_bgcolor:'white',paper_bgcolor:'white',font:{family:'Barlow,sans-serif',size:11},margin:{l:10,r:20,t:10,b:30}},
    {xaxis:{gridcolor:'#EEE',tickprefix:'R$ ',tickformat:',d'},yaxis:{showgrid:false}}),
    {responsive:true,displayModeBar:false});
}
"""

# ── HTML ─────────────────────────────────────────────────────────
HTML = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>FinOps KMM Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Barlow:wght@400;500;600&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@2.44.0/tabler-icons.min.css">
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Barlow',sans-serif;background:#F5F5F5;color:#111}}
.topbar{{background:#0D0F2B;padding:12px 24px;display:flex;align-items:center;justify-content:space-between}}
.logo{{color:#fff;font-size:16px;font-weight:600;display:flex;align-items:center;gap:8px}}
.badge{{background:#F4400A;color:#fff;font-size:11px;padding:2px 8px;border-radius:4px;font-weight:600}}
.sub{{color:#C8C8C8;font-size:11px}}
.tabs{{background:#fff;border-bottom:2px solid #EEE;display:flex;padding:0 24px;overflow-x:auto}}
.tab{{padding:10px 18px;font-size:13px;font-weight:500;cursor:pointer;border:none;background:none;color:#666;border-bottom:3px solid transparent;margin-bottom:-2px;white-space:nowrap;font-family:'Barlow',sans-serif}}
.tab.active{{color:#F4400A;border-bottom:3px solid #F4400A;font-weight:600}}
.page{{display:none;padding:16px 24px 28px}}
.page.active{{display:block}}
.kpis{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px}}
.kpi{{background:#fff;border-radius:8px;padding:14px 16px;border-left:4px solid #F4400A;border-top:0.5px solid #EEE;border-right:0.5px solid #EEE;border-bottom:0.5px solid #EEE}}
.kpi-label{{font-size:11px;color:#666;margin-bottom:4px;font-weight:500;text-transform:uppercase;letter-spacing:0.3px}}
.kpi-value{{font-size:26px;font-weight:600;color:#111}}
.kpi-sub{{font-size:10px;color:#999;margin-top:2px}}
.card{{background:#fff;border-radius:8px;border:0.5px solid #EEE;padding:16px;margin-bottom:12px}}
.card-title{{font-size:13px;font-weight:600;color:#111;margin-bottom:8px}}
.row2{{display:grid;grid-template-columns:2fr 1fr;gap:12px;margin-bottom:12px}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
thead tr{{background:#F4400A}}
thead th{{color:#fff;padding:9px 12px;text-align:left;font-weight:600;font-size:11px;text-transform:uppercase}}
tbody tr{{border-bottom:0.5px solid #EEE}}
tbody tr:nth-child(even){{background:#FFF4F1}}
tbody tr:hover{{background:#FFE8E2}}
td{{padding:8px 12px;color:#111;font-size:12px}}
.badge-ok{{background:#E6F4EC;color:#2B7A4B;font-size:10px;padding:2px 7px;border-radius:4px;font-weight:600}}
.badge-warn{{background:#FFF0EB;color:#F4400A;font-size:10px;padding:2px 7px;border-radius:4px;font-weight:600}}
.drill-bar{{display:flex;gap:8px;align-items:center;margin-bottom:12px;flex-wrap:wrap}}
.dbtn{{background:#F4400A;color:#fff;border:none;border-radius:4px;padding:6px 14px;font-size:12px;cursor:pointer;font-family:'Barlow',sans-serif;font-weight:600}}
.dbtn.ghost{{background:#fff;color:#F4400A;border:1px solid #F4400A}}
.pct-wrap{{display:flex;align-items:center;gap:6px}}
.pct-bg{{width:70px;height:6px;border-radius:3px;background:#EEE;position:relative;display:inline-block}}
.pct-fill{{height:100%;border-radius:3px;background:#F4400A;position:absolute;left:0;top:0}}
.footer{{text-align:right;font-size:10px;color:#999;padding:8px 24px 16px}}
</style>
</head>
<body>
<div class="topbar">
  <div class="logo"><i class="ti ti-chart-pie"></i>FinOps KMM<span class="badge">Azure Cost</span></div>
  <div class="sub">{data_min} – {data_max} &nbsp;·&nbsp; {len(fact):,} registros &nbsp;·&nbsp; stpslkmmfinopseusprd</div>
</div>
<div class="tabs">
  <button class="tab active" onclick="showTab('p1',this)">Visão Geral</button>
  <button class="tab" onclick="showTab('p2',this)">Por Cliente</button>
  <button class="tab" onclick="showTab('p3',this)">Pipeline</button>
  <button class="tab" onclick="showTab('p4',this)">Por Serviço</button>
  <button class="tab" onclick="showTab('p5',this)">Custo por Cliente</button>
  <button class="tab" onclick="showTab('p6',this)">Alocação por RG</button>
</div>

<div class="page active" id="p1">
  <div class="kpis">
    <div class="kpi"><div class="kpi-label">Total Alocado</div><div class="kpi-value">{fmt(total_alocado)}</div><div class="kpi-sub">{n_datas} dias processados</div></div>
    <div class="kpi"><div class="kpi-label">Média Diária</div><div class="kpi-value">{fmt(media_diaria)}</div><div class="kpi-sub">por dia</div></div>
    <div class="kpi"><div class="kpi-label">Dias OK</div><div class="kpi-value">{dias_ok} / {n_datas}</div><div class="kpi-sub">status pipeline</div></div>
    <div class="kpi"><div class="kpi-label">Cobertura</div><div class="kpi-value">{cobertura:.1f}%</div><div class="kpi-sub">alocado vs bruto</div></div>
  </div>
  <div class="row2">
    <div class="card"><div class="card-title">Custo alocado por data</div><div id="g1" style="height:220px"></div></div>
    <div class="card"><div class="card-title">Por método de alocação</div><div id="g2" style="height:220px"></div></div>
  </div>
  <div class="card"><div class="card-title">Top 10 clientes por custo alocado</div><div id="g3" style="height:290px"></div></div>
</div>

<div class="page" id="p2">
  <div class="row2">
    <div class="card"><div class="card-title">Resumo por cliente</div>
      <table><thead><tr><th>Cliente</th><th>Alocado</th><th>% Total</th><th>Método principal</th></tr></thead>
      <tbody>{t_clientes}</tbody></table></div>
    <div class="card"><div class="card-title">Top recursos</div>
      <table><thead><tr><th>Recurso</th><th>Alocado</th></tr></thead>
      <tbody>{t_recursos}</tbody></table></div>
  </div>
  <div class="card"><div class="card-title">Evolução diária — top 3 clientes</div><div id="g4" style="height:260px"></div></div>
</div>

<div class="page" id="p3">
  <div class="kpis" style="grid-template-columns:repeat(2,1fr);max-width:420px;margin-bottom:16px">
    <div class="kpi"><div class="kpi-label">Dias OK</div><div class="kpi-value">{dias_ok}</div><div class="kpi-sub">de {n_datas} processados</div></div>
    <div class="kpi"><div class="kpi-label">Cobertura</div><div class="kpi-value">{cobertura:.1f}%</div><div class="kpi-sub">alocado vs bruto</div></div>
  </div>
  <div class="card"><div class="card-title">Manifest do pipeline</div>
    <table><thead><tr><th>Data</th><th>Status</th><th>Linhas</th><th>Total Alocado</th><th>Total Custo</th><th>Processado em</th></tr></thead>
    <tbody>{t_manifest}</tbody></table></div>
  <div class="card"><div class="card-title">TotalCost vs TotalAllocated</div><div id="g5" style="height:240px"></div></div>
</div>

<div class="page" id="p4">
  <div class="card"><div class="card-title">Top 15 ServiceKeys por custo alocado</div><div id="g6" style="height:440px"></div></div>
  <div class="card"><div class="card-title">Detalhe por serviço, cliente e método</div>
    <table><thead><tr><th>ServiceKey</th><th>Cliente</th><th>Alocado</th><th>Método</th><th>Share médio</th></tr></thead>
    <tbody>{t_servicos}</tbody></table></div>
</div>

<div class="page" id="p5">
  <div class="card"><div class="card-title">Custo dedicado vs compartilhado por cliente</div>
    <table><thead><tr><th>Cliente</th><th>Custo Dedicado</th><th>Custo Compartilhado (WP)</th><th>% do Pool WP</th><th>Total Alocado</th></tr></thead>
    <tbody>{t_custo_cli}</tbody></table></div>
  <div class="card"><div class="card-title">Dedicado vs Compartilhado — comparativo</div><div id="g7" style="height:320px"></div></div>
</div>

<div class="page" id="p6">
  <div class="drill-bar">
    <button class="dbtn" onclick="setDrill(0)">AllocationMethod</button>
    <i class="ti ti-chevron-right" style="color:#999;font-size:14px"></i>
    <button class="dbtn ghost" onclick="setDrill(1)">Resource Group</button>
    <i class="ti ti-chevron-right" style="color:#999;font-size:14px"></i>
    <button class="dbtn ghost" onclick="setDrill(2)">Recurso</button>
  </div>
  <div class="card"><div class="card-title" id="drill-title">Custo por AllocationMethod</div><div id="g8" style="height:320px"></div></div>
  <div class="card"><div class="card-title">Detalhe tabular</div><div id="drill-table"></div></div>
</div>

<div class="footer">Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')} · FinOps KMM Pipeline</div>

<script>
{JS_DATA}
{JS_LOGIC}
</script>
</body>
</html>"""

with open(OUTPUT, "w", encoding="utf-8") as f:
    f.write(HTML)

print(f"\n✅ Dashboard gerado: {OUTPUT}")
print(f"   Inicie o servidor: cd output && python -m http.server 8080")
print(f"   Abra: http://localhost:8080/dashboard.html")