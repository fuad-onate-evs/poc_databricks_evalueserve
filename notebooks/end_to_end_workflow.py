# Databricks notebook source
# MAGIC %md
# MAGIC # Renewable-Energy PoC — End-to-End Data Workflow
# MAGIC
# MAGIC Two renders of the same flow: a **native Mermaid** diagram (always works in
# MAGIC Databricks markdown) and an **interactive React** version (`displayHTML`, below)
# MAGIC where you click nodes and toggle the Kafka path.
# MAGIC
# MAGIC Source of truth: `docs/end-to-end-data-workflow.md` + `docs/end-to-end-workflow.html`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Native diagram (Mermaid)
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TB
# MAGIC     subgraph SRC["1 - Sources"]
# MAGIC         DGF["DGF . api.minenergia.cl (MAIN) - hourly solar/wind"]
# MAGIC         GEN["CEN generation - coordinado/real/reducciones (synthetic today)"]
# MAGIC         OWID["Our World in Data - country renewable stats (CSV)"]
# MAGIC     end
# MAGIC     subgraph ING["2 - Ingestion (Kafka = optional branch)"]
# MAGIC         POLL["dgf_poller.py (Databricks Job, hourly)"]
# MAGIC         VOL[("UC Volume landing /Volumes/.../lookup/...")]
# MAGIC         KAFKA{{"Kafka topic - only if sub-minute / streaming"}}
# MAGIC     end
# MAGIC     subgraph DBX["3 - Medallion (DLT)"]
# MAGIC         BRONZE["BRONZE - Auto Loader (cloudFiles) raw + _rescued_data"]
# MAGIC         SILVER["SILVER - resource: DV hub/link/sat (hourly); conglomerate: dim+fact"]
# MAGIC         GOLD["GOLD - daily/weekly/monthly + diff; YoY increase; Chile vs world"]
# MAGIC     end
# MAGIC     subgraph USE["4 - Consumption"]
# MAGIC         DASH["AI/BI dashboards - Medallion Health, End-to-End"]
# MAGIC         SQL["SQL editor . notebooks"]
# MAGIC     end
# MAGIC     DGF --> POLL
# MAGIC     POLL -->|recommended: land JSON files| VOL
# MAGIC     POLL -.->|only if streaming required| KAFKA
# MAGIC     KAFKA -.->|read_kafka / Kafka source| BRONZE
# MAGIC     GEN --> VOL
# MAGIC     OWID --> VOL
# MAGIC     VOL --> BRONZE
# MAGIC     BRONZE --> SILVER --> GOLD
# MAGIC     GOLD --> DASH
# MAGIC     GOLD --> SQL
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interactive diagram (click nodes · toggle Kafka)
# MAGIC Run the next cell. If your workspace blocks the external CDNs in `displayHTML`,
# MAGIC use the Mermaid render above (or open `docs/end-to-end-workflow.html` in a browser).

# COMMAND ----------

html = r"""
<!doctype html><html><head><meta charset="utf-8"/>
<script src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
<script src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
<script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
<script src="https://cdn.tailwindcss.com"></script>
<style>.node{transition:transform .12s ease;cursor:pointer}.node:hover{transform:translateY(-2px)}</style>
</head><body class="text-slate-800" style="font-family:ui-sans-serif,system-ui,sans-serif">
<div id="root" class="p-4"></div>
<script type="text/babel">
const {useState}=React;
const NODES={
 dgf:{stage:'sources',title:'DGF . api.minenergia.cl',badge:'MAIN',badgeCls:'bg-sky-600 text-white',desc:'U. de Chile Geophysics (DGF) + MinEnergia API - hourly solar/wind. Pulled by the poller (pending approval).',file:'scripts/dgf_poller.py'},
 cen:{stage:'sources',title:'CEN generation feed',badge:'synthetic today',badgeCls:'bg-amber-100 text-amber-800',desc:'Coordinador generation per plant, hourly - coordinado/real/reducciones. Synthetic CSV until a real feed is wired.',file:'lookup CSVs'},
 owid:{stage:'sources',title:'Our World in Data',desc:'Country renewable stats (Entity/Year) - hydro, solar, wind, share.',file:'lookup CSVs'},
 poller:{stage:'ingest',title:'dgf_poller.py',badge:'hourly Job',badgeCls:'bg-violet-100 text-violet-800',desc:'Django login (CSRF, no reCAPTCHA), polls /api/, lands files. Runs as a Databricks Job hourly.',file:'scripts/dgf_poller.py'},
 volume:{stage:'ingest',title:'UC Volume landing',badge:'recommended',badgeCls:'bg-emerald-100 text-emerald-800',desc:'/Volumes/<catalog>/<bronze_schema>/lookup/... landed JSON/CSV. Cheap, replayable, keeps file_path lineage.',file:'resources/volume.yml'},
 kafka:{stage:'ingest',title:'Kafka topic',badge:'OPTIONAL',badgeCls:'bg-amber-500 text-white',optional:true,desc:'Streaming bus between poller and bronze. Bypassed for hourly cadence (broker = cost+ops). Use only for sub-minute / multi-consumer / replay.',file:'dev: localhost:9092'},
 bronze:{stage:'bronze',title:'BRONZE',desc:'Auto Loader (cloudFiles) ingests landed files -> raw + _rescued_data + file_path. (Or a Kafka source.)',file:'transformations/bronze_*.py'},
 silver:{stage:'silver',title:'SILVER',desc:'resource: Data Vault hub/link/sat (hourly). conglomerate: dim_country + fact_*.',file:'transformations/silver_*.py'},
 gold:{stage:'gold',title:'GOLD',desc:'resource: daily/weekly/monthly + real-vs-coordinated diff. conglomerate: YoY increase + Chile-vs-LATAM/World.',file:'transformations/gold_*.py'},
 dash:{stage:'consume',title:'AI/BI dashboards',desc:'Lakeview - Medallion Health + End-to-End.',file:'Lakeview'},
 sql:{stage:'consume',title:'SQL editor . notebooks',desc:'Ad-hoc SQL + sample_data_queries / pipeline_walkthrough / data_catalog.',file:'notebooks/'},
};
const SM={sources:{label:'1 - Sources',ring:'ring-sky-200',head:'text-sky-700'},ingest:{label:'2 - Ingestion',ring:'ring-violet-200',head:'text-violet-700'},bronze:{label:'3 - Medallion (DLT)',ring:'ring-amber-200',head:'text-amber-700'},silver:{label:'Silver',ring:'ring-slate-300',head:'text-slate-600'},gold:{label:'Gold',ring:'ring-yellow-300',head:'text-yellow-700'},consume:{label:'4 - Consumption',ring:'ring-emerald-200',head:'text-emerald-700'}};
function Node({id,selected,onSelect,mode}){const n=NODES[id];const dim=(id==='kafka'&&mode==='batch')||(id==='volume'&&mode==='stream');const a=selected===id;
 return(<div onClick={()=>onSelect(a?null:id)} className={"node rounded-xl px-3 py-2 bg-white shadow-sm ring-1 border text-sm "+(a?"ring-2 ring-indigo-400 shadow-md":SM[n.stage].ring)+" "+(n.optional?"border-dashed border-amber-400":"border-slate-200")} style={{opacity:dim?0.4:1}}>
 <div className="flex items-center gap-2"><span className="font-semibold">{n.title}</span>{n.badge&&<span className={"text-[10px] px-1.5 py-0.5 rounded-full "+n.badgeCls}>{n.badge}</span>}</div>
 <div className="text-[11px] text-slate-400 mt-0.5">{n.file}</div></div>);}
const Chev=()=><div className="text-2xl text-slate-300 self-center px-1">&#9654;</div>;
const Dn=()=><div className="text-xl text-slate-300 text-center leading-none">&#9660;</div>;
function Stage({stage,children}){return(<div className="rounded-2xl bg-white/50 p-3 ring-1 ring-slate-200"><div className={"text-xs font-semibold mb-2 "+SM[stage].head}>{SM[stage].label}</div>{children}</div>);}
function App(){const[selected,setSelected]=useState('kafka');const[mode,setMode]=useState('batch');const s=selected?NODES[selected]:null;
 const col=(ids)=><div className="flex flex-col gap-2 justify-center min-w-[180px]">{ids.map(id=><Node key={id} id={id} selected={selected} onSelect={setSelected} mode={mode}/>)}</div>;
 return(<div><h1 className="text-lg font-bold">End-to-End Data Workflow</h1>
 <div className="flex items-center gap-3 my-3"><span className="text-xs text-slate-500">Ingestion mode:</span>
  <div className="inline-flex rounded-lg bg-slate-100 p-1 text-xs">
   <button onClick={()=>setMode('batch')} className={"px-3 py-1 rounded-md "+(mode==='batch'?'bg-white shadow text-emerald-700 font-semibold':'text-slate-500')}>Batch . hourly</button>
   <button onClick={()=>setMode('stream')} className={"px-3 py-1 rounded-md "+(mode==='stream'?'bg-white shadow text-amber-700 font-semibold':'text-slate-500')}>Streaming . Kafka</button></div>
  <span className="text-xs text-slate-400">{mode==='batch'?'Poller -> UC Volume -> Auto Loader. No broker.':'Poller -> Kafka -> read by bronze. Always-on.'}</span></div>
 <div className="flex gap-1 overflow-x-auto pb-3">
  <Stage stage="sources">{col(['dgf','cen','owid'])}</Stage><Chev/>
  <Stage stage="ingest"><div className="flex flex-col gap-2 justify-center min-w-[190px]"><Node id="poller" selected={selected} onSelect={setSelected} mode={mode}/><Dn/><Node id="volume" selected={selected} onSelect={setSelected} mode={mode}/><div className="text-center text-[10px] text-slate-400">- or -</div><Node id="kafka" selected={selected} onSelect={setSelected} mode={mode}/></div></Stage><Chev/>
  <Stage stage="bronze"><div className="flex flex-col gap-2 justify-center min-w-[200px]"><Node id="bronze" selected={selected} onSelect={setSelected} mode={mode}/><Dn/><Node id="silver" selected={selected} onSelect={setSelected} mode={mode}/><Dn/><Node id="gold" selected={selected} onSelect={setSelected} mode={mode}/></div></Stage><Chev/>
  <Stage stage="consume">{col(['dash','sql'])}</Stage></div>
 <div className="mt-2 rounded-xl border border-slate-200 bg-white p-4 shadow-sm min-h-[90px]">{s?(<div><div className="flex items-center gap-2"><h2 className="font-semibold">{s.title}</h2>{s.badge&&<span className={"text-[10px] px-1.5 py-0.5 rounded-full "+s.badgeCls}>{s.badge}</span>}<code className="ml-auto text-[11px] text-slate-400">{s.file}</code></div><p className="text-sm text-slate-600 mt-1.5">{s.desc}</p></div>):(<p className="text-sm text-slate-400">Click a node to see what it does and which code owns it.</p>)}</div>
 </div>);}
ReactDOM.createRoot(document.getElementById('root')).render(<App/>);
</script></body></html>
"""
displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where Kafka fits
# MAGIC Kafka is a **streaming message bus** that would sit **between the poller and bronze**.
# MAGIC The brief writes `DGF -> Kafka -> Databricks`, but the agreed cadence is **hourly**, so the
# MAGIC recommended design (`docs/card-56-bronze-landing-design.md` §3) **drops the broker**: the poller
# MAGIC lands JSON in a UC Volume and Auto Loader ingests it. Turn Kafka on only for **sub-minute /
# MAGIC multi-consumer / replay** needs.
# MAGIC
# MAGIC | Stage | What happens | Code |
# MAGIC |---|---|---|
# MAGIC | Source | DGF API (main), CEN generation (synthetic), OWID CSV | `scripts/dgf_poller.py` |
# MAGIC | Ingestion | Hourly poll -> land files in UC Volume (or Kafka, optional) | `dgf_poller.py`, `resources/volume.yml` |
# MAGIC | Bronze | Auto Loader reads landed files; raw + `_rescued_data` + `file_path` | `transformations/bronze_*.py` |
# MAGIC | Silver | resource: DV hub/link/sat (hourly); conglomerate: dim/fact | `transformations/silver_*.py` |
# MAGIC | Gold | daily/weekly/monthly + diff; YoY increase + comparisons | `transformations/gold_*.py` |
# MAGIC | Consumption | AI/BI dashboards, SQL editor, notebooks | `notebooks/`, Lakeview |
