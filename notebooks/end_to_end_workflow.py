# Databricks notebook source
# MAGIC %md
# MAGIC # Renewable-Energy PoC — End-to-End Data Workflow
# MAGIC
# MAGIC Run the next cell for the **interactive diagram** — click any node for detail, and
# MAGIC toggle the ingestion mode to see where **Kafka** fits. It is self-contained
# MAGIC (inline HTML/CSS/JS, **no external libraries**), so it renders even if this
# MAGIC workspace blocks CDNs or native Mermaid.

# COMMAND ----------

html = r"""
<div id="wf">
<style>
  #wf{font-family:ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;color:#1e293b;background:#f8fafc;padding:14px;border-radius:12px}
  #wf h1{font-size:18px;margin:0 0 2px}
  #wf .sub{font-size:12px;color:#64748b;margin-bottom:10px}
  #wf .banner{background:#fffbeb;border:1px solid #fde68a;color:#92400e;font-size:11.5px;padding:6px 10px;border-radius:8px;margin-bottom:10px}
  #wf .ctrls{display:flex;align-items:center;gap:10px;margin-bottom:8px;flex-wrap:wrap}
  #wf .seg{display:inline-flex;background:#e2e8f0;border-radius:9px;padding:3px}
  #wf .seg button{border:0;background:transparent;font-size:12px;padding:5px 11px;border-radius:7px;cursor:pointer;color:#475569}
  #wf .seg button.active{background:#fff;box-shadow:0 1px 2px rgba(0,0,0,.1);font-weight:600;color:#0f766e}
  #wf .note{font-size:12px;color:#94a3b8}
  #wf .legend{display:flex;gap:13px;flex-wrap:wrap;font-size:11px;color:#64748b;margin-bottom:10px;align-items:center}
  #wf .legend i{display:inline-block;width:9px;height:9px;border-radius:3px;margin-right:4px;vertical-align:middle}
  #wf .legend i.dash{background:transparent;border:1.5px dashed #f59e0b}
  #wf .flow{display:flex;gap:6px;align-items:stretch;overflow-x:auto;padding-bottom:8px}
  #wf .stage{background:rgba(255,255,255,.6);border:1px solid #e2e8f0;border-radius:16px;padding:10px;min-width:185px}
  #wf .sh{font-size:11px;font-weight:700;margin-bottom:8px;color:#475569}
  #wf .col{display:flex;flex-direction:column;gap:8px;justify-content:center;height:calc(100% - 22px)}
  #wf .node{background:#fff;border:1px solid #e2e8f0;border-left:4px solid #cbd5e1;border-radius:11px;padding:8px 10px;cursor:pointer;transition:transform .12s,box-shadow .12s,opacity .2s;box-shadow:0 1px 2px rgba(0,0,0,.05)}
  #wf .node:hover{transform:translateY(-2px);box-shadow:0 4px 10px rgba(0,0,0,.08)}
  #wf .node.sel{outline:2px solid #6366f1;outline-offset:1px}
  #wf .node.dim{opacity:.38}
  #wf .node.opt{border-style:dashed;border-color:#f59e0b}
  #wf .node.sources{border-left-color:#0284c7}
  #wf .node.ingest{border-left-color:#7c3aed}
  #wf .node.bronze{border-left-color:#b45309}
  #wf .node.silver{border-left-color:#475569}
  #wf .node.gold{border-left-color:#a16207}
  #wf .node.consume{border-left-color:#059669}
  #wf .nt{font-weight:600;font-size:13px;display:flex;align-items:center;gap:6px;flex-wrap:wrap}
  #wf .nf{font-size:10.5px;color:#94a3b8;margin-top:2px}
  #wf .sdot{display:inline-block;width:8px;height:8px;border-radius:50%}
  #wf .badge{font-size:9.5px;padding:1px 6px;border-radius:999px}
  #wf .b-main{background:#0284c7;color:#fff}#wf .b-syn{background:#fef3c7;color:#92400e}
  #wf .b-rec{background:#d1fae5;color:#065f46}#wf .b-job{background:#ede9fe;color:#5b21b6}#wf .b-opt{background:#f59e0b;color:#fff}
  #wf .chev{align-self:center;color:#cbd5e1;font-size:22px;padding:0 2px}
  #wf .dn{text-align:center;color:#cbd5e1;font-size:15px;line-height:1}
  #wf .orr{text-align:center;font-size:10px;color:#94a3b8}
  #wf .detail{margin-top:10px;background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:12px;min-height:74px;box-shadow:0 1px 2px rgba(0,0,0,.05)}
  #wf .dh{display:flex;align-items:center;gap:8px}#wf .dh b{font-size:14px}
  #wf .dh code{margin-left:auto;font-size:11px;color:#94a3b8}
  #wf .dst{font-size:11px;margin-top:4px}
  #wf .dd{font-size:13px;color:#475569;margin-top:6px}
</style>
<h1>End-to-End Data Workflow</h1>
<div class="sub">Chilean renewable-energy medallion on Databricks · click any node · toggle ingestion mode to see Kafka's role.</div>
<div class="banner">&#9888; Running on <b>synthetic sample data</b> — the real DGF feed is pending approval (card&nbsp;#56). Treat magnitudes as placeholders.</div>
<div class="ctrls">
  <span class="note" style="font-weight:600;color:#64748b">Ingestion mode:</span>
  <div class="seg"><button id="mb" class="active" onclick="wfMode('batch')">Batch · hourly</button><button id="ms" onclick="wfMode('stream')">Streaming · Kafka</button></div>
  <span class="note" id="mnote"></span>
</div>
<div class="legend">
  <span><i style="background:#0284c7"></i>Sources</span>
  <span><i style="background:#7c3aed"></i>Ingestion</span>
  <span><i style="background:#b45309"></i>Bronze</span>
  <span><i style="background:#475569"></i>Silver</span>
  <span><i style="background:#a16207"></i>Gold</span>
  <span><i style="background:#059669"></i>Consumption</span>
  <span><i class="dash"></i>dashed = optional (Kafka)</span>
  <span><span class="sdot" style="background:#f59e0b"></span>&nbsp;pending&nbsp;&nbsp;<span class="sdot" style="background:#94a3b8"></span>&nbsp;synthetic</span>
</div>
<div class="flow" id="flow"></div>
<div class="detail" id="detail"></div>
</div>
<script>
var NODES={
 dgf:{stage:'sources',title:'DGF · api.minenergia.cl',badge:'MAIN',bc:'b-main',st:'pending approval',sc:'#f59e0b',file:'scripts/dgf_poller.py',desc:'U. de Chile Geophysics (DGF) + MinEnergía API — hourly solar/wind. Pulled by the poller. Login works but the account is pending admin approval, so no real data flows yet.'},
 cen:{stage:'sources',title:'CEN generation feed',badge:'synthetic',bc:'b-syn',st:'synthetic',sc:'#94a3b8',file:'lookup CSVs',desc:'Coordinador generation per plant, hourly — coordinado / real / reducciones. Synthetic CSV today; a real source is out of card #56 scope.'},
 owid:{stage:'sources',title:'Our World in Data',st:'synthetic',sc:'#94a3b8',file:'lookup CSVs',desc:'Country renewable stats (Entity / Year) — hydro, solar, wind, capacity, share of renewables. Loaded from sample CSVs.'},
 poller:{stage:'ingest',title:'dgf_poller.py',badge:'hourly Job',bc:'b-job',file:'scripts/dgf_poller.py',desc:'Django login (CSRF double-submit, no reCAPTCHA), polls /api/, lands files. Runs as a Databricks Job hourly. --self-test passes against the live site today.'},
 volume:{stage:'ingest',title:'UC Volume landing',badge:'recommended',bc:'b-rec',file:'resources/volume.yml',desc:'/Volumes/<catalog>/<bronze_schema>/lookup/… landed JSON/CSV. Cheap, replayable, keeps the file_path lineage that silver relies on.'},
 kafka:{stage:'ingest',title:'Kafka topic',badge:'OPTIONAL',bc:'b-opt',opt:true,file:'dev: localhost:9092',desc:'Streaming message bus between poller and bronze. BYPASSED for hourly cadence (a broker = cost + ops). Switch on only for sub-minute / multi-consumer / replay-from-log.'},
 bronze:{stage:'bronze',title:'BRONZE',badge:'11 tables',bc:'b-syn',file:'transformations/bronze_*.py',desc:'Auto Loader (cloudFiles) ingests landed files → raw + _rescued_data + file_path lineage. 100% of columns documented via in-code schema COMMENTs.'},
 silver:{stage:'silver',title:'SILVER',file:'transformations/silver_*.py',desc:'resource → Data Vault hub / link / sat keyed on plant, truncated to hourly. conglomerate → dim_country + fact_*. ⚠ known PR #21 bug: coordinated SCD key collapses the coordinated series.'},
 gold:{stage:'gold',title:'GOLD',file:'transformations/gold_*.py',desc:'resource → daily / weekly / monthly measures + real-vs-coordinated diff. conglomerate → YoY increase + Chile-vs-LATAM/World comparisons.'},
 dash:{stage:'consume',title:'AI/BI dashboards',file:'Lakeview',desc:'Lakeview — Medallion Health + End-to-End (ingestion → counts → gold metrics → pipeline runs → data quality, all Santiago-time).'},
 sql:{stage:'consume',title:'SQL editor · notebooks',file:'notebooks/',desc:'Ad-hoc SQL + the sample_data_queries / pipeline_walkthrough / data_catalog notebooks.'}
};
var mode='batch';
function nodeHTML(id){var n=NODES[id];return '<div class="node '+n.stage+(n.opt?' opt':'')+'" data-id="'+id+'" onclick="wfSel(\''+id+'\')">'+
 '<div class="nt">'+(n.st?'<span class="sdot" title="'+n.st+'" style="background:'+n.sc+'"></span>':'')+n.title+(n.badge?' <span class="badge '+n.bc+'">'+n.badge+'</span>':'')+'</div><div class="nf">'+(n.file||'')+'</div></div>';}
function stage(label,inner){return '<div class="stage"><div class="sh">'+label+'</div><div class="col">'+inner+'</div></div>';}
function chev(){return '<div class="chev">&#9654;</div>';}
function dn(){return '<div class="dn">&#9660;</div>';}
var flow=
 stage('① Sources', nodeHTML('dgf')+nodeHTML('cen')+nodeHTML('owid'))+chev()+
 stage('② Ingestion', nodeHTML('poller')+dn()+nodeHTML('volume')+'<div class="orr">— or —</div>'+nodeHTML('kafka'))+chev()+
 stage('③ Medallion (DLT)', nodeHTML('bronze')+dn()+nodeHTML('silver')+dn()+nodeHTML('gold'))+chev()+
 stage('④ Consumption', nodeHTML('dash')+nodeHTML('sql'));
document.getElementById('flow').innerHTML=flow;
function wfSel(id){var n=NODES[id];var ns=document.querySelectorAll('#wf .node');for(var i=0;i<ns.length;i++){ns[i].classList.toggle('sel',ns[i].getAttribute('data-id')===id);}
 var st=n.st?'<div class="dst" style="color:'+n.sc+'">&#9679; status: '+n.st+'</div>':'';
 document.getElementById('detail').innerHTML='<div class="dh"><b>'+n.title+'</b>'+(n.badge?' <span class="badge '+n.bc+'">'+n.badge+'</span>':'')+' <code>'+(n.file||'')+'</code></div>'+st+'<div class="dd">'+n.desc+'</div>';}
function wfMode(m){mode=m;document.getElementById('mb').classList.toggle('active',m==='batch');document.getElementById('ms').classList.toggle('active',m==='stream');
 document.querySelector('#wf .node[data-id=kafka]').classList.toggle('dim',m==='batch');
 document.querySelector('#wf .node[data-id=volume]').classList.toggle('dim',m==='stream');
 document.getElementById('mnote').textContent=(m==='batch')?'Poller → UC Volume → Auto Loader. No broker, replayable, cheapest.':'Poller → Kafka topic → read natively by bronze. Lowest latency, always-on.';}
wfMode('batch');wfSel('kafka');
</script>
"""
displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where Kafka fits
# MAGIC Kafka is a **streaming message bus** that would sit **between the poller and bronze**.
# MAGIC The card #56 brief writes `DGF → Kafka → Databricks`, but the agreed cadence is **hourly**,
# MAGIC so the recommended design (`docs/card-56-bronze-landing-design.md` §3) **drops the broker**:
# MAGIC the poller lands JSON in a UC Volume and Auto Loader ingests it. Turn Kafka on only for
# MAGIC **sub-minute / multi-consumer / replay-from-log** needs.
# MAGIC
# MAGIC | Stage | What happens | Code |
# MAGIC |---|---|---|
# MAGIC | Source | DGF API (main), CEN generation (synthetic today), OWID CSV | `scripts/dgf_poller.py` |
# MAGIC | Ingestion | Hourly poll → land files in UC Volume (or Kafka, optional) | `dgf_poller.py`, `resources/volume.yml` |
# MAGIC | Bronze | Auto Loader reads landed files; raw + `_rescued_data` + `file_path` | `transformations/bronze_*.py` |
# MAGIC | Silver | resource: DV hub/link/sat (hourly); conglomerate: dim/fact | `transformations/silver_*.py` |
# MAGIC | Gold | daily/weekly/monthly + diff; YoY increase + comparisons | `transformations/gold_*.py` |
# MAGIC | Consumption | AI/BI dashboards, SQL editor, notebooks | `notebooks/`, Lakeview |
# MAGIC
# MAGIC > A GitHub-rendered **Mermaid** version (for PRs / Trello links) lives in
# MAGIC > `docs/end-to-end-data-workflow.md`; a standalone interactive page in `docs/end-to-end-workflow.html`.
