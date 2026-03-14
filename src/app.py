"""
Web GUI for Transaction Scheduler Simulator
Run: python app.py  →  open http://localhost:5000
Requires: pip install flask
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from scheduler import analyze, AnalysisReport
from explainer import build_trace
from locking import simulate_2pl
from generator import (
    GeneratorConfig, generate_random, generate_serial,
    generate_serializable, generate_non_serializable, schedule_to_text,
)

try:
    from flask import Flask, request, jsonify, render_template_string
except ImportError:
    print("Flask not installed. Run: pip install flask")
    sys.exit(1)

app = Flask(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# HTML TEMPLATE  (single-file, no external deps except CDN)
# ─────────────────────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Transaction Scheduler Simulator</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
:root{
  --bg:#0d1117;--surface:#161b22;--panel:#1c2230;--border:#30363d;
  --accent:#6c63ff;--accent2:#7c73ff;
  --green:#3fb950;--red:#f85149;--yellow:#d29922;--blue:#58a6ff;
  --text:#e6edf3;--muted:#8b949e;--subtle:#484f58;
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:14px;display:flex;flex-direction:column;height:100vh;overflow:hidden}

/* HEADER */
.app-header{background:var(--surface);border-bottom:1px solid var(--border);padding:12px 20px;display:flex;align-items:center;gap:14px;flex-shrink:0}
.logo{width:34px;height:34px;background:var(--accent);border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:15px;font-weight:800;color:#fff;flex-shrink:0;letter-spacing:-1px}
.hdr-text h1{font-size:16px;font-weight:700;color:var(--text)}
.hdr-text p{font-size:11px;color:var(--muted);margin-top:1px}
.hdr-pills{margin-left:auto;display:flex;gap:5px;flex-wrap:wrap}
.pill{font-size:10px;font-weight:700;padding:2px 8px;border-radius:12px;border:1px solid var(--border);color:var(--muted);white-space:nowrap}
.pill.req{border-color:var(--accent);color:var(--accent)}
.pill.bon{border-color:var(--subtle);color:var(--subtle)}

/* LAYOUT */
.app-body{display:grid;grid-template-columns:380px 1fr;flex:1;overflow:hidden}

/* SIDEBAR */
.sidebar{background:var(--surface);border-right:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden}
.sidebar-scroll{padding:14px;overflow-y:auto;flex:1}
.sec-label{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:var(--muted);margin-bottom:8px;padding-bottom:5px;border-bottom:1px solid var(--border)}
.card{background:var(--panel);border:1px solid var(--border);border-radius:8px;padding:12px;margin-bottom:10px}
textarea{width:100%;height:190px;background:var(--bg);border:1px solid var(--border);border-radius:6px;color:var(--text);font-family:'JetBrains Mono','Fira Code','Courier New',monospace;font-size:12.5px;padding:10px;resize:vertical;line-height:1.6}
textarea:focus{outline:none;border-color:var(--accent)}
.hint{font-size:10px;color:var(--subtle);margin-top:5px}
.btn-row{display:flex;gap:6px;margin-top:8px;flex-wrap:wrap}
button{padding:6px 14px;border:none;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600;transition:background .15s,color .15s}
.btn-p{background:var(--accent);color:#fff}.btn-p:hover{background:var(--accent2)}
.btn-g{background:transparent;border:1px solid var(--border);color:var(--muted)}.btn-g:hover{border-color:var(--accent);color:var(--accent)}
select{background:var(--bg);border:1px solid var(--border);color:var(--text);border-radius:6px;padding:5px 8px;font-size:12px;width:100%;margin-top:6px}
.err{background:#1a0a0a;border:1px solid var(--red);border-radius:6px;padding:8px 10px;color:var(--red);font-size:12px;margin-top:8px;display:none}
.ops-table{width:100%;border-collapse:collapse;font-size:12px}
.ops-table th{background:var(--bg);padding:4px 8px;text-align:left;color:var(--muted);border-bottom:1px solid var(--border);font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.4px}
.ops-table td{padding:4px 8px;border-bottom:1px solid #1a2030}
.r{color:#60a5fa}.w{color:#f87171}.c{color:#a78bfa}

/* MAIN AREA */
.main{display:flex;flex-direction:column;overflow:hidden}

/* RESULT STRIP */
.strip{display:flex;background:var(--surface);border-bottom:1px solid var(--border);overflow-x:auto;flex-shrink:0}
.sc{flex:1;min-width:100px;padding:8px 10px;border-right:1px solid var(--border);text-align:center;position:relative}
.sc:last-child{border-right:none}
.sc-label{font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:var(--muted);white-space:nowrap}
.sc-val{font-size:15px;font-weight:800;margin-top:2px}
.sc-sub{font-size:9px;color:var(--muted);margin-top:1px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.sc-val.yes{color:var(--green)}.sc-val.no{color:var(--red)}.sc-val.nd{color:var(--subtle)}
.sc.bonus-sc{opacity:.75}
.sc.bonus-sc .sc-label::after{content:' ★';color:var(--yellow)}
.sc-divider{width:1px;background:var(--subtle);align-self:stretch;margin:6px 0}

/* TABS */
.tabs{display:flex;background:var(--surface);border-bottom:1px solid var(--border);flex-shrink:0}
.tab{padding:9px 16px;font-size:12px;font-weight:600;color:var(--muted);border:none;background:none;cursor:pointer;border-bottom:2px solid transparent;transition:color .15s}
.tab:hover{color:var(--text)}.tab.active{color:var(--accent);border-bottom-color:var(--accent)}
.tab.btab{color:var(--subtle)}.tab.btab.active{color:var(--yellow);border-bottom-color:var(--yellow)}

/* TAB PANELS */
.panels{flex:1;overflow-y:auto}
.panel{display:none;padding:16px}.panel.active{display:block}

/* GRAPH */
#gc{width:100%;height:240px;background:var(--bg);border-radius:8px;border:1px solid var(--border);position:relative;overflow:hidden;margin-bottom:8px}
svg{width:100%;height:100%}
.node circle{stroke-width:2px}.node text{font-size:13px;font-weight:700;fill:#fff;pointer-events:none}

/* GRAPH LEGEND */
.legend{display:flex;gap:14px;flex-wrap:wrap;font-size:11px;color:var(--muted);margin-bottom:12px}
.li{display:flex;align-items:center;gap:4px}
.ld{width:11px;height:11px;border-radius:50%;border:2px solid;flex-shrink:0}
.ll{width:20px;height:2px;flex-shrink:0}

/* INFO BANNER */
.info{background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:10px 12px;font-size:12px;color:var(--muted);margin-bottom:12px;line-height:1.6}
.info strong{color:var(--text)}

/* VERDICT BANNERS */
.verdict{border-radius:6px;padding:10px 14px;font-size:12px;font-weight:600;margin-bottom:10px}
.verdict.ok{background:#0d2116;border:1px solid var(--green);color:var(--green)}
.verdict.bad{background:#1a0a0a;border:1px solid var(--red);color:var(--red)}
.verdict.warn{background:#1a160a;border:1px solid var(--yellow);color:var(--yellow)}

/* EDGE TABLE */
.etable{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:12px}
.etable th{background:var(--bg);padding:5px 8px;text-align:left;color:var(--muted);border-bottom:1px solid var(--border);font-size:11px;font-weight:700;text-transform:uppercase}
.etable td{padding:5px 8px;border-bottom:1px solid #1a2030}
.ww{color:#f97316}.wr{color:#60a5fa}.rw{color:#c084fc}
.chip{display:inline-block;padding:3px 10px;border-radius:16px;font-size:11px;font-weight:700;font-family:monospace;margin:2px}
.chip-ok{background:#0d2116;border:1px solid var(--green);color:var(--green)}
.chip-bad{background:#1a0a0a;border:1px solid var(--red);color:var(--red)}

/* PROPERTY CARDS */
.pcard{border:1px solid var(--border);border-radius:8px;padding:12px;margin-bottom:8px;background:var(--bg)}
.pcard.pok{border-color:#1a3a28}.pcard.pbad{border-color:#3a1a1a}
.pcard-hdr{display:flex;align-items:center;gap:8px;margin-bottom:5px}
.pcard-name{font-weight:700;font-size:13px}
.pcard-badge{font-size:10px;font-weight:700;padding:2px 8px;border-radius:10px;margin-left:auto;white-space:nowrap}
.pb-ok{background:#0d2116;color:var(--green)}.pb-bad{background:#1a0a0a;color:var(--red)}
.pcard-def{font-size:11px;color:var(--muted);font-style:italic;margin-bottom:6px;line-height:1.5}
.pcard-body{font-size:12px;line-height:1.6}
.pcard-body.ok-body{color:var(--green)}.pcard-body.bad-body{color:var(--red)}

/* HIERARCHY */
.hier{display:flex;align-items:center;flex-wrap:wrap;gap:0;margin:12px 0 16px}
.hc{padding:6px 12px;border-radius:6px;border:1px solid var(--border);font-size:12px;font-weight:700;background:var(--bg);color:var(--subtle);transition:all .3s}
.hc.hok{background:#0d2116;border-color:var(--green);color:var(--green)}
.hc.hbad{background:#1a0a0a;border-color:var(--red);color:var(--red)}
.harrow{padding:0 6px;color:var(--subtle);font-size:13px}

/* WAITING */
.waiting{color:var(--subtle);font-size:13px;text-align:center;padding:40px}
</style>
</head>
<body>

<!-- ── HEADER ── -->
<header class="app-header">
  <div class="logo">TS</div>
  <div class="hdr-text">
    <h1>Transaction Scheduler Simulator</h1>
    <p>Concurrency Control &amp; Schedule Correctness Analyzer &nbsp;·&nbsp; Ctrl+Enter to analyze</p>
  </div>
  <div class="hdr-pills">
    <span class="pill req">Conflict-Serializable</span>
    <span class="pill req">Recoverable</span>
    <span class="pill req">ACA</span>
    <span class="pill req">Strict</span>
    <span class="pill req">Rigorous</span>
    <span class="pill bon">★ View-Serializable</span>
    <span class="pill bon">★ 2PL</span>
    <span class="pill bon">★ Trace</span>
  </div>
</header>

<!-- ── BODY ── -->
<div class="app-body">

  <!-- LEFT SIDEBAR -->
  <div class="sidebar">
    <div class="sidebar-scroll">

      <div class="card">
        <div class="sec-label">Schedule Input</div>
        <textarea id="sched" spellcheck="false"
          placeholder="START(T1)&#10;START(T2)&#10;READ(T1,A)&#10;READ(T2,B)&#10;WRITE(T2,A)&#10;WRITE(T1,B)&#10;COMMIT(T1)&#10;COMMIT(T2)"></textarea>
        <div class="hint">Operations: START, COMMIT, ABORT, READ, WRITE, INCREMENT, DECREMENT</div>
        <div class="btn-row">
          <button class="btn-p" onclick="runAnalysis()">&#9654; Analyze</button>
          <button class="btn-g" onclick="clearAll()">Clear</button>
        </div>
        <select id="demo-select" onchange="loadSelected()">
          <option value="">— Load a Demo Schedule —</option>
          <option value="serial">1. Serial Schedule (T1 then T2)</option>
          <option value="non_serial">2. Non-Serializable (Cycle T1→T2→T1)</option>
          <option value="dirty_read">3. Recoverable but not ACA (Dirty Read)</option>
          <option value="non_recoverable">4. Non-Recoverable (T2 commits first)</option>
          <option value="strict">5. Strict but not Rigorous</option>
          <option value="inc_dec">6. INCREMENT / DECREMENT as Writes</option>
          <option value="blind_write">7. ★ Blind Write — View-serial, not Conflict-serial</option>
        </select>
        <div id="err" class="err"></div>
      </div>

      <div class="card">
        <div class="sec-label">Schedule Steps</div>
        <table class="ops-table">
          <thead><tr><th>#</th><th>Tx</th><th>Operation</th><th>Item</th></tr></thead>
          <tbody id="ops-body">
            <tr><td colspan="4" style="color:var(--subtle);padding:10px 8px;font-size:12px">No schedule yet.</td></tr>
          </tbody>
        </table>
      </div>

    </div>
  </div>

  <!-- RIGHT MAIN -->
  <div class="main">

    <!-- RESULT STRIP -->
    <div class="strip">
      <div class="sc" title="A schedule is conflict-serializable if its precedence graph contains no cycle.">
        <div class="sc-label">Conflict-Serial.</div>
        <div class="sc-val nd" id="r-ser">—</div>
        <div class="sc-sub" id="r-orders-sub">—</div>
      </div>
      <div class="sc" title="A transaction commits only after all transactions whose dirty data it read have committed.">
        <div class="sc-label">Recoverable</div>
        <div class="sc-val nd" id="r-rec">—</div>
      </div>
      <div class="sc" title="ACA: transactions read only data written by already-committed transactions.">
        <div class="sc-label">ACA</div>
        <div class="sc-val nd" id="r-aca">—</div>
      </div>
      <div class="sc" title="Strict: no R/W on item X until the last writer of X has committed or aborted.">
        <div class="sc-label">Strict</div>
        <div class="sc-val nd" id="r-strict">—</div>
      </div>
      <div class="sc" title="Rigorous: no R/W on item X until the last accessor (reader or writer) of X has committed or aborted.">
        <div class="sc-label">Rigorous</div>
        <div class="sc-val nd" id="r-rig">—</div>
      </div>
      <div class="sc bonus-sc" title="Bonus: view-serializable if view-equivalent to some serial schedule. Strictly broader than conflict serializability.">
        <div class="sc-label">View-Serial.</div>
        <div class="sc-val nd" id="r-vs">—</div>
        <div class="sc-sub" id="r-vs-sub">—</div>
      </div>
    </div>

    <!-- TABS -->
    <div class="tabs">
      <button class="tab active" onclick="switchTab(this,'conflict')">Conflict Analysis</button>
      <button class="tab" onclick="switchTab(this,'recov')">Recoverability</button>
      <button class="tab btab" onclick="switchTab(this,'view')">★ View Serializability</button>
    </div>

    <!-- TAB PANELS -->
    <div class="panels">

      <!-- ── CONFLICT ANALYSIS ── -->
      <div class="panel active" id="panel-conflict">
        <div class="info">
          <strong>Conflict Equivalence (Required Analysis)</strong> — Two operations conflict if they belong to
          different transactions, access the same item, and at least one is a write. A schedule is
          <strong>conflict-serializable</strong> if its precedence (serialization) graph is acyclic.
        </div>

        <div id="gc">
          <svg id="graph-svg">
            <defs>
              <marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
                <path d="M0,0 L0,6 L8,3 z" fill="#6c63ff"/>
              </marker>
              <marker id="arrow-red" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
                <path d="M0,0 L0,6 L8,3 z" fill="#f85149"/>
              </marker>
            </defs>
            <g id="graph-g"></g>
          </svg>
          <div id="graph-empty" style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);color:var(--subtle);font-size:12px;text-align:center;pointer-events:none">
            Precedence graph will appear here
          </div>
        </div>

        <div class="legend">
          <div class="li"><div class="ld" style="border-color:#6c63ff;background:#1e2140"></div>Transaction</div>
          <div class="li"><div class="ld" style="border-color:#f85149;background:#2d1117"></div>In cycle</div>
          <div class="li"><div class="ll" style="background:#6c63ff"></div>Conflict edge</div>
          <div class="li"><div class="ll" style="background:#f85149"></div>Cycle edge</div>
          <div class="li"><span class="ww" style="font-weight:700">WW</span>&nbsp;Write-Write</div>
          <div class="li"><span class="wr" style="font-weight:700">WR</span>&nbsp;Write-Read</div>
          <div class="li"><span class="rw" style="font-weight:700">RW</span>&nbsp;Read-Write</div>
        </div>

        <div id="conflict-detail"><div class="waiting">Run an analysis to see conflict details.</div></div>
      </div>

      <!-- ── RECOVERABILITY ── -->
      <div class="panel" id="panel-recov">
        <div class="info">
          The four properties form a strict containment hierarchy — satisfying a stronger property
          automatically satisfies all weaker ones:<br>
          <strong style="color:var(--green)">Rigorous &nbsp;⊂&nbsp; Strict &nbsp;⊂&nbsp; ACA &nbsp;⊂&nbsp; Recoverable</strong>
        </div>

        <div class="hier">
          <div class="hc" id="h-rig">Rigorous</div>
          <div class="harrow">⊂</div>
          <div class="hc" id="h-strict">Strict</div>
          <div class="harrow">⊂</div>
          <div class="hc" id="h-aca">ACA</div>
          <div class="harrow">⊂</div>
          <div class="hc" id="h-rec">Recoverable</div>
        </div>

        <div id="rec-detail"><div class="waiting">Run an analysis to see the recoverability breakdown.</div></div>
      </div>

      <!-- ── VIEW SERIALIZABILITY (BONUS) ── -->
      <div class="panel" id="panel-view">
        <div class="info">
          <strong>★ Bonus Feature — View Serializability</strong><br>
          Two schedules are <em>view-equivalent</em> if for every data item X they have the same:
          (1) initial reads, (2) reads-from relationships, and (3) final writes.
          A schedule is <strong>view-serializable</strong> if it is view-equivalent to some serial schedule.<br><br>
          View serializability is <strong>strictly broader</strong> than conflict serializability —
          the classic <em>blind-write</em> schedule is view-serializable but not conflict-serializable.<br><br>
          <strong>Note:</strong> The required serializability determination (Conflict Analysis tab) uses
          <em>only conflict equivalence</em>. This tab is an additional bonus analysis.
        </div>
        <div id="view-detail"><div class="waiting">Run an analysis to see the view serializability result.</div></div>
      </div>

    </div><!-- /panels -->
  </div><!-- /main -->
</div><!-- /app-body -->

<script>
const DEMOS = {
  serial:         `START(T1)\nSTART(T2)\nREAD(T1,A)\nWRITE(T1,A)\nCOMMIT(T1)\nREAD(T2,A)\nWRITE(T2,B)\nCOMMIT(T2)`,
  non_serial:     `START(T1)\nSTART(T2)\nREAD(T1,A)\nREAD(T2,B)\nWRITE(T2,A)\nWRITE(T1,B)\nCOMMIT(T1)\nCOMMIT(T2)`,
  dirty_read:     `START(T1)\nSTART(T2)\nWRITE(T1,A)\nREAD(T2,A)\nCOMMIT(T1)\nCOMMIT(T2)`,
  non_recoverable:`START(T1)\nSTART(T2)\nWRITE(T1,A)\nREAD(T2,A)\nCOMMIT(T2)\nCOMMIT(T1)`,
  strict:         `START(T1)\nSTART(T2)\nWRITE(T1,A)\nCOMMIT(T1)\nREAD(T2,A)\nWRITE(T2,A)\nCOMMIT(T2)`,
  inc_dec:        `START(T1)\nSTART(T2)\nREAD(T1,X)\nINCREMENT(T2,X)\nWRITE(T1,X)\nCOMMIT(T1)\nCOMMIT(T2)`,
  blind_write:    `START(T1)\nSTART(T2)\nSTART(T3)\nREAD(T1,A)\nWRITE(T2,A)\nWRITE(T1,A)\nWRITE(T3,A)\nCOMMIT(T1)\nCOMMIT(T2)\nCOMMIT(T3)`
};

function loadSelected(){ const v=document.getElementById('demo-select').value; if(v) document.getElementById('sched').value=DEMOS[v]; }
function clearAll(){ document.getElementById('sched').value=''; resetAll(); }

function switchTab(btn, name){
  document.querySelectorAll('.tab').forEach(b=>b.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('panel-'+name).classList.add('active');
}

function sv(id, val){
  const el=document.getElementById(id);
  el.textContent = val?'✓ YES':'✗ NO';
  el.className='sc-val '+(val?'yes':'no');
}

function resetAll(){
  ['r-ser','r-rec','r-aca','r-strict','r-rig','r-vs'].forEach(id=>{
    const el=document.getElementById(id); el.textContent='—'; el.className='sc-val nd';
  });
  document.getElementById('r-orders-sub').textContent='—';
  document.getElementById('r-vs-sub').textContent='—';
  document.getElementById('graph-g').innerHTML='';
  document.getElementById('graph-empty').style.display='block';
  document.getElementById('ops-body').innerHTML='<tr><td colspan="4" style="color:var(--subtle);padding:10px 8px;font-size:12px">No schedule yet.</td></tr>';
  document.getElementById('conflict-detail').innerHTML='<div class="waiting">Run an analysis to see conflict details.</div>';
  document.getElementById('rec-detail').innerHTML='<div class="waiting">Run an analysis to see the recoverability breakdown.</div>';
  document.getElementById('view-detail').innerHTML='<div class="waiting">Run an analysis to see the view serializability result.</div>';
  document.getElementById('err').style.display='none';
  ['h-rig','h-strict','h-aca','h-rec'].forEach(id=>{ document.getElementById(id).className='hc'; });
}

async function runAnalysis(){
  const text=document.getElementById('sched').value.trim();
  if(!text){ alert('Please enter a schedule.'); return; }
  document.getElementById('err').style.display='none';
  try{
    const resp=await fetch('/analyze',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({schedule:text})});
    const d=await resp.json();
    if(d.error){ const e=document.getElementById('err'); e.textContent='⚠ '+d.error; e.style.display='block'; return; }
    render(d);
  } catch(e){ const eb=document.getElementById('err'); eb.textContent='Network error: '+e.message; eb.style.display='block'; }
}

function render(d){
  sv('r-ser',   d.is_serializable);
  sv('r-rec',   d.is_recoverable);
  sv('r-aca',   d.is_aca);
  sv('r-strict',d.is_strict);
  sv('r-rig',   d.is_rigorous);
  sv('r-vs',    d.is_view_serializable);

  const sub=document.getElementById('r-orders-sub');
  if(d.is_serializable && d.serial_orders.length){ sub.textContent=d.serial_orders.map(o=>o.join('→')).join(' | '); sub.style.color='var(--green)'; }
  else{ sub.textContent=d.is_serializable?'':'cycle detected'; sub.style.color='var(--red)'; }

  const vsub=document.getElementById('r-vs-sub');
  if(d.is_view_serializable && d.view_serial_orders&&d.view_serial_orders.length){ vsub.textContent=d.view_serial_orders.map(o=>o.join('→')).join(' | '); vsub.style.color='var(--green)'; }
  else{ vsub.textContent=''; }

  // Ops table
  const tb=document.getElementById('ops-body'); tb.innerHTML='';
  for(const op of d.operations){
    const cls=op.is_write?'w':(op.is_read?'r':'c');
    tb.innerHTML+=`<tr class="${cls}"><td>${op.step}</td><td><strong>${op.tx}</strong></td><td>${op.op_type}</td><td>${op.item||''}</td></tr>`;
  }

  renderConflict(d);
  renderRecov(d);
  renderView(d);
  drawGraph(d.transactions,d.edges,d.cycles_flat,d.is_serializable);
}

function renderConflict(d){
  let h='';
  if(d.edges.length===0){
    h+=`<div class="verdict ok">No conflicting operation pairs — the precedence graph is empty.</div>`;
  } else {
    h+=`<div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:var(--muted);margin-bottom:6px">Precedence Graph Edges</div>`;
    h+=`<table class="etable"><thead><tr><th>From</th><th>To</th><th>Type</th><th>Item</th></tr></thead><tbody>`;
    for(const e of d.edges){
      const cls=e.reason==='WW'?'ww':(e.reason==='WR'?'wr':'rw');
      h+=`<tr><td><strong>${e.from_tx}</strong></td><td><strong>${e.to_tx}</strong></td><td class="${cls}"><strong>${e.reason}</strong></td><td><code>${e.item}</code></td></tr>`;
    }
    h+=`</tbody></table>`;
  }

  if(d.cycles&&d.cycles.length){
    h+=`<div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:var(--red);margin-bottom:6px">Cycles Detected</div>`;
    for(const c of d.cycles) h+=`<span class="chip chip-bad">${c.join(' → ')}</span>`;
    h+=`<div class="verdict bad" style="margin-top:10px">✗ NOT Conflict-Serializable — the precedence graph contains a cycle. No equivalent serial schedule exists via conflict equivalence.</div>`;
  } else {
    h+=`<div class="verdict ok">✓ No cycles in the precedence graph — schedule IS conflict-serializable.</div>`;
    if(d.serial_orders&&d.serial_orders.length){
      h+=`<div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:var(--muted);margin:8px 0 4px">Equivalent Serial Order(s)</div>`;
      for(const o of d.serial_orders) h+=`<span class="chip chip-ok">${o.join(' → ')}</span>`;
    }
  }
  document.getElementById('conflict-detail').innerHTML=h;
}

const PDEFS={
  recoverable:'A transaction commits only after every transaction whose dirty data it has read has committed.',
  aca:'Transactions read only data written by already-committed transactions (avoids cascading aborts).',
  strict:'No transaction reads or writes a data item until the transaction that last wrote it has committed or aborted.',
  rigorous:'No transaction reads or writes a data item until the last transaction that accessed (read or wrote) it has committed or aborted. Implies Strict.'
};

function renderRecov(d){
  const hier={
    'h-rec': d.is_recoverable,
    'h-aca': d.is_aca,
    'h-strict': d.is_strict,
    'h-rig': d.is_rigorous
  };
  for(const [id,val] of Object.entries(hier)) document.getElementById(id).className='hc '+(val?'hok':'hbad');

  const props=[
    {name:'Rigorous',    key:'rigorous',    sat:d.is_rigorous},
    {name:'Strict',      key:'strict',      sat:d.is_strict},
    {name:'ACA',         key:'aca',         sat:d.is_aca},
    {name:'Recoverable', key:'recoverable', sat:d.is_recoverable},
  ];

  // Extract per-property violations from rec_explanation
  const recLines = (d.rec_explanation||d.explanation||[]);
  const sections = {recoverable:[], aca:[], strict:[], rigorous:[]};
  let cur=null;
  for(const ln of recLines){
    if(ln.includes('Recoverability Check')) cur='recoverable';
    else if(ln.includes('ACA')) cur='aca';
    else if(ln.includes('Strict Schedule')) cur='strict';
    else if(ln.includes('Rigorous Schedule')) cur='rigorous';
    if(cur && ln.includes('VIOLATION')) sections[cur].push(ln.trim());
  }

  let h='';
  for(const p of props){
    const cls=p.sat?'pok':'pbad';
    const bc=p.sat?'pb-ok':'pb-bad';
    const bt=p.sat?'✓ SATISFIED':'✗ VIOLATED';
    const viols=sections[p.key]||[];
    h+=`<div class="pcard ${cls}">
      <div class="pcard-hdr"><div class="pcard-name">${p.name}</div><div class="pcard-badge ${bc}">${bt}</div></div>
      <div class="pcard-def">${PDEFS[p.key]}</div>
      <div class="pcard-body ${p.sat?'ok-body':'bad-body'}">`;
    if(p.sat){
      h+=`No violations detected.`;
    } else if(viols.length){
      for(const v of viols) h+=`<div>${v}</div>`;
    }
    h+=`</div></div>`;
  }
  document.getElementById('rec-detail').innerHTML=h;
}

function renderView(d){
  let h='';
  if(d.is_view_serializable){
    h+=`<div class="verdict ok">✓ View-Serializable — a view-equivalent serial schedule exists.</div>`;
    if(d.view_serial_orders&&d.view_serial_orders.length){
      h+=`<div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:var(--muted);margin:8px 0 4px">View-Equivalent Serial Order(s)</div>`;
      for(const o of d.view_serial_orders) h+=`<span class="chip chip-ok">${o.join(' → ')}</span>`;
    }
    if(!d.is_serializable){
      h+=`<div class="verdict warn" style="margin-top:10px">
        This schedule is <strong>view-serializable but NOT conflict-serializable</strong> —
        demonstrating that view serializability is strictly broader than conflict serializability
        (classic blind-write example).
      </div>`;
    }
  } else {
    h+=`<div class="verdict bad">✗ NOT View-Serializable — no serial schedule is view-equivalent to this schedule.</div>`;
  }

  const vlines=(d.view_explanation||[]).filter(l=>l.trim()&&!l.includes('==='));
  if(vlines.length){
    h+=`<div style="margin-top:12px;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:10px;font-size:11px;font-family:monospace;color:var(--muted);line-height:1.7">`;
    for(const l of vlines) h+=`<div>${l}</div>`;
    h+=`</div>`;
  }
  document.getElementById('view-detail').innerHTML=h;
}

function drawGraph(txns,edges,cycleNodes,isSer){
  const g=document.getElementById('graph-g');
  g.innerHTML='';
  document.getElementById('graph-empty').style.display='none';
  if(!txns||txns.length===0){ document.getElementById('graph-empty').style.display='block'; return; }

  const W=document.getElementById('gc').clientWidth;
  const H=document.getElementById('gc').clientHeight;
  const R=28;

  const pos={};
  txns.forEach((t,i)=>{
    const angle=(2*Math.PI*i/txns.length)-Math.PI/2;
    const rad=txns.length===1?0:Math.min(W,H)/2-R-22;
    pos[t]={x:W/2+rad*Math.cos(angle),y:H/2+rad*Math.sin(angle)};
  });

  const eg=document.createElementNS('http://www.w3.org/2000/svg','g');
  const seen={};
  for(const e of edges){
    const key=e.from_tx+'→'+e.to_tx;
    const inCyc=cycleNodes.includes(e.from_tx)&&cycleNodes.includes(e.to_tx)&&!isSer;
    const col=inCyc?'#f85149':'#6c63ff';
    const mEnd=inCyc?'url(#arrow-red)':'url(#arrow)';
    const p1=pos[e.from_tx],p2=pos[e.to_tx];
    const dx=p2.x-p1.x,dy=p2.y-p1.y,dist=Math.sqrt(dx*dx+dy*dy)||1;
    const x1=p1.x+dx/dist*R,y1=p1.y+dy/dist*R;
    const x2=p2.x-dx/dist*(R+6),y2=p2.y-dy/dist*(R+6);
    const rev=e.to_tx+'→'+e.from_tx;
    const off=seen[rev]?22:0;
    seen[key]=true;
    const mx=(x1+x2)/2-dy/dist*off,my=(y1+y2)/2+dx/dist*off;
    const path=document.createElementNS('http://www.w3.org/2000/svg','path');
    path.setAttribute('d',`M${x1},${y1} Q${mx},${my} ${x2},${y2}`);
    path.setAttribute('fill','none');path.setAttribute('stroke',col);
    path.setAttribute('stroke-width','2');path.setAttribute('marker-end',mEnd);
    eg.appendChild(path);
    const lbl=document.createElementNS('http://www.w3.org/2000/svg','text');
    lbl.setAttribute('x',mx);lbl.setAttribute('y',my-5);
    lbl.setAttribute('text-anchor','middle');lbl.setAttribute('fill','#8b949e');lbl.setAttribute('font-size','10');
    lbl.textContent=`${e.reason}(${e.item})`;
    eg.appendChild(lbl);
  }
  g.appendChild(eg);

  for(const t of txns){
    const {x,y}=pos[t];
    const grp=document.createElementNS('http://www.w3.org/2000/svg','g');
    grp.setAttribute('class','node');
    const inCyc=cycleNodes.includes(t)&&!isSer;
    const circ=document.createElementNS('http://www.w3.org/2000/svg','circle');
    circ.setAttribute('cx',x);circ.setAttribute('cy',y);circ.setAttribute('r',R);
    circ.setAttribute('fill',inCyc?'#2d1117':'#1e2140');
    circ.setAttribute('stroke',inCyc?'#f85149':'#6c63ff');
    circ.setAttribute('stroke-width','2');
    grp.appendChild(circ);
    const txt=document.createElementNS('http://www.w3.org/2000/svg','text');
    txt.setAttribute('x',x);txt.setAttribute('y',y+5);
    txt.setAttribute('text-anchor','middle');
    txt.setAttribute('fill',inCyc?'#f85149':'#e6edf3');
    txt.setAttribute('font-size','13');txt.setAttribute('font-weight','700');
    txt.textContent=t;
    grp.appendChild(txt);
    g.appendChild(grp);
  }
}

document.addEventListener('keydown',e=>{ if((e.ctrlKey||e.metaKey)&&e.key==='Enter') runAnalysis(); });
</script>
</body>
</html>
"""


# ─────────────────────────────────────────────────────────────────────────────
# API ENDPOINT
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/analyze", methods=["POST"])
def api_analyze():
    data = request.get_json(force=True)
    schedule_text = data.get("schedule", "")
    try:
        report = analyze(schedule_text)
        ser  = report.serializability
        rec  = report.recoverability
        vser = report.view_serializability
        s    = report.schedule

        # Flatten cycle nodes for client-side highlighting
        cycle_flat = list({t for cyc in ser.cycles for t in cyc})

        ops_out = []
        for op in s.operations:
            ops_out.append({
                "step":     op.step,
                "tx":       op.tx,
                "op_type":  op.op_type.name,
                "item":     op.item,
                "is_read":  op.is_read(),
                "is_write": op.is_write(),
            })

        edges_out = [
            {"from_tx": e.from_tx, "to_tx": e.to_tx,
             "item": e.item, "reason": e.reason}
            for e in ser.edges
        ]

        return jsonify({
            "is_serializable":      ser.is_serializable,
            "serial_orders":        ser.serial_orders,
            "cycles":               ser.cycles,
            "cycles_flat":          cycle_flat,
            "is_recoverable":       rec.is_recoverable,
            "is_aca":               rec.is_aca,
            "is_strict":            rec.is_strict,
            "is_rigorous":          rec.is_rigorous,
            "is_view_serializable": vser.is_view_serializable,
            "view_serial_orders":   vser.equivalent_serial_orders,
            "transactions":         s.transactions,
            "operations":           ops_out,
            "edges":                edges_out,
            # per-section explanations for structured frontend rendering
            "ser_explanation":      ser.explanation,
            "rec_explanation":      rec.explanation,
            "view_explanation":     vser.explanation,
            # combined for backward-compat
            "explanation":          ser.explanation + [""] + rec.explanation,
        })

    except ValueError as e:
        return jsonify({"error": str(e)}), 400


if __name__ == "__main__":
    print("Starting Transaction Scheduler Simulator...")
    print("Open http://localhost:5000 in your browser")
    app.run(debug=True, port=5000)

# ── BONUS endpoints added below ──────────────────────────────────────────────

@app.route("/trace", methods=["POST"])
def api_trace():
    data = request.get_json(force=True)
    try:
        from scheduler import parse_schedule
        s = parse_schedule(data.get("schedule", ""))
        trace = build_trace(s)
        events_out = [
            {"step": e.step, "op": e.op, "category": e.category, "message": e.message}
            for e in trace.events
        ]
        return jsonify({"events": events_out, "summary": trace.summary})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/locking", methods=["POST"])
def api_locking():
    data = request.get_json(force=True)
    try:
        from scheduler import parse_schedule
        s = parse_schedule(data.get("schedule", ""))
        result = simulate_2pl(s)
        actions_out = [
            {"step": a.step, "tx": a.tx, "item": a.item or "",
             "action": a.action, "message": a.message,
             "lock_type": a.lock_type.value if a.lock_type else ""}
            for a in result.lock_actions
        ]
        return jsonify({
            "is_2pl":        result.is_2pl,
            "is_strict_2pl": result.is_strict_2pl,
            "violations":    result.violations,
            "explanation":   result.explanation,
            "actions":       actions_out,
        })
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/generate", methods=["POST"])
def api_generate():
    data  = request.get_json(force=True)
    gtype = data.get("type", "random")
    cfg   = GeneratorConfig(
        num_transactions=int(data.get("txns",  2)),
        num_items=        int(data.get("items", 2)),
        ops_per_tx=       int(data.get("ops",   3)),
        seed=             data.get("seed"),
    )
    fn_map = {
        "random":           generate_random,
        "serial":           generate_serial,
        "serializable":     generate_serializable,
        "non_serializable": generate_non_serializable,
    }
    try:
        schedule  = fn_map.get(gtype, generate_random)(cfg)
        text      = schedule_to_text(schedule)
        return jsonify({"schedule": text})
    except Exception as e:
        return jsonify({"error": str(e)}), 400
