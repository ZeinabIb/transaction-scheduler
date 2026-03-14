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
  :root{--bg:#0f1117;--panel:#1a1d27;--border:#2d3149;--accent:#6c63ff;
        --green:#22c55e;--red:#ef4444;--yellow:#f59e0b;--text:#e2e8f0;--muted:#94a3b8}
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:'Segoe UI',sans-serif;font-size:14px;padding:16px}
  h1{text-align:center;font-size:22px;color:var(--accent);margin-bottom:4px}
  .subtitle{text-align:center;color:var(--muted);margin-bottom:20px;font-size:13px}
  .grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;max-width:1400px;margin:0 auto}
  @media(max-width:900px){.grid{grid-template-columns:1fr}}
  .card{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:16px}
  label{display:block;font-weight:600;margin-bottom:6px;color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.5px}
  textarea{width:100%;height:230px;background:#12141f;border:1px solid var(--border);border-radius:6px;
           color:var(--text);font-family:'Courier New',monospace;font-size:13px;padding:10px;resize:vertical}
  textarea:focus{outline:none;border-color:var(--accent)}
  .btn-row{display:flex;gap:8px;margin-top:10px}
  button{padding:8px 18px;border:none;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600;transition:.15s}
  .btn-primary{background:var(--accent);color:#fff}
  .btn-primary:hover{background:#7c73ff}
  .btn-ghost{background:transparent;border:1px solid var(--border);color:var(--muted)}
  .btn-ghost:hover{border-color:var(--accent);color:var(--accent)}
  .badge{display:inline-block;padding:3px 10px;border-radius:20px;font-size:12px;font-weight:700;margin:2px}
  .yes{background:#14532d;color:var(--green)} .no{background:#450a0a;color:var(--red)}
  .result-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin:10px 0}
  .prop{background:#12141f;border-radius:6px;padding:10px 12px;border:1px solid var(--border)}
  .prop .name{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.4px}
  .prop .val{font-size:15px;font-weight:700;margin-top:4px}
  .green{color:var(--green)} .red{color:var(--red)}
  pre{background:#12141f;border-radius:6px;padding:12px;font-size:12px;overflow-x:auto;
      border:1px solid var(--border);white-space:pre-wrap;max-height:340px;overflow-y:auto}
  #graph-container{width:100%;height:320px;background:#12141f;border-radius:8px;
                   border:1px solid var(--border);position:relative;overflow:hidden}
  svg{width:100%;height:100%}
  .node circle{stroke-width:2px}
  .node text{font-size:13px;font-weight:700;fill:#fff;pointer-events:none}
  .link{stroke-width:2px;marker-end:url(#arrow)}
  .link-label{font-size:10px;fill:var(--muted)}
  .error-msg{background:#450a0a;border:1px solid var(--red);border-radius:6px;padding:10px;color:var(--red);margin-top:8px;font-size:13px}
  .section-title{font-size:13px;font-weight:700;color:var(--accent);margin:14px 0 6px;text-transform:uppercase;letter-spacing:.4px}
  select{background:#12141f;border:1px solid var(--border);color:var(--text);border-radius:6px;padding:6px 10px;font-size:13px}
  .ops-table{width:100%;border-collapse:collapse;font-size:12px}
  .ops-table th{background:#12141f;padding:6px 10px;text-align:left;color:var(--muted);border-bottom:1px solid var(--border)}
  .ops-table td{padding:5px 10px;border-bottom:1px solid #1e2136}
  .op-read{color:#60a5fa} .op-write{color:#f87171} .op-ctrl{color:#a78bfa}
</style>
</head>
<body>
<h1>⚡ Transaction Scheduler Simulator</h1>
<p class="subtitle">Conflict Serializability · View Serializability · Recoverability · ACA · Strict · Rigorous</p>

<div class="grid">

  <!-- LEFT PANEL: Input -->
  <div>
    <div class="card">
      <label>Schedule Input</label>
      <textarea id="sched" placeholder="START(T1)&#10;START(T2)&#10;READ(T1,A)&#10;READ(T2,B)&#10;WRITE(T2,A)&#10;WRITE(T1,B)&#10;COMMIT(T1)&#10;COMMIT(T2)"></textarea>
      <div class="btn-row">
        <button class="btn-primary" onclick="runAnalysis()">▶ Analyze</button>
        <button class="btn-ghost" onclick="loadDemo()">Load Demo</button>
        <button class="btn-ghost" onclick="clearAll()">Clear</button>
        <select id="demo-select" onchange="loadSelected()">
          <option value="">— Demo Schedules —</option>
          <option value="serial">Serial (T1→T2)</option>
          <option value="non_serial">Non-Serializable (Cycle)</option>
          <option value="dirty_read">Dirty Read (not ACA)</option>
          <option value="non_recoverable">Non-Recoverable</option>
          <option value="strict">Strict Schedule</option>
          <option value="inc_dec">INCREMENT/DECREMENT</option>
          <option value="blind_write">★ Blind Write (View-serial, Not Conflict-serial)</option>
        </select>
      </div>
      <div id="error-box" style="display:none" class="error-msg"></div>
    </div>

    <!-- Operations table -->
    <div class="card" style="margin-top:12px" id="ops-card">
      <div class="section-title">📋 Schedule Steps</div>
      <table class="ops-table" id="ops-table">
        <thead><tr><th>#</th><th>Tx</th><th>Operation</th><th>Item</th></tr></thead>
        <tbody id="ops-body"></tbody>
      </table>
    </div>
  </div>

  <!-- RIGHT PANEL: Results -->
  <div>
    <div class="card" id="results-card">
      <div class="section-title">📊 Results</div>
      <div class="result-grid">
        <div class="prop"><div class="name">Conflict-Serializable</div><div class="val" id="r-serial">—</div></div>
        <div class="prop"><div class="name">View-Serializable ★</div><div class="val" id="r-view-serial">—</div></div>
        <div class="prop"><div class="name">Recoverable</div><div class="val" id="r-rec">—</div></div>
        <div class="prop"><div class="name">ACA</div><div class="val" id="r-aca">—</div></div>
        <div class="prop"><div class="name">Strict</div><div class="val" id="r-strict">—</div></div>
        <div class="prop"><div class="name">Rigorous</div><div class="val" id="r-rigorous">—</div></div>
        <div class="prop"><div class="name">Conflict Serial Order(s)</div><div class="val" id="r-orders" style="font-size:12px">—</div></div>
        <div class="prop"><div class="name">View Serial Order(s) ★</div><div class="val" id="r-view-orders" style="font-size:12px">—</div></div>
      </div>
      <div style="font-size:11px;color:var(--muted);margin-bottom:8px">★ Bonus analysis only — serializability is determined exclusively by conflict equivalence (precedence graph).</div>

      <!-- Precedence graph -->
      <div class="section-title">🔗 Precedence Graph</div>
      <div id="graph-container">
        <svg id="graph-svg">
          <defs>
            <marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
              <path d="M0,0 L0,6 L8,3 z" fill="#6c63ff"/>
            </marker>
            <marker id="arrow-red" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
              <path d="M0,0 L0,6 L8,3 z" fill="#ef4444"/>
            </marker>
          </defs>
          <g id="graph-g"></g>
        </svg>
        <div id="graph-empty" style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);
             color:var(--muted);font-size:13px;text-align:center">
          Run an analysis to see the graph
        </div>
      </div>

      <!-- Detailed log -->
      <div class="section-title">📝 Detailed Explanation</div>
      <pre id="detail-log">Run an analysis to see details.</pre>
    </div>
  </div>

</div>

<script>
const DEMOS = {
  serial:`START(T1)\nSTART(T2)\nREAD(T1,A)\nWRITE(T1,A)\nCOMMIT(T1)\nREAD(T2,A)\nWRITE(T2,B)\nCOMMIT(T2)`,
  non_serial:`START(T1)\nSTART(T2)\nREAD(T1,A)\nREAD(T2,B)\nWRITE(T2,A)\nWRITE(T1,B)\nCOMMIT(T1)\nCOMMIT(T2)`,
  dirty_read:`START(T1)\nSTART(T2)\nWRITE(T1,A)\nREAD(T2,A)\nCOMMIT(T1)\nCOMMIT(T2)`,
  non_recoverable:`START(T1)\nSTART(T2)\nWRITE(T1,A)\nREAD(T2,A)\nCOMMIT(T2)\nCOMMIT(T1)`,
  strict:`START(T1)\nSTART(T2)\nWRITE(T1,A)\nCOMMIT(T1)\nREAD(T2,A)\nWRITE(T2,A)\nCOMMIT(T2)`,
  inc_dec:`START(T1)\nSTART(T2)\nREAD(T1,X)\nINCREMENT(T2,X)\nWRITE(T1,X)\nCOMMIT(T1)\nCOMMIT(T2)`,
  blind_write:`START(T1)\nSTART(T2)\nSTART(T3)\nREAD(T1,A)\nWRITE(T2,A)\nWRITE(T1,A)\nWRITE(T3,A)\nCOMMIT(T1)\nCOMMIT(T2)\nCOMMIT(T3)`
};

function loadDemo(){ document.getElementById('sched').value = DEMOS.non_serial; }
function clearAll(){ document.getElementById('sched').value=''; clearResults(); }
function loadSelected(){
  const v = document.getElementById('demo-select').value;
  if(v) document.getElementById('sched').value = DEMOS[v];
}

function setVal(id, val, isGood){
  const el = document.getElementById(id);
  el.textContent = val ? '✓  YES' : '✗  NO';
  el.className = 'val ' + (val ? 'green' : 'red');
}

function clearResults(){
  ['r-serial','r-view-serial','r-rec','r-aca','r-strict','r-rigorous'].forEach(id=>{
    const el=document.getElementById(id);
    el.textContent='—'; el.className='val';
  });
  document.getElementById('r-orders').textContent='—';
  document.getElementById('r-view-orders').textContent='—';
  document.getElementById('detail-log').textContent='Run an analysis to see details.';
  document.getElementById('graph-g').innerHTML='';
  document.getElementById('graph-empty').style.display='block';
  document.getElementById('error-box').style.display='none';
  document.getElementById('ops-body').innerHTML='';
}

async function runAnalysis(){
  const text = document.getElementById('sched').value.trim();
  if(!text){ alert('Please enter a schedule.'); return; }
  document.getElementById('error-box').style.display='none';

  try{
    const resp = await fetch('/analyze',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({schedule: text})
    });
    const data = await resp.json();
    if(data.error){
      document.getElementById('error-box').style.display='block';
      document.getElementById('error-box').textContent = '⚠ ' + data.error;
      return;
    }
    renderResults(data);
  } catch(e){
    document.getElementById('error-box').style.display='block';
    document.getElementById('error-box').textContent = 'Network error: ' + e.message;
  }
}

function renderResults(d){
  setVal('r-serial',      d.is_serializable);
  setVal('r-view-serial', d.is_view_serializable);
  setVal('r-rec',         d.is_recoverable);
  setVal('r-aca',         d.is_aca);
  setVal('r-strict',      d.is_strict);
  setVal('r-rigorous',    d.is_rigorous);

  const orders = d.serial_orders.length
    ? d.serial_orders.map(o=>o.join('→')).join(' | ')
    : (d.is_serializable ? '(none)' : 'N/A');
  document.getElementById('r-orders').textContent = orders;

  const viewOrders = d.view_serial_orders && d.view_serial_orders.length
    ? d.view_serial_orders.map(o=>o.join('→')).join(' | ')
    : (d.is_view_serializable ? '(none)' : 'N/A');
  document.getElementById('r-view-orders').textContent = viewOrders;

  // Ops table
  const tbody = document.getElementById('ops-body');
  tbody.innerHTML = '';
  for(const op of d.operations){
    const cls = op.is_write ? 'op-write' : (op.is_read ? 'op-read' : 'op-ctrl');
    tbody.innerHTML += `<tr class="${cls}">
      <td>${op.step}</td><td>${op.tx}</td><td>${op.op_type}</td><td>${op.item||''}</td>
    </tr>`;
  }

  // Detail log
  document.getElementById('detail-log').textContent = d.explanation.join('\n');

  // Graph
  drawGraph(d.transactions, d.edges, d.cycles_flat, d.is_serializable);
}

function drawGraph(txns, edges, cycleNodes, isSer){
  const g = document.getElementById('graph-g');
  g.innerHTML = '';
  document.getElementById('graph-empty').style.display = 'none';

  const W = document.getElementById('graph-container').clientWidth;
  const H = document.getElementById('graph-container').clientHeight;
  const R = 26;

  // Layout nodes in circle
  const pos = {};
  txns.forEach((t,i)=>{
    const angle = (2*Math.PI*i/txns.length) - Math.PI/2;
    pos[t] = {
      x: W/2 + (Math.min(W,H)/2 - R - 20) * Math.cos(angle),
      y: H/2 + (Math.min(W,H)/2 - R - 20) * Math.sin(angle)
    };
  });
  if(txns.length === 1){
    pos[txns[0]] = {x:W/2, y:H/2};
  }

  // Edges
  const edgeGroup = document.createElementNS('http://www.w3.org/2000/svg','g');
  const edgesSeen = {};
  for(const e of edges){
    const key = e.from_tx+'→'+e.to_tx;
    const isCycleEdge = cycleNodes.includes(e.from_tx) && cycleNodes.includes(e.to_tx) && !isSer;
    const color = isCycleEdge ? '#ef4444' : '#6c63ff';
    const mEnd = isCycleEdge ? 'url(#arrow-red)' : 'url(#arrow)';

    const p1 = pos[e.from_tx], p2 = pos[e.to_tx];
    const dx = p2.x-p1.x, dy = p2.y-p1.y;
    const dist = Math.sqrt(dx*dx+dy*dy)||1;
    const x1 = p1.x + dx/dist*R, y1 = p1.y + dy/dist*R;
    const x2 = p2.x - dx/dist*(R+6), y2 = p2.y - dy/dist*(R+6);

    // Curve offset for multi-edges
    const revKey = e.to_tx+'→'+e.from_tx;
    const offset = edgesSeen[revKey] ? 20 : 0;
    edgesSeen[key] = true;

    const mx = (x1+x2)/2 - dy/dist*offset;
    const my = (y1+y2)/2 + dx/dist*offset;

    const path = document.createElementNS('http://www.w3.org/2000/svg','path');
    path.setAttribute('d', `M${x1},${y1} Q${mx},${my} ${x2},${y2}`);
    path.setAttribute('fill','none');
    path.setAttribute('stroke', color);
    path.setAttribute('stroke-width','2');
    path.setAttribute('marker-end', mEnd);
    edgeGroup.appendChild(path);

    // Edge label
    const lbl = document.createElementNS('http://www.w3.org/2000/svg','text');
    lbl.setAttribute('x', mx); lbl.setAttribute('y', my-4);
    lbl.setAttribute('text-anchor','middle');
    lbl.setAttribute('fill','#94a3b8'); lbl.setAttribute('font-size','10');
    lbl.textContent = `${e.reason}(${e.item})`;
    edgeGroup.appendChild(lbl);
  }
  g.appendChild(edgeGroup);

  // Nodes
  for(const t of txns){
    const {x,y} = pos[t];
    const grp = document.createElementNS('http://www.w3.org/2000/svg','g');
    grp.setAttribute('class','node');

    const inCycle = cycleNodes.includes(t) && !isSer;
    const circ = document.createElementNS('http://www.w3.org/2000/svg','circle');
    circ.setAttribute('cx',x); circ.setAttribute('cy',y); circ.setAttribute('r',R);
    circ.setAttribute('fill', inCycle ? '#450a0a' : '#1e2140');
    circ.setAttribute('stroke', inCycle ? '#ef4444' : '#6c63ff');
    circ.setAttribute('stroke-width','2');
    grp.appendChild(circ);

    const txt = document.createElementNS('http://www.w3.org/2000/svg','text');
    txt.setAttribute('x',x); txt.setAttribute('y',y+5);
    txt.setAttribute('text-anchor','middle');
    txt.setAttribute('fill', inCycle ? '#ef4444' : '#fff');
    txt.setAttribute('font-size','13'); txt.setAttribute('font-weight','700');
    txt.textContent = t;
    grp.appendChild(txt);

    g.appendChild(grp);
  }
}

// Keyboard shortcut
document.addEventListener('keydown', e=>{
  if((e.ctrlKey||e.metaKey) && e.key==='Enter') runAnalysis();
});
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

        explanation = (
            ser.explanation + [""] + rec.explanation
        )

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
            "explanation":          explanation,
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
