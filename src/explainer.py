"""
Step-by-Step Violation Explainer
=================================
Walks through a schedule operation-by-operation and narrates exactly
what happens at each step, flagging every violation as it occurs.

This supplements the summary analysis in scheduler.py with a
chronological, human-readable trace.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from scheduler import (
    Schedule, Operation, OpType, WRITE_LIKE,
    analyze_serializability, analyze_recoverability,
)


@dataclass
class StepEvent:
    step:       int
    op:         str          # human-readable operation
    category:   str          # "normal" | "conflict" | "violation" | "info"
    message:    str          # explanation for this step


@dataclass
class StepByStepTrace:
    events:     list[StepEvent] = field(default_factory=list)
    summary:    list[str]       = field(default_factory=list)

    def to_text(self) -> str:
        lines = []
        for ev in self.events:
            prefix = {
                'normal':    '  ✦',
                'conflict':  '  ⚡',
                'violation': '  ✗',
                'info':      '  ℹ',
            }.get(ev.category, '  •')
            lines.append(f"Step {ev.step:>2}  {ev.op:<28}  {prefix} {ev.message}")
        lines.append('')
        lines += self.summary
        return '\n'.join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN TRACER
# ─────────────────────────────────────────────────────────────────────────────

def build_trace(schedule: Schedule) -> StepByStepTrace:
    """
    Walk the schedule step by step, building a narrative trace that explains:
      - What each operation does
      - Which conflicts it introduces (and what edge it adds to the precedence graph)
      - Which recoverability rule it violates (if any)
    """
    ops         = schedule.operations
    trace       = StepByStepTrace()
    events      = trace.events

    # State tracking
    last_writer:   dict[str, str]  = {}   # item → tx that last wrote it
    last_accessor: dict[str, str]  = {}   # item → tx that last accessed it (R or W)
    dirty_reads:   dict[tuple[str,str], str] = {}  # (Ti, item) → Tj (Ti read Tj's dirty write)
    commit_step:   dict[str, int]  = {}
    abort_step:    dict[str, int]  = {}
    started:       set[str]        = set()

    # Pre-compute commit/abort steps for recoverability checks
    for op in ops:
        if op.op_type == OpType.COMMIT:
            commit_step[op.tx] = op.step
        elif op.op_type == OpType.ABORT:
            abort_step[op.tx] = op.step

    # Pre-run serializability to get edge list for annotation
    ser = analyze_serializability(schedule)
    edge_set = {(e.from_tx, e.to_tx, e.item): e.reason for e in ser.edges}

    # Helper: find the edge reason for a given op pair
    def get_edge(from_tx, to_tx, item):
        return edge_set.get((from_tx, to_tx, item))

    # ── Walk each operation ───────────────────────────────────────────────────
    for op in ops:
        op_str = str(op)
        msg    = ''
        cat    = 'normal'

        if op.op_type == OpType.START:
            started.add(op.tx)
            msg = f"{op.tx} begins execution."

        elif op.op_type in (OpType.COMMIT, OpType.ABORT):
            action = "commits" if op.op_type == OpType.COMMIT else "aborts"

            # Check recoverability: did this tx read dirty data?
            violations = []
            for (ti, item), tj in dirty_reads.items():
                if ti == op.tx and op.op_type == OpType.COMMIT:
                    tj_commit = commit_step.get(tj)
                    if tj_commit is None or tj_commit > op.step:
                        violations.append(
                            f"READ({ti},{item}) from uncommitted {tj} "
                            f"→ NON-RECOVERABLE"
                        )

            if violations:
                cat = 'violation'
                msg = f"{op.tx} {action}. ✗ RECOVERABILITY VIOLATION: " + "; ".join(violations)
            else:
                msg = f"{op.tx} {action} successfully."

        elif op.is_read() or op.is_write():
            item     = op.item
            tx       = op.tx
            action   = op.op_type.name  # READ, WRITE, INCREMENT, DECREMENT
            messages = []
            violation_msgs = []

            # ── Conflict detection ────────────────────────────────────────────
            lw = last_writer.get(item)
            la = last_accessor.get(item)

            if lw and lw != tx:
                edge_reason = get_edge(lw, tx, item) or get_edge(tx, lw, item)
                conflict_type = None
                if op.is_read():
                    conflict_type = f"WR conflict with {lw}'s earlier WRITE({item})"
                    messages.append(f"⚡ {conflict_type}  →  edge {lw}→{tx} in precedence graph")
                else:
                    conflict_type = f"WW conflict with {lw}'s earlier WRITE({item})"
                    messages.append(f"⚡ {conflict_type}  →  edge {lw}→{tx} in precedence graph")
                cat = 'conflict'

            if la and la != tx and op.is_write():
                if la != lw:   # avoid duplicate message
                    messages.append(
                        f"⚡ RW conflict: {la} previously READ({item}), now {tx} writes  "
                        f"→  edge {la}→{tx} in precedence graph"
                    )
                    cat = 'conflict'

            # ── ACA / Strict / Rigorous violation detection ───────────────────
            if op.is_read() and lw and lw != tx:
                lw_commit = commit_step.get(lw)
                if lw_commit is None or lw_commit > op.step:
                    violation_msgs.append(
                        f"✗ ACA VIOLATION: reading dirty data from {lw} "
                        f"(not yet committed at step {op.step})"
                    )
                    dirty_reads[(tx, item)] = lw
                    cat = 'violation'

            if lw and lw != tx:
                lw_done = min(
                    commit_step.get(lw, float('inf')),
                    abort_step.get(lw,  float('inf'))
                )
                if lw_done > op.step:
                    violation_msgs.append(
                        f"✗ STRICT VIOLATION: {lw} (last writer of {item}) "
                        f"has not yet committed/aborted"
                    )
                    cat = 'violation'

            if la and la != tx:
                la_done = min(
                    commit_step.get(la, float('inf')),
                    abort_step.get(la,  float('inf'))
                )
                if la_done > op.step:
                    violation_msgs.append(
                        f"✗ RIGOROUS VIOLATION: {la} (last accessor of {item}) "
                        f"has not yet committed/aborted"
                    )
                    cat = 'violation'

            # ── Combine messages ──────────────────────────────────────────────
            base = f"{tx} {action.lower()}s {item}."
            all_msgs = [base] + messages + violation_msgs
            msg = "  ".join(all_msgs)

            # ── Update state ──────────────────────────────────────────────────
            if op.is_write():
                last_writer[item]   = tx
            last_accessor[item] = tx

        events.append(StepEvent(step=op.step, op=op_str, category=cat, message=msg))

    # ── Final summary ─────────────────────────────────────────────────────────
    rec = analyze_recoverability(schedule)
    summary = [
        "─" * 70,
        "STEP-BY-STEP SUMMARY",
        "─" * 70,
    ]

    if ser.cycles:
        for cyc in ser.cycles:
            summary.append(f"  ✗ Cycle in precedence graph: {' → '.join(cyc)}")
        summary.append("  ✗ Schedule is NOT conflict-serializable")
    else:
        orders = " | ".join("→".join(o) for o in ser.serial_orders)
        summary.append(f"  ✓ No cycles — conflict-serializable")
        summary.append(f"    Equivalent serial order(s): {orders}")

    props = [
        ("Recoverable", rec.is_recoverable),
        ("ACA",         rec.is_aca),
        ("Strict",      rec.is_strict),
        ("Rigorous",    rec.is_rigorous),
    ]
    for name, val in props:
        tick = "✓" if val else "✗"
        summary.append(f"  {tick} {name}: {'YES' if val else 'NO'}")

    # Count conflict steps
    n_conflicts  = sum(1 for e in events if e.category == 'conflict')
    n_violations = sum(1 for e in events if e.category == 'violation')
    summary.append(f"\n  Conflict steps   : {n_conflicts}")
    summary.append(f"  Violation steps  : {n_violations}")

    trace.summary = summary
    return trace


# ── Standalone usage ──────────────────────────────────────────────────────────
if __name__ == '__main__':
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from scheduler import parse_schedule

    text = """
    START(T1)
    START(T2)
    WRITE(T1,A)
    READ(T2,A)
    COMMIT(T2)
    COMMIT(T1)
    """
    s = parse_schedule(text)
    trace = build_trace(s)
    print(trace.to_text())
