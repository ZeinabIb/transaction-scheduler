"""
Transaction Scheduler Simulator
================================
Analyzes database schedules for:
  - Conflict serializability (via precedence/serialization graph)
  - Recoverability, ACA, Strict, and Rigorous schedule properties

Supported operations:
  START(Ti), COMMIT(Ti), ABORT(Ti),
  READ(Ti, X), WRITE(Ti, X), INCREMENT(Ti, X), DECREMENT(Ti, X)
"""

from __future__ import annotations
import re
from dataclasses import dataclass, field
from enum import Enum, auto
from itertools import permutations
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# 1. DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

class OpType(Enum):
    START     = auto()
    READ      = auto()
    WRITE     = auto()
    INCREMENT = auto()
    DECREMENT = auto()
    COMMIT    = auto()
    ABORT     = auto()


# Operations that are considered "write-like" for conflict and strictness checks
WRITE_LIKE = {OpType.WRITE, OpType.INCREMENT, OpType.DECREMENT}
READ_LIKE  = {OpType.READ}


@dataclass
class Operation:
    step:    int           # Position in schedule (1-based)
    tx:      str           # Transaction ID, e.g. "T1"
    op_type: OpType
    item:    Optional[str] = None   # Data item (X, Y, …) if applicable

    def is_read(self)  -> bool: return self.op_type in READ_LIKE
    def is_write(self) -> bool: return self.op_type in WRITE_LIKE

    def __str__(self) -> str:
        if self.item:
            return f"{self.op_type.name}({self.tx},{self.item})"
        return f"{self.op_type.name}({self.tx})"


@dataclass
class Schedule:
    operations: list[Operation] = field(default_factory=list)

    @property
    def transactions(self) -> list[str]:
        """Return ordered unique list of transaction IDs."""
        seen, result = set(), []
        for op in self.operations:
            if op.tx not in seen:
                seen.add(op.tx)
                result.append(op.tx)
        return result


# ─────────────────────────────────────────────────────────────────────────────
# 2. PARSER
# ─────────────────────────────────────────────────────────────────────────────

# Regex patterns for each operation format
_OP_PATTERNS = [
    (re.compile(r'^START\s*\(\s*(\w+)\s*\)$',     re.I), OpType.START,     False),
    (re.compile(r'^COMMIT\s*\(\s*(\w+)\s*\)$',    re.I), OpType.COMMIT,    False),
    (re.compile(r'^ABORT\s*\(\s*(\w+)\s*\)$',     re.I), OpType.ABORT,     False),
    (re.compile(r'^READ\s*\(\s*(\w+)\s*,\s*(\w+)\s*\)$',      re.I), OpType.READ,      True),
    (re.compile(r'^WRITE\s*\(\s*(\w+)\s*,\s*(\w+)\s*\)$',     re.I), OpType.WRITE,     True),
    (re.compile(r'^INCREMENT\s*\(\s*(\w+)\s*,\s*(\w+)\s*\)$', re.I), OpType.INCREMENT, True),
    (re.compile(r'^DECREMENT\s*\(\s*(\w+)\s*,\s*(\w+)\s*\)$', re.I), OpType.DECREMENT, True),
]


def parse_schedule(text: str) -> Schedule:
    """
    Parse a schedule from a text string.
    Accepts one operation per line, or comma/semicolon separated.
    """
    # Normalize line separators; only split on semicolons and commas
    # that are NOT inside parentheses (so READ(T1,A) stays intact).
    import re as _re
    # Replace semicolons with newlines
    text = text.replace(';', '\n')
    # Replace commas that are outside parentheses with newlines
    # (track paren depth)
    result_chars: list[str] = []
    depth = 0
    for ch in text:
        if ch == '(':
            depth += 1
            result_chars.append(ch)
        elif ch == ')':
            depth -= 1
            result_chars.append(ch)
        elif ch == ',' and depth == 0:
            result_chars.append('\n')
        else:
            result_chars.append(ch)
    text = ''.join(result_chars)
    tokens = [t.strip() for t in text.splitlines() if t.strip()]

    ops: list[Operation] = []
    errors: list[str] = []

    for step, token in enumerate(tokens, start=1):
        matched = False
        for pattern, op_type, has_item in _OP_PATTERNS:
            m = pattern.match(token)
            if m:
                tx = m.group(1).upper()
                item = m.group(2).upper() if has_item else None
                ops.append(Operation(step=step, tx=tx, op_type=op_type, item=item))
                matched = True
                break
        if not matched:
            errors.append(f"  Step {step}: unrecognized token '{token}'")

    if errors:
        raise ValueError("Parse errors:\n" + "\n".join(errors))

    _validate_structure(ops)
    return Schedule(operations=ops)


def _validate_structure(ops: list[Operation]) -> None:
    """Validate well-formedness: START before ops, single COMMIT/ABORT."""
    started:   dict[str, int]  = {}
    finished:  dict[str, str]  = {}
    issues: list[str] = []

    for op in ops:
        tx = op.tx
        if op.op_type == OpType.START:
            if tx in started:
                issues.append(f"{tx} has multiple START operations.")
            started[tx] = op.step
        elif op.op_type in (OpType.COMMIT, OpType.ABORT):
            if tx not in started:
                issues.append(f"{tx} has {op.op_type.name} without START.")
            if tx in finished:
                issues.append(f"{tx} has multiple COMMIT/ABORT operations.")
            finished[tx] = op.op_type.name
        else:
            if tx not in started:
                issues.append(f"{tx} performs {op} before START.")
            if tx in finished:
                issues.append(f"{tx} performs {op} after {finished[tx]}.")

    if issues:
        raise ValueError("Structural errors:\n" + "\n".join(f"  {i}" for i in issues))


# ─────────────────────────────────────────────────────────────────────────────
# 3. CONFLICT SERIALIZABILITY  (Precedence / Serialization Graph)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ConflictEdge:
    from_tx:   str
    to_tx:     str
    item:      str
    reason:    str   # human-readable conflict description

@dataclass
class SerializabilityResult:
    is_serializable: bool
    edges:           list[ConflictEdge]
    cycles:          list[list[str]]
    serial_orders:   list[list[str]]   # topological orderings if no cycle
    explanation:     list[str]


@dataclass
class ViewSerializabilityResult:
    is_view_serializable:     bool
    equivalent_serial_orders: list[list[str]]   # empty if not serializable
    explanation:              list[str]


def analyze_serializability(schedule: Schedule) -> SerializabilityResult:
    """
    Build the precedence graph and detect cycles via DFS.
    Two operations conflict iff:
      - They belong to different transactions
      - They access the same data item
      - At least one is a write-like operation
    """
    ops  = schedule.operations
    txns = schedule.transactions
    edges: list[ConflictEdge] = []
    seen_pairs: set[tuple[str,str,str]] = set()  # (from, to, item) dedup

    explanation: list[str] = []
    explanation.append("=== Precedence Graph Construction ===")

    # Build adjacency: for each data item, scan pairs in schedule order
    for i in range(len(ops)):
        oi = ops[i]
        if not (oi.is_read() or oi.is_write()):
            continue
        for j in range(i + 1, len(ops)):
            oj = ops[j]
            if not (oj.is_read() or oj.is_write()):
                continue
            if oi.tx == oj.tx:
                continue
            if oi.item != oj.item:
                continue
            # At least one must be write-like
            if not (oi.is_write() or oj.is_write()):
                continue

            # Conflict: oi precedes oj → edge from_tx → to_tx
            key = (oi.tx, oj.tx, oi.item)
            if key not in seen_pairs:
                seen_pairs.add(key)
                conflict_type = _conflict_label(oi, oj)
                e = ConflictEdge(
                    from_tx=oi.tx, to_tx=oj.tx,
                    item=oi.item, reason=conflict_type
                )
                edges.append(e)
                explanation.append(
                    f"  Edge {oi.tx} → {oj.tx}  "
                    f"[{conflict_type} on {oi.item}]  "
                    f"(step {oi.step} before step {oj.step})"
                )

    if not edges:
        explanation.append("  (No conflicting pairs found — graph is empty)")

    # Detect cycles
    adj: dict[str, set[str]] = {t: set() for t in txns}
    for e in edges:
        adj[e.from_tx].add(e.to_tx)

    cycles = _find_cycles(txns, adj)

    if cycles:
        for cyc in cycles:
            explanation.append(f"  Cycle detected: {' → '.join(cyc)}")
        return SerializabilityResult(
            is_serializable=False,
            edges=edges,
            cycles=cycles,
            serial_orders=[],
            explanation=explanation,
        )

    # No cycles → topological sort(s)
    orders = _all_topological_sorts(txns, adj)
    explanation.append("  No cycles detected.")
    explanation.append(f"  Equivalent serial order(s): "
                       + ", ".join("→".join(o) for o in orders))

    return SerializabilityResult(
        is_serializable=True,
        edges=edges,
        cycles=[],
        serial_orders=orders,
        explanation=explanation,
    )


def _conflict_label(oi: Operation, oj: Operation) -> str:
    if oi.is_write() and oj.is_write(): return "WW"
    if oi.is_write() and oj.is_read():  return "WR"
    if oi.is_read()  and oj.is_write(): return "RW"
    return "??"


def _find_cycles(txns: list[str], adj: dict[str, set[str]]) -> list[list[str]]:
    """Find all elementary cycles using Johnson's-style DFS."""
    cycles: list[list[str]] = []
    visited: set[str] = set()
    path:    list[str] = []

    def dfs(node: str, start: str):
        path.append(node)
        for nbr in sorted(adj.get(node, [])):
            if nbr == start:
                cycles.append(path[:] + [start])
            elif nbr not in visited:
                dfs(nbr, start)
        path.pop()

    for tx in txns:
        visited.add(tx)
        dfs(tx, tx)

    # Deduplicate cycles (normalize rotation)
    unique = []
    seen_cycles: set[frozenset] = set()
    for c in cycles:
        key = frozenset(c)
        if key not in seen_cycles:
            seen_cycles.add(key)
            unique.append(c)
    return unique


def _all_topological_sorts(txns: list[str], adj: dict[str, set[str]]) -> list[list[str]]:
    """Return all topological orderings (Kahn's + backtracking)."""
    in_deg = {t: 0 for t in txns}
    for t in txns:
        for nbr in adj[t]:
            in_deg[nbr] += 1

    results: list[list[str]] = []

    def bt(path: list[str], indeg: dict[str, int]):
        available = sorted(t for t in txns if t not in path and indeg[t] == 0)
        if not available:
            if len(path) == len(txns):
                results.append(path[:])
            return
        for t in available:
            path.append(t)
            for nbr in adj[t]:
                indeg[nbr] -= 1
            bt(path, indeg)
            path.pop()
            for nbr in adj[t]:
                indeg[nbr] += 1

    bt([], dict(in_deg))
    return results


# ─────────────────────────────────────────────────────────────────────────────
# 4. RECOVERABILITY PROPERTIES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RecoverabilityResult:
    is_recoverable: bool
    is_aca:         bool
    is_strict:      bool
    is_rigorous:    bool
    explanation:    list[str]


def analyze_recoverability(schedule: Schedule) -> RecoverabilityResult:
    ops = schedule.operations

    # Pre-compute commit/abort step for each transaction
    commit_step: dict[str, int] = {}
    abort_step:  dict[str, int] = {}
    for op in ops:
        if op.op_type == OpType.COMMIT:
            commit_step[op.tx] = op.step
        elif op.op_type == OpType.ABORT:
            abort_step[op.tx] = op.step

    explanation: list[str] = []

    # ── Recoverable ──────────────────────────────────────────────────────────
    # Ti commits only after every Tj whose dirty data Ti read has committed.
    rec_violations: list[str] = []
    explanation.append("=== Recoverability Check ===")

    # For each READ(Ti, X) at step s, find the latest WRITE(Tj, X) before s
    for op in ops:
        if not op.is_read():
            continue
        writer = _last_writer_before(ops, op.tx, op.item, op.step)
        if writer is None:
            continue   # reading initial value — no dependency
        # Ti read data written by writer_tx
        ti_commit = commit_step.get(op.tx)
        wj_commit = commit_step.get(writer)
        if ti_commit is None:
            continue   # Ti never commits — no recoverability issue
        wj_abort = abort_step.get(writer)
        if wj_commit is None:
            # writer aborted or is still active — Ti committed without writer committing
            outcome = "aborted" if wj_abort is not None else "never commits"
            msg = (f"  VIOLATION: {op.tx} reads {op.item} written by {writer} "
                   f"(step {op.step}), but {writer} {outcome} while "
                   f"{op.tx} commits at step {ti_commit}.")
            rec_violations.append(msg)
            explanation.append(msg)
        elif ti_commit < wj_commit:
            msg = (f"  VIOLATION: {op.tx} commits (step {ti_commit}) before "
                   f"{writer} commits (step {wj_commit}), "
                   f"but {op.tx} read dirty data ({op.item}) from {writer}.")
            rec_violations.append(msg)
            explanation.append(msg)

    is_recoverable = len(rec_violations) == 0
    if is_recoverable:
        explanation.append("  ✓ Recoverable: all transactions commit after "
                           "the transactions whose data they read.")
    else:
        explanation.append("  ✗ NOT Recoverable: a transaction committed before "
                           "a transaction whose data it read.")

    # ── ACA ──────────────────────────────────────────────────────────────────
    aca_violations: list[str] = []
    explanation.append("\n=== ACA (Avoids Cascading Aborts) Check ===")

    for op in ops:
        if not op.is_read():
            continue
        writer = _last_writer_before(ops, op.tx, op.item, op.step)
        if writer is None:
            continue
        wj_commit = commit_step.get(writer)
        # ACA: Ti may only read data written by Tj if Tj committed before this read
        wj_abort = abort_step.get(writer)
        if wj_commit is None or wj_commit > op.step:
            if wj_abort is not None and wj_abort < op.step:
                status = f"{writer} already aborted before this read"
            elif wj_commit is None:
                status = f"{writer} has not committed"
            else:
                status = f"{writer} commits later at step {wj_commit}"
            msg = (f"  VIOLATION: {op.tx} reads {op.item} (step {op.step}) "
                   f"written by {writer} — {status}.")
            aca_violations.append(msg)
            explanation.append(msg)

    is_aca = len(aca_violations) == 0
    if is_aca:
        explanation.append("  ✓ ACA: all reads are from committed transactions.")
    else:
        explanation.append("  ✗ NOT ACA: a transaction read data written by an "
                           "uncommitted (or aborted) transaction.")

    # ── Strict ────────────────────────────────────────────────────────────────
    # No R or W on X until the last transaction that wrote X has committed/aborted.
    strict_violations: list[str] = []
    explanation.append("\n=== Strict Schedule Check ===")

    for op in ops:
        if not (op.is_read() or op.is_write()):
            continue
        writer = _last_writer_before(ops, op.tx, op.item, op.step)
        if writer is None:
            continue
        w_commit = commit_step.get(writer)
        w_abort  = abort_step.get(writer)
        w_done   = min(
            w_commit if w_commit else float('inf'),
            w_abort  if w_abort  else float('inf')
        )
        if w_done > op.step:
            action = "reads" if op.is_read() else "writes"
            msg = (f"  VIOLATION: {op.tx} {action} {op.item} (step {op.step}) "
                   f"before {writer} (last writer) commits/aborts (step {int(w_done)}).")
            strict_violations.append(msg)
            explanation.append(msg)

    is_strict = len(strict_violations) == 0
    if is_strict:
        explanation.append("  ✓ Strict: no R/W on item until last writer committed/aborted.")
    else:
        explanation.append("  ✗ NOT Strict: a transaction read or wrote an item "
                           "before the last writer committed/aborted.")

    # ── Rigorous ──────────────────────────────────────────────────────────────
    # Stronger: no R or W until the last READ or WRITE lock holder committed/aborted.
    rigorous_violations: list[str] = []
    explanation.append("\n=== Rigorous Schedule Check ===")

    for op in ops:
        if not (op.is_read() or op.is_write()):
            continue
        # Last accessor (read OR write) on same item, different tx, before this step
        last_acc = _last_accessor_before(ops, op.tx, op.item, op.step)
        if last_acc is None:
            continue
        la_commit = commit_step.get(last_acc)
        la_abort  = abort_step.get(last_acc)
        la_done   = min(
            la_commit if la_commit else float('inf'),
            la_abort  if la_abort  else float('inf')
        )
        if la_done > op.step:
            action = "reads" if op.is_read() else "writes"
            msg = (f"  VIOLATION: {op.tx} {action} {op.item} (step {op.step}) "
                   f"before {last_acc} (last accessor) commits/aborts (step {int(la_done)}).")
            rigorous_violations.append(msg)
            explanation.append(msg)

    is_rigorous = len(rigorous_violations) == 0
    if is_rigorous:
        explanation.append("  ✓ Rigorous: no R/W on item until last accessor committed/aborted.")
    else:
        explanation.append("  ✗ NOT Rigorous: a transaction read or wrote an item "
                           "before the last accessor (reader or writer) committed/aborted.")

    return RecoverabilityResult(
        is_recoverable=is_recoverable,
        is_aca=is_aca,
        is_strict=is_strict,
        is_rigorous=is_rigorous,
        explanation=explanation,
    )


def _last_writer_before(ops: list[Operation], tx: str, item: str, step: int) -> Optional[str]:
    """Return the transaction ID of the last write-like op on `item` before `step`, from a different tx."""
    last: Optional[str] = None
    for op in ops:
        if op.step >= step:
            break
        if op.tx != tx and op.item == item and op.is_write():
            last = op.tx
    return last


def _last_accessor_before(ops: list[Operation], tx: str, item: str, step: int) -> Optional[str]:
    """Return the tx of the last R or W op on `item` before `step`, from a different tx."""
    last: Optional[str] = None
    for op in ops:
        if op.step >= step:
            break
        if op.tx != tx and op.item == item and (op.is_read() or op.is_write()):
            last = op.tx
    return last


# ─────────────────────────────────────────────────────────────────────────────
# 5. VIEW SERIALIZABILITY
# ─────────────────────────────────────────────────────────────────────────────

def _extract_view(ops: list[Operation]) -> tuple[dict, dict]:
    """
    Extract the view of a schedule.
    Returns:
        reads_from:   (reader_tx, item, occurrence_index) -> writer_tx | None
        final_writes: item -> last_writer_tx
    occurrence_index is 0-based per (tx, item) pair, handling multiple reads
    of the same item by the same transaction.
    """
    reads_from:  dict = {}
    final_writes: dict = {}
    read_counts:  dict = {}
    last_writer:  dict = {}

    for op in ops:
        if op.is_read():
            key = (op.tx, op.item)
            idx = read_counts.get(key, 0)
            read_counts[key] = idx + 1
            reads_from[(op.tx, op.item, idx)] = last_writer.get(op.item)
        elif op.is_write():
            last_writer[op.item] = op.tx
            final_writes[op.item] = op.tx

    return reads_from, final_writes


def _serial_ops(schedule: Schedule, order: list[str]) -> list[Operation]:
    """Build operation list for a serial schedule in the given transaction order."""
    groups: dict[str, list[Operation]] = {tx: [] for tx in order}
    for op in schedule.operations:
        if op.tx in groups:
            groups[op.tx].append(op)
    result = []
    step = 1
    for tx in order:
        for op in groups[tx]:
            result.append(Operation(step=step, tx=op.tx,
                                    op_type=op.op_type, item=op.item))
            step += 1
    return result


def _fmt_reads_from(rf: dict) -> str:
    if not rf:
        return "{}"
    parts = []
    for (tx, item, idx), writer in sorted(rf.items()):
        occ = f"[{idx}]" if idx > 0 else ""
        parts.append(f"{tx} reads {item}{occ} from "
                     f"{'initial' if writer is None else writer}")
    return "; ".join(parts)


def analyze_view_serializability(schedule: Schedule) -> ViewSerializabilityResult:
    """
    Check view serializability by comparing the schedule's view against all
    n! serial schedules of the same transactions.

    Two schedules are view-equivalent iff for every data item X:
      1. Same transaction reads the initial value of X (initial reads).
      2. Same Ti reads the value written by the same Tj on X (reads-from).
      3. Same transaction performs the last write on X (final writes).
    """
    ops  = schedule.operations
    txns = schedule.transactions

    explanation: list[str] = []
    explanation.append("=== View Serializability Analysis ===")

    s_reads_from, s_final_writes = _extract_view(ops)
    explanation.append(f"  reads_from  : {_fmt_reads_from(s_reads_from)}")
    explanation.append(f"  final_writes: {s_final_writes or '(none)'}")

    matching: list[list[str]] = []
    for perm in permutations(txns):
        order = list(perm)
        p_rf, p_fw = _extract_view(_serial_ops(schedule, order))
        if s_reads_from == p_rf and s_final_writes == p_fw:
            matching.append(order)
            explanation.append(
                f"  View-equivalent serial order: {' → '.join(order)}")

    is_vs = bool(matching)
    if not is_vs:
        explanation.append("  No view-equivalent serial schedule found.")
        explanation.append("  Schedule is NOT view-serializable.")
    else:
        explanation.append(
            f"  Schedule IS view-serializable "
            f"({len(matching)} equivalent serial order(s)).")

    return ViewSerializabilityResult(
        is_view_serializable=is_vs,
        equivalent_serial_orders=matching,
        explanation=explanation,
    )


# ─────────────────────────────────────────────────────────────────────────────
# 6. FULL ANALYSIS  (entry point used by UI)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AnalysisReport:
    schedule:             Schedule
    serializability:      SerializabilityResult
    recoverability:       RecoverabilityResult
    view_serializability: ViewSerializabilityResult


def analyze(schedule_text: str) -> AnalysisReport:
    schedule = parse_schedule(schedule_text)
    ser  = analyze_serializability(schedule)
    rec  = analyze_recoverability(schedule)
    vser = analyze_view_serializability(schedule)
    return AnalysisReport(schedule=schedule,
                          serializability=ser,
                          recoverability=rec,
                          view_serializability=vser)
