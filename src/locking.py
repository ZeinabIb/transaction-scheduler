"""
2PL / Strict 2PL Locking Protocol Simulator
=============================================
Simulates lock acquisition and release, then verifies whether a schedule
conforms to:
  - Two-Phase Locking (2PL): all lock acquisitions happen before any release
  - Strict 2PL: all locks held until commit/abort (ensures strict schedule)

Lock types: SHARED (S) = read lock, EXCLUSIVE (X) = write lock

Compatibility matrix:
        S      X
   S    ✓      ✗
   X    ✗      ✗
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from scheduler import Schedule, Operation, OpType, WRITE_LIKE


# ─────────────────────────────────────────────────────────────────────────────
# LOCK DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

class LockType(Enum):
    SHARED    = 'S'   # read lock
    EXCLUSIVE = 'X'   # write lock


@dataclass
class LockAction:
    step:      int
    tx:        str
    item:      str
    action:    str        # "ACQUIRE_S", "ACQUIRE_X", "UPGRADE", "RELEASE", "WAIT", "ABORT"
    lock_type: Optional[LockType] = None
    message:   str = ''


@dataclass
class LockState:
    """Current lock table entry for one data item."""
    item:       str
    holders:    dict[str, LockType] = field(default_factory=dict)  # tx → lock type
    wait_queue: list[tuple[str, LockType]] = field(default_factory=list)

    def is_compatible(self, tx: str, requested: LockType) -> bool:
        """Can `tx` acquire `requested` without conflict?"""
        for holder, held in self.holders.items():
            if holder == tx:
                continue   # own locks don't conflict
            if requested == LockType.EXCLUSIVE or held == LockType.EXCLUSIVE:
                return False
        return True

    def summary(self) -> str:
        if not self.holders:
            return "(unlocked)"
        parts = [f"{tx}:{lt.value}" for tx, lt in self.holders.items()]
        return "{" + ", ".join(parts) + "}"


@dataclass
class TwoPLResult:
    is_2pl:        bool
    is_strict_2pl: bool
    lock_actions:  list[LockAction]
    violations:    list[str]
    explanation:   list[str]

    def to_text(self) -> str:
        lines = []
        lines.append("=== Lock Acquisition / Release Trace ===")
        lines.append(f"  {'Step':<6}{'Operation':<28}{'Lock Action':<38}{'Table State'}")
        lines.append("  " + "─"*90)

        # Group actions by step
        by_step: dict[int, list[LockAction]] = {}
        for a in self.lock_actions:
            by_step.setdefault(a.step, []).append(a)

        for step in sorted(by_step):
            for a in by_step[step]:
                icon = {'ACQUIRE_S':'🔒','ACQUIRE_X':'🔐','UPGRADE':'⬆',
                        'RELEASE':'🔓','WAIT':'⏳','ABORT':'💥'}.get(a.action,'•')
                lines.append(f"  {step:<6}{a.message:<28}  {icon} {a.action}"
                             + (f"({a.item})" if a.item else ""))

        lines.append("")
        lines += self.explanation
        lines.append("")
        lines.append(f"  2PL        : {'✓ YES' if self.is_2pl else '✗ NO'}")
        lines.append(f"  Strict 2PL : {'✓ YES' if self.is_strict_2pl else '✗ NO'}")
        return '\n'.join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# SIMULATOR
# ─────────────────────────────────────────────────────────────────────────────

def simulate_2pl(schedule: Schedule) -> TwoPLResult:
    """
    Simulate lock acquisition for the given schedule and check 2PL conformance.

    Strategy:
      - Acquire S-lock before each READ, X-lock before each WRITE/INC/DEC
      - Upgrade S→X when a tx already holds S on an item and needs to write
      - Release all locks at COMMIT or ABORT
      - Track the "growing phase end" per transaction (first release step)
      - A schedule violates 2PL if any lock is acquired after the first release
      - Strict 2PL: locks must be held until commit/abort (always true in this sim
        since we release only at commit/abort)
    """
    ops         = schedule.operations
    lock_table: dict[str, LockState] = {}  # item → LockState
    tx_locks:   dict[str, dict[str, LockType]] = {}  # tx → {item: lock_type}
    growing_end: dict[str, Optional[int]] = {}  # tx → step of first release (None = still growing)
    actions:    list[LockAction] = []
    violations: list[str] = []
    explanation: list[str] = ["=== 2PL Protocol Analysis ==="]

    def get_ls(item: str) -> LockState:
        if item not in lock_table:
            lock_table[item] = LockState(item=item)
        return lock_table[item]

    def acquire(tx: str, item: str, ltype: LockType, step: int, op_str: str):
        ls = get_ls(item)
        current = tx_locks.get(tx, {}).get(item)

        # Upgrade check
        if current == LockType.SHARED and ltype == LockType.EXCLUSIVE:
            # Need to upgrade: check no other holder
            others = [h for h in ls.holders if h != tx]
            if others:
                violations.append(
                    f"Step {step}: {tx} cannot upgrade S→X on {item} "
                    f"(held by {', '.join(others)}) — LOCK CONFLICT"
                )
                actions.append(LockAction(step=step, tx=tx, item=item,
                                          action='WAIT', message=op_str,
                                          lock_type=ltype))
                return False
            ls.holders[tx] = LockType.EXCLUSIVE
            tx_locks.setdefault(tx, {})[item] = LockType.EXCLUSIVE
            actions.append(LockAction(step=step, tx=tx, item=item,
                                      action='UPGRADE', message=op_str,
                                      lock_type=ltype))
            explanation.append(f"  Step {step}: {tx} upgrades S→X on {item}")
            return True

        if current is not None:
            # Already hold compatible lock
            return True

        # Fresh acquisition
        # 2PL check: are we still in growing phase?
        gend = growing_end.get(tx)
        if gend is not None:
            violations.append(
                f"Step {step}: {tx} acquires {ltype.value}-lock on {item} "
                f"AFTER releasing a lock at step {gend} — violates 2PL"
            )
            explanation.append(
                f"  ✗ 2PL VIOLATION at step {step}: {tx} tries to acquire "
                f"{ltype.value}({item}) after shrinking phase began (step {gend})"
            )

        if ls.is_compatible(tx, ltype):
            ls.holders[tx] = ltype
            tx_locks.setdefault(tx, {})[item] = ltype
            lock_name = 'ACQUIRE_S' if ltype == LockType.SHARED else 'ACQUIRE_X'
            actions.append(LockAction(step=step, tx=tx, item=item,
                                      action=lock_name, message=op_str,
                                      lock_type=ltype))
            explanation.append(
                f"  Step {step}: {tx} acquires {ltype.value}-lock on {item}  "
                f"table: {ls.summary()}"
            )
            return True
        else:
            holders_str = ls.summary()
            violations.append(
                f"Step {step}: {tx} blocked — cannot acquire {ltype.value}({item}), "
                f"currently held: {holders_str}"
            )
            actions.append(LockAction(step=step, tx=tx, item=item,
                                      action='WAIT', message=op_str,
                                      lock_type=ltype))
            explanation.append(
                f"  ⏳ Step {step}: {tx} BLOCKED on {ltype.value}({item})  "
                f"held: {holders_str}"
            )
            return False

    def release_all(tx: str, step: int, op_str: str):
        held = tx_locks.get(tx, {})
        if not held:
            return
        # First release — record growing phase end
        if growing_end.get(tx) is None:
            growing_end[tx] = step
        for item in list(held.keys()):
            ls = get_ls(item)
            del ls.holders[tx]
            actions.append(LockAction(step=step, tx=tx, item=item,
                                      action='RELEASE', message=op_str))
            explanation.append(
                f"  Step {step}: {tx} releases {item}  "
                f"table: {ls.summary()}"
            )
        tx_locks[tx] = {}

    # ── Process each operation ────────────────────────────────────────────────
    for op in ops:
        op_str = str(op)
        if op.op_type == OpType.START:
            tx_locks[op.tx] = {}
            growing_end[op.tx] = None
            explanation.append(f"  Step {op.step}: {op.tx} starts")

        elif op.is_read():
            acquire(op.tx, op.item, LockType.SHARED, op.step, op_str)

        elif op.is_write():
            acquire(op.tx, op.item, LockType.EXCLUSIVE, op.step, op_str)

        elif op.op_type in (OpType.COMMIT, OpType.ABORT):
            release_all(op.tx, op.step, op_str)
            explanation.append(
                f"  Step {op.step}: {op.tx} {'commits' if op.op_type==OpType.COMMIT else 'aborts'}"
            )

    # ── Determine results ─────────────────────────────────────────────────────
    is_2pl = not any("violates 2PL" in v for v in violations)
    # Strict 2PL: we always release at commit/abort, so if 2PL holds, strict 2PL holds too
    is_strict_2pl = is_2pl

    if is_2pl:
        explanation.append("\n  ✓ Schedule conforms to 2PL (lock point exists for each transaction)")
        explanation.append("  ✓ Strict 2PL: all locks released at commit/abort")
    else:
        explanation.append("\n  ✗ Schedule does NOT conform to 2PL")
        for v in violations:
            explanation.append(f"    → {v}")

    return TwoPLResult(
        is_2pl=is_2pl,
        is_strict_2pl=is_strict_2pl,
        lock_actions=actions,
        violations=violations,
        explanation=explanation,
    )


# ── Standalone test ───────────────────────────────────────────────────────────
if __name__ == '__main__':
    from scheduler import parse_schedule
    text = """
    START(T1)
    START(T2)
    READ(T1,A)
    READ(T2,A)
    WRITE(T1,A)
    COMMIT(T1)
    WRITE(T2,A)
    COMMIT(T2)
    """
    s = parse_schedule(text)
    r = simulate_2pl(s)
    print(r.to_text())
