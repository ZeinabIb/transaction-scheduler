"""
Schedule Generator
==================
Randomly generates transaction schedules with controllable properties:
  - Number of transactions and data items
  - Guaranteed serializable or non-serializable output
  - Optional abort transactions
"""

from __future__ import annotations
import random
from dataclasses import dataclass
from typing import Optional
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from scheduler import OpType, Operation, Schedule, analyze_serializability


@dataclass
class GeneratorConfig:
    num_transactions: int  = 2       # Number of transactions
    num_items:        int  = 2       # Number of data items (A, B, C, ...)
    ops_per_tx:       int  = 3       # Read/write ops per transaction (excluding START/COMMIT)
    write_prob:       float = 0.5    # Probability each op is a write vs read
    abort_prob:       float = 0.0    # Probability each transaction aborts instead of commits
    inc_dec_prob:     float = 0.1    # Probability write is INCREMENT or DECREMENT
    seed:             Optional[int] = None


def _items(n: int) -> list[str]:
    """Return data item labels: A, B, C, ..., Z, A1, B1, ..."""
    base = [chr(ord('A') + i) for i in range(min(n, 26))]
    extra = [chr(ord('A') + i % 26) + str(i // 26) for i in range(max(0, n - 26))]
    return (base + extra)[:n]


def _tx_name(i: int) -> str:
    return f"T{i+1}"


def generate_random(cfg: GeneratorConfig) -> Schedule:
    """Generate a completely random interleaved schedule."""
    if cfg.seed is not None:
        random.seed(cfg.seed)

    items = _items(cfg.num_items)
    txns  = [_tx_name(i) for i in range(cfg.num_transactions)]

    # Build per-transaction operation lists (without START/COMMIT yet)
    tx_ops: dict[str, list[tuple[OpType, str]]] = {}
    for tx in txns:
        ops = []
        for _ in range(cfg.ops_per_tx):
            item = random.choice(items)
            if random.random() < cfg.write_prob:
                if random.random() < cfg.inc_dec_prob:
                    op = random.choice([OpType.INCREMENT, OpType.DECREMENT])
                else:
                    op = OpType.WRITE
            else:
                op = OpType.READ
            ops.append((op, item))
        tx_ops[tx] = ops

    # Interleave: for each tx keep a pointer, randomly pick which tx goes next
    pointers   = {tx: 0 for tx in txns}
    active     = list(txns)
    committed  = set()
    schedule_ops: list[tuple[str, OpType, Optional[str]]] = []

    # Add all STARTs first
    for tx in txns:
        schedule_ops.append((tx, OpType.START, None))

    while active:
        tx = random.choice(active)
        ptr = pointers[tx]
        tx_op_list = tx_ops[tx]

        if ptr < len(tx_op_list):
            op_type, item = tx_op_list[ptr]
            schedule_ops.append((tx, op_type, item))
            pointers[tx] += 1
        else:
            # Done with data ops — commit or abort
            if random.random() < cfg.abort_prob:
                schedule_ops.append((tx, OpType.ABORT, None))
            else:
                schedule_ops.append((tx, OpType.COMMIT, None))
                committed.add(tx)
            active.remove(tx)

    # Build Operation list
    ops_list = []
    for step, (tx, op_type, item) in enumerate(schedule_ops, start=1):
        ops_list.append(Operation(step=step, tx=tx, op_type=op_type, item=item))

    return Schedule(operations=ops_list)


def generate_serializable(cfg: GeneratorConfig, max_attempts: int = 200) -> Schedule:
    """Generate a random schedule guaranteed to be conflict-serializable."""
    cfg = GeneratorConfig(**{**cfg.__dict__})  # copy
    for _ in range(max_attempts):
        s = generate_random(cfg)
        r = analyze_serializability(s)
        if r.is_serializable:
            return s
    # Fallback: generate a true serial schedule
    return generate_serial(cfg)


def generate_non_serializable(cfg: GeneratorConfig, max_attempts: int = 200) -> Schedule:
    """Generate a random schedule guaranteed NOT to be conflict-serializable."""
    cfg = GeneratorConfig(**{**cfg.__dict__})
    # Ensure enough write ops to create conflicts
    if cfg.write_prob < 0.4:
        cfg.write_prob = 0.4
    for _ in range(max_attempts):
        s = generate_random(cfg)
        r = analyze_serializability(s)
        if not r.is_serializable:
            return s
    # Fallback: build a known non-serializable schedule manually
    return _build_classic_non_serial(cfg)


def generate_serial(cfg: GeneratorConfig) -> Schedule:
    """Generate a true serial schedule (all of T1, then T2, ...)."""
    if cfg.seed is not None:
        random.seed(cfg.seed)
    items = _items(cfg.num_items)
    txns  = [_tx_name(i) for i in range(cfg.num_transactions)]
    ops_list: list[Operation] = []
    step = 1
    for tx in txns:
        ops_list.append(Operation(step=step, tx=tx, op_type=OpType.START)); step += 1
        for _ in range(cfg.ops_per_tx):
            item = random.choice(items)
            op   = OpType.WRITE if random.random() < cfg.write_prob else OpType.READ
            ops_list.append(Operation(step=step, tx=tx, op_type=op, item=item)); step += 1
        ops_list.append(Operation(step=step, tx=tx, op_type=OpType.COMMIT)); step += 1
    return Schedule(operations=ops_list)


def _build_classic_non_serial(cfg: GeneratorConfig) -> Schedule:
    """Always returns a non-serializable schedule (classic RW/WR cycle)."""
    items = _items(max(cfg.num_items, 2))
    A, B  = items[0], items[1]
    ops   = [
        Operation(1, 'T1', OpType.START),
        Operation(2, 'T2', OpType.START),
        Operation(3, 'T1', OpType.READ,  A),
        Operation(4, 'T2', OpType.READ,  B),
        Operation(5, 'T2', OpType.WRITE, A),
        Operation(6, 'T1', OpType.WRITE, B),
        Operation(7, 'T1', OpType.COMMIT),
        Operation(8, 'T2', OpType.COMMIT),
    ]
    return Schedule(operations=ops)


def schedule_to_text(schedule: Schedule) -> str:
    """Convert a Schedule back to parseable text."""
    return '\n'.join(str(op) for op in schedule.operations)


# ── CLI helper ────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Schedule Generator')
    parser.add_argument('--type', choices=['random', 'serial', 'serializable', 'non_serializable'],
                        default='random')
    parser.add_argument('--txns',  type=int, default=2)
    parser.add_argument('--items', type=int, default=2)
    parser.add_argument('--ops',   type=int, default=3)
    parser.add_argument('--seed',  type=int, default=None)
    args = parser.parse_args()

    cfg = GeneratorConfig(
        num_transactions=args.txns,
        num_items=args.items,
        ops_per_tx=args.ops,
        seed=args.seed,
    )
    fn = {'random': generate_random, 'serial': generate_serial,
          'serializable': generate_serializable,
          'non_serializable': generate_non_serializable}[args.type]
    s = fn(cfg)
    print(schedule_to_text(s))
