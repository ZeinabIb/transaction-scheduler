# Transaction Scheduling & Serializability Analyzer

## Overview
A complete implementation of a Transaction Scheduler Simulator for the
*Transaction Processing Systems* graduate course project.

### Features
- Parse schedules with START / READ / WRITE / INCREMENT / DECREMENT / COMMIT / ABORT
- **Conflict serializability** via precedence graph (cycle detection + all serial orders)
- **Recoverability**, **ACA**, **Strict**, and **Rigorous** schedule analysis
- INCREMENT/DECREMENT treated correctly as write-like operations
- Interactive CLI **and** web-based GUI with precedence graph visualization

---

## Quick Start

### CLI (no dependencies)
```bash
# Interactive prompt
python src/cli.py

# Analyse a file
python src/cli.py --file my_schedule.txt

# Run built-in demos
python src/cli.py --demo
```

### Web GUI
```bash
pip install flask
python src/app.py
# Open http://localhost:5000
```

---

## Input Format
One operation per line (or comma/semicolon-separated):
```
START(T1)
START(T2)
READ(T1,A)
WRITE(T2,A)
COMMIT(T1)
COMMIT(T2)
```

**Supported operations:**
| Operation | Format |
|-----------|--------|
| Start transaction | `START(Ti)` |
| Read | `READ(Ti, X)` |
| Write | `WRITE(Ti, X)` |
| Increment | `INCREMENT(Ti, X)` |
| Decrement | `DECREMENT(Ti, X)` |
| Commit | `COMMIT(Ti)` |
| Abort | `ABORT(Ti)` |

---

## Project Structure
```
transaction-scheduler/
├── src/
│   ├── scheduler.py   # Core engine: parser + analysis algorithms
│   ├── cli.py         # Command-line interface
│   └── app.py         # Flask web application
├── tests.py           # Unit tests (pytest or plain Python)
└── README.md
```

---

## Running Tests
```bash
# With pytest:
python -m pytest tests.py -v

# Without pytest:
python tests.py
```

---

## Theory Summary

### Conflict Serializability
Two operations **conflict** if they (1) belong to different transactions,
(2) access the same data item, and (3) at least one is a write-like op.

A schedule is **conflict-serializable** iff its **precedence graph** is acyclic.

### Recoverability Hierarchy
```
Rigorous ⊂ Strict ⊂ ACA ⊂ Recoverable
```

| Property | Rule |
|----------|------|
| Recoverable | Ti commits after all Tj it read from commit |
| ACA | Ti only reads from committed transactions |
| Strict | No R/W on X until last writer of X commits/aborts |
| Rigorous | No R/W on X until last accessor of X commits/aborts |

python src/cli.py --generate serializable     # guaranteed serializable
python src/cli.py --generate non_serializable # guaranteed cycle
python src/cli.py --generate random --txns 3 --items 3 --ops 4 --seed 42
python src/cli.py --generate serial