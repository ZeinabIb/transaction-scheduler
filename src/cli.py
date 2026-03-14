"""
Command-Line Interface for the Transaction Scheduler Simulator
Usage:
    python cli.py                          # interactive prompt
    python cli.py --file schedule.txt      # analyse a file
    python cli.py --demo                   # run built-in demo schedules
    python cli.py --generate serializable  # generate a random schedule
"""

import argparse
import sys
import textwrap
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from scheduler import analyze, AnalysisReport, parse_schedule, Schedule
from explainer import build_trace
from locking import simulate_2pl
from generator import (
    GeneratorConfig, generate_random, generate_serial,
    generate_serializable, generate_non_serializable, schedule_to_text,
)


# ─────────────────────────────────────────────────────────────────────────────
# FORMATTING HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _sep(char: str = "─", width: int = 70) -> str:
    return char * width

def _tick(val: bool) -> str:
    return "✓  YES" if val else "✗  NO"


def print_report(report: AnalysisReport,
                 show_trace: bool = True,
                 show_locking: bool = True) -> None:
    s   = report.schedule
    ser = report.serializability
    rec = report.recoverability

    print(_sep("═"))
    print("  TRANSACTION SCHEDULER SIMULATOR  —  Analysis Report")
    print(_sep("═"))

    print(f"\nSchedule")
    print(f"  Transactions : {', '.join(s.transactions)}")
    print(f"  Operations   : {len(s.operations)}")
    print(f"  Step-by-step :")
    for op in s.operations:
        print(f"    {op.step:>3}.  {op}")

    print(f"\n{_sep()}")
    print("SERIALIZABILITY (Conflict Equivalence)")
    print(_sep())
    for line in ser.explanation:
        print(line)
    print(f"\n  Result  →  Conflict-Serializable:  {_tick(ser.is_serializable)}")
    if ser.is_serializable and ser.serial_orders:
        orders_str = " | ".join(" → ".join(o) for o in ser.serial_orders)
        print(f"  Equivalent serial order(s): {orders_str}")
    if ser.cycles:
        for cyc in ser.cycles:
            print(f"  Cycle: {' → '.join(cyc)}")

    print(f"\n{_sep()}")
    print("RECOVERABILITY PROPERTIES")
    print(_sep())
    for line in rec.explanation:
        print(line)
    print(f"\n  Summary:")
    print(f"    Recoverable  : {_tick(rec.is_recoverable)}")
    print(f"    ACA          : {_tick(rec.is_aca)}")
    print(f"    Strict       : {_tick(rec.is_strict)}")
    print(f"    Rigorous     : {_tick(rec.is_rigorous)}")
    print(f"\n  (Hierarchy: Rigorous ⊂ Strict ⊂ ACA ⊂ Recoverable)")

    # ── BONUS: VIEW SERIALIZABILITY ────────────────────────────────────────────
    vser = report.view_serializability
    print(f"\n{_sep()}")
    print("BONUS: VIEW SERIALIZABILITY  (additional analysis — beyond required)")
    print(_sep())
    print("  Note: The required serializability determination above uses ONLY")
    print("  conflict equivalence (precedence graph). View equivalence is provided")
    print("  here as an extra analysis and is NOT used for the conflict-serializable result.")
    print()
    for line in vser.explanation:
        print(line)
    print(f"\n  Result  →  View-Serializable: {_tick(vser.is_view_serializable)}")
    if vser.is_view_serializable and vser.equivalent_serial_orders:
        orders_str = " | ".join(" → ".join(o) for o in vser.equivalent_serial_orders)
        print(f"  View-equivalent serial order(s): {orders_str}")
    print(f"\n  (View Serializable ⊇ Conflict Serializable: every CS schedule is also VS)")

    # ── BONUS 1: Step-by-step trace ───────────────────────────────────────────
    if show_trace:
        print(f"\n{_sep()}")
        print("BONUS: STEP-BY-STEP VIOLATION TRACE")
        print(_sep())
        trace = build_trace(s)
        print(trace.to_text())

    # ── BONUS 2: 2PL / Strict 2PL ─────────────────────────────────────────────
    if show_locking:
        print(f"\n{_sep()}")
        print("BONUS: 2PL / STRICT 2PL LOCKING ANALYSIS")
        print(_sep())
        lock_result = simulate_2pl(s)
        print(lock_result.to_text())

    print(_sep("═"))


# ─────────────────────────────────────────────────────────────────────────────
# DEMO SCHEDULES
# ─────────────────────────────────────────────────────────────────────────────

DEMOS = {
    "1 - Classic serializable, strict, rigorous": """
START(T1)
START(T2)
READ(T1,A)
WRITE(T1,A)
COMMIT(T1)
READ(T2,A)
WRITE(T2,B)
COMMIT(T2)
""",
    "2 - Non-serializable (cycle T1->T2->T1)": """
START(T1)
START(T2)
READ(T1,A)
READ(T2,B)
WRITE(T2,A)
WRITE(T1,B)
COMMIT(T1)
COMMIT(T2)
""",
    "3 - Recoverable but NOT ACA (dirty read)": """
START(T1)
START(T2)
WRITE(T1,A)
READ(T2,A)
COMMIT(T1)
COMMIT(T2)
""",
    "4 - Non-recoverable (T2 commits before T1)": """
START(T1)
START(T2)
WRITE(T1,A)
READ(T2,A)
COMMIT(T2)
COMMIT(T1)
""",
    "5 - Strict but not rigorous": """
START(T1)
START(T2)
WRITE(T1,A)
COMMIT(T1)
READ(T2,A)
WRITE(T2,A)
COMMIT(T2)
""",
    "6 - INCREMENT / DECREMENT treated as writes": """
START(T1)
START(T2)
READ(T1,X)
INCREMENT(T2,X)
WRITE(T1,X)
COMMIT(T1)
COMMIT(T2)
""",
    "7 - BONUS: View-serializable but NOT conflict-serializable (blind write)": """
START(T1)
START(T2)
START(T3)
READ(T1,A)
WRITE(T2,A)
WRITE(T1,A)
WRITE(T3,A)
COMMIT(T1)
COMMIT(T2)
COMMIT(T3)
""",
}


def run_demos(show_trace=True, show_locking=True) -> None:
    for name, sched_text in DEMOS.items():
        print(f"\n{'#'*70}")
        print(f"  DEMO: {name}")
        print(f"{'#'*70}")
        try:
            report = analyze(sched_text)
            print_report(report, show_trace=show_trace, show_locking=show_locking)
        except ValueError as e:
            print(f"  ERROR: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# BONUS: SCHEDULE GENERATION
# ─────────────────────────────────────────────────────────────────────────────

def run_generate(gen_type: str, txns: int, items: int, ops: int,
                 seed, analyze_after: bool,
                 show_trace: bool, show_locking: bool) -> None:
    cfg = GeneratorConfig(
        num_transactions=txns,
        num_items=items,
        ops_per_tx=ops,
        seed=seed,
    )
    fn_map = {
        'random':           generate_random,
        'serial':           generate_serial,
        'serializable':     generate_serializable,
        'non_serializable': generate_non_serializable,
    }
    schedule = fn_map[gen_type](cfg)
    text     = schedule_to_text(schedule)

    print(f"\n{'='*70}")
    print(f"  BONUS: Schedule Generator  [type={gen_type}  txns={txns}  items={items}  ops/tx={ops}]")
    print(f"{'='*70}\n")
    print(text)

    if analyze_after:
        print()
        try:
            report = analyze(text)
            print_report(report, show_trace=show_trace, show_locking=show_locking)
        except ValueError as e:
            print(f"  ERROR: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# INTERACTIVE LOOP
# ─────────────────────────────────────────────────────────────────────────────

_HELP = textwrap.dedent("""\
    Enter a schedule, one operation per line (or comma/semicolon-separated).
    Supported operations:
      START(Ti)  COMMIT(Ti)  ABORT(Ti)
      READ(Ti,X)  WRITE(Ti,X)  INCREMENT(Ti,X)  DECREMENT(Ti,X)

    Commands:
      DONE / blank line   Analyse the entered schedule
      DEMO                Run all built-in demo schedules
      GENERATE            Generate & analyse a random schedule
      TRACE ON/OFF        Toggle step-by-step explanation
      LOCK  ON/OFF        Toggle 2PL analysis
      QUIT                Exit
""")


def interactive_loop(show_trace: bool = True, show_locking: bool = True) -> None:
    print("========================================================")
    print("  Transaction Scheduling & Serializability Analyzer v2")
    print("  Bonus: Step-by-step trace | 2PL | Schedule generator")
    print("========================================================")
    print(_HELP)

    while True:
        trace_lbl = "ON" if show_trace   else "OFF"
        lock_lbl  = "ON" if show_locking else "OFF"
        print(f"\n[trace:{trace_lbl}  2pl:{lock_lbl}]  Enter schedule:")
        lines = []

        while True:
            try:
                line = input("  > ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nGoodbye.")
                return

            upper = line.upper()
            if upper == "QUIT":
                print("Goodbye."); return
            if upper == "DEMO":
                run_demos(show_trace=show_trace, show_locking=show_locking); break
            if upper == "GENERATE":
                run_generate('random', 2, 2, 3, None, True, show_trace, show_locking); break
            if upper == "TRACE ON":  show_trace   = True;  print("  Trace: ON");   continue
            if upper == "TRACE OFF": show_trace   = False; print("  Trace: OFF");  continue
            if upper == "LOCK ON":   show_locking = True;  print("  2PL: ON");     continue
            if upper == "LOCK OFF":  show_locking = False; print("  2PL: OFF");    continue
            if upper in ("DONE", "") and lines:
                try:
                    report = analyze("\n".join(lines))
                    print_report(report, show_trace=show_trace, show_locking=show_locking)
                except ValueError as e:
                    print(f"\n  ERROR: {e}")
                break
            if line:
                lines.append(line)


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Transaction Scheduler Simulator (with bonus features)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--file",     "-f", help="Path to a schedule text file")
    parser.add_argument("--demo",     "-d", action="store_true")
    parser.add_argument("--generate", "-g",
                        choices=["random","serial","serializable","non_serializable"])
    parser.add_argument("--txns",  type=int, default=2)
    parser.add_argument("--items", type=int, default=2)
    parser.add_argument("--ops",   type=int, default=3)
    parser.add_argument("--seed",  type=int, default=None)
    parser.add_argument("--no-trace",   action="store_true")
    parser.add_argument("--no-locking", action="store_true")
    args = parser.parse_args()

    show_trace   = not args.no_trace
    show_locking = not args.no_locking

    if args.generate:
        run_generate(args.generate, args.txns, args.items, args.ops,
                     args.seed, True, show_trace, show_locking)
    elif args.demo:
        run_demos(show_trace=show_trace, show_locking=show_locking)
    elif args.file:
        text = Path(args.file).read_text()
        try:
            report = analyze(text)
            print_report(report, show_trace=show_trace, show_locking=show_locking)
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        interactive_loop(show_trace=show_trace, show_locking=show_locking)


if __name__ == "__main__":
    main()
