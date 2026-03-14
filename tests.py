"""
Unit tests for Transaction Scheduler Simulator
Covers all required properties with known textbook examples.
Run: python -m pytest tests.py -v
"""

import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from scheduler import (
    parse_schedule, analyze,
    analyze_serializability, analyze_recoverability,
    analyze_view_serializability,
    ViewSerializabilityResult,
    OpType, Operation, Schedule,
)


# ─────────────────────────────────────────────────────────────────────────────
# PARSER TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestParser:
    def test_basic_parse(self):
        s = parse_schedule("""
            START(T1)
            READ(T1,A)
            WRITE(T1,A)
            COMMIT(T1)
        """)
        assert len(s.operations) == 4
        assert s.operations[0].op_type == OpType.START
        assert s.operations[1].op_type == OpType.READ
        assert s.operations[1].item == "A"
        assert s.operations[2].op_type == OpType.WRITE
        assert s.operations[3].op_type == OpType.COMMIT

    def test_case_insensitive(self):
        s = parse_schedule("start(T1)\nread(t1,x)\ncommit(T1)")
        assert s.operations[0].op_type == OpType.START
        assert s.operations[1].op_type == OpType.READ

    def test_increment_decrement(self):
        s = parse_schedule("""
            START(T1)
            INCREMENT(T1,X)
            DECREMENT(T1,Y)
            COMMIT(T1)
        """)
        assert s.operations[1].op_type == OpType.INCREMENT
        assert s.operations[2].op_type == OpType.DECREMENT
        assert s.operations[1].is_write()
        assert s.operations[2].is_write()

    def test_abort(self):
        s = parse_schedule("START(T1)\nWRITE(T1,A)\nABORT(T1)")
        assert s.operations[2].op_type == OpType.ABORT

    def test_comma_separated(self):
        s = parse_schedule("START(T1),READ(T1,A),COMMIT(T1)")
        assert len(s.operations) == 3

    def test_invalid_token_raises(self):
        with pytest.raises(ValueError, match="Parse errors"):
            parse_schedule("START(T1)\nINVALID(T1)\nCOMMIT(T1)")

    def test_missing_start_raises(self):
        with pytest.raises(ValueError, match="without START"):
            parse_schedule("COMMIT(T1)")

    def test_op_after_commit_raises(self):
        with pytest.raises(ValueError, match="after COMMIT"):
            parse_schedule("START(T1)\nCOMMIT(T1)\nREAD(T1,A)")

    def test_multiple_start_raises(self):
        with pytest.raises(ValueError, match="multiple START"):
            parse_schedule("START(T1)\nSTART(T1)\nCOMMIT(T1)")


# ─────────────────────────────────────────────────────────────────────────────
# SERIALIZABILITY TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestSerializability:

    def test_serial_schedule_is_serializable(self):
        """A serial schedule T1 then T2 must be conflict-serializable."""
        s = parse_schedule("""
            START(T1)
            READ(T1,A)
            WRITE(T1,A)
            COMMIT(T1)
            START(T2)
            READ(T2,A)
            WRITE(T2,B)
            COMMIT(T2)
        """)
        r = analyze_serializability(s)
        assert r.is_serializable is True
        assert r.cycles == []

    def test_classic_non_serializable_cycle(self):
        """
        Classic non-serializable schedule:
        T1: R(A), W(B)   T2: R(B), W(A)
        Edges: T1→T2 (WR on A), T2→T1 (WR on B)  → cycle
        """
        s = parse_schedule("""
            START(T1)
            START(T2)
            READ(T1,A)
            READ(T2,B)
            WRITE(T2,A)
            WRITE(T1,B)
            COMMIT(T1)
            COMMIT(T2)
        """)
        r = analyze_serializability(s)
        assert r.is_serializable is False
        assert len(r.cycles) > 0

    def test_serial_order_single(self):
        """Single valid serial order should be returned."""
        s = parse_schedule("""
            START(T1)
            START(T2)
            READ(T1,A)
            WRITE(T1,A)
            COMMIT(T1)
            READ(T2,A)
            COMMIT(T2)
        """)
        r = analyze_serializability(s)
        assert r.is_serializable is True
        # T1 must come before T2 (T1 writes A, T2 reads A after)
        assert ["T1", "T2"] in r.serial_orders

    def test_no_conflicts_any_order(self):
        """Transactions on disjoint items → no edges → all orders valid."""
        s = parse_schedule("""
            START(T1)
            START(T2)
            WRITE(T1,A)
            WRITE(T2,B)
            COMMIT(T1)
            COMMIT(T2)
        """)
        r = analyze_serializability(s)
        assert r.is_serializable is True
        assert len(r.serial_orders) == 2  # both T1→T2 and T2→T1 valid

    def test_increment_treated_as_write(self):
        """INCREMENT/DECREMENT must generate conflict edges like WRITE."""
        s = parse_schedule("""
            START(T1)
            START(T2)
            READ(T1,X)
            INCREMENT(T2,X)
            WRITE(T1,X)
            COMMIT(T1)
            COMMIT(T2)
        """)
        r = analyze_serializability(s)
        # T2's INCREMENT on X after T1's READ → RW edge T1→T2
        # T1's WRITE on X after T2's INCREMENT → WW edge T2→T1  → cycle
        assert r.is_serializable is False

    def test_three_transactions_serializable(self):
        """Three transactions with consistent ordering."""
        s = parse_schedule("""
            START(T1)
            START(T2)
            START(T3)
            WRITE(T1,A)
            COMMIT(T1)
            WRITE(T2,A)
            COMMIT(T2)
            READ(T3,A)
            COMMIT(T3)
        """)
        r = analyze_serializability(s)
        assert r.is_serializable is True


# ─────────────────────────────────────────────────────────────────────────────
# RECOVERABILITY TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestRecoverability:

    def test_recoverable_schedule(self):
        """T2 reads from T1 and commits after T1."""
        s = parse_schedule("""
            START(T1)
            START(T2)
            WRITE(T1,A)
            READ(T2,A)
            COMMIT(T1)
            COMMIT(T2)
        """)
        r = analyze_recoverability(s)
        assert r.is_recoverable is True

    def test_non_recoverable_schedule(self):
        """T2 commits before T1 after reading T1's dirty write."""
        s = parse_schedule("""
            START(T1)
            START(T2)
            WRITE(T1,A)
            READ(T2,A)
            COMMIT(T2)
            COMMIT(T1)
        """)
        r = analyze_recoverability(s)
        assert r.is_recoverable is False

    def test_aca_violation(self):
        """T2 reads uncommitted data from T1 → violates ACA."""
        s = parse_schedule("""
            START(T1)
            START(T2)
            WRITE(T1,A)
            READ(T2,A)
            COMMIT(T1)
            COMMIT(T2)
        """)
        r = analyze_recoverability(s)
        assert r.is_recoverable is True
        assert r.is_aca is False   # T2 read before T1 committed

    def test_aca_satisfied(self):
        """T2 reads only after T1 commits."""
        s = parse_schedule("""
            START(T1)
            START(T2)
            WRITE(T1,A)
            COMMIT(T1)
            READ(T2,A)
            COMMIT(T2)
        """)
        r = analyze_recoverability(s)
        assert r.is_recoverable is True
        assert r.is_aca is True

    def test_strict_violation(self):
        """T2 writes item X before T1 (last writer) commits."""
        s = parse_schedule("""
            START(T1)
            START(T2)
            WRITE(T1,A)
            WRITE(T2,A)
            COMMIT(T1)
            COMMIT(T2)
        """)
        r = analyze_recoverability(s)
        assert r.is_strict is False

    def test_strict_satisfied(self):
        """T2 reads/writes only after T1 committed."""
        s = parse_schedule("""
            START(T1)
            START(T2)
            WRITE(T1,A)
            COMMIT(T1)
            READ(T2,A)
            WRITE(T2,A)
            COMMIT(T2)
        """)
        r = analyze_recoverability(s)
        assert r.is_strict is True

    def test_rigorous_violation(self):
        """T2 writes A while T1 (last reader) is still active."""
        s = parse_schedule("""
            START(T1)
            START(T2)
            READ(T1,A)
            WRITE(T2,A)
            COMMIT(T1)
            COMMIT(T2)
        """)
        r = analyze_recoverability(s)
        assert r.is_rigorous is False

    def test_rigorous_satisfied(self):
        """All accesses happen strictly after previous accessor commits."""
        s = parse_schedule("""
            START(T1)
            START(T2)
            READ(T1,A)
            COMMIT(T1)
            WRITE(T2,A)
            COMMIT(T2)
        """)
        r = analyze_recoverability(s)
        assert r.is_rigorous is True

    def test_hierarchy(self):
        """Rigorous ⊆ Strict ⊆ ACA ⊆ Recoverable."""
        # A rigorous schedule must satisfy all weaker properties
        s = parse_schedule("""
            START(T1)
            START(T2)
            WRITE(T1,A)
            COMMIT(T1)
            READ(T2,A)
            WRITE(T2,A)
            COMMIT(T2)
        """)
        r = analyze_recoverability(s)
        if r.is_rigorous:
            assert r.is_strict
        if r.is_strict:
            assert r.is_aca
        if r.is_aca:
            assert r.is_recoverable

    def test_abort_transaction(self):
        """Transaction that aborts should still affect strictness analysis."""
        s = parse_schedule("""
            START(T1)
            START(T2)
            WRITE(T1,A)
            ABORT(T1)
            WRITE(T2,A)
            COMMIT(T2)
        """)
        r = analyze_recoverability(s)
        # T2 writes A before T1 abort step is processed — strict violation
        assert r.is_strict is False


# ─────────────────────────────────────────────────────────────────────────────
# VIEW SERIALIZABILITY TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestViewSerializability:

    def test_serial_schedule_is_view_serializable(self):
        """A serial schedule is trivially view-serializable (view-equivalent to itself)."""
        s = parse_schedule("""
            START(T1)
            READ(T1,A)
            WRITE(T1,A)
            COMMIT(T1)
            START(T2)
            READ(T2,A)
            WRITE(T2,B)
            COMMIT(T2)
        """)
        r = analyze_view_serializability(s)
        assert r.is_view_serializable is True
        assert ["T1", "T2"] in r.equivalent_serial_orders

    def test_conflict_serializable_implies_view_serializable(self):
        """Every conflict-serializable schedule must also be view-serializable (CS ⊆ VS)."""
        s = parse_schedule("""
            START(T1)
            START(T2)
            READ(T1,A)
            WRITE(T1,A)
            COMMIT(T1)
            READ(T2,A)
            WRITE(T2,B)
            COMMIT(T2)
        """)
        cs = analyze_serializability(s)
        vs = analyze_view_serializability(s)
        assert cs.is_serializable is True
        assert vs.is_view_serializable is True

    def test_classic_blind_write_view_serializable_not_conflict(self):
        """
        Classic example: view-serializable but NOT conflict-serializable.

        Schedule: READ(T1,A), WRITE(T2,A), WRITE(T1,A), WRITE(T3,A)

        Conflict graph has cycle T1->T2->T1 (RW + WW) -> NOT conflict-serializable.

        View: T1 reads initial A; T3 makes the final write.
        Serial T1->T2->T3: T1 reads initial A (first), T3 writes last -> MATCH.
        => View-serializable with order T1->T2->T3.
        """
        s = parse_schedule("""
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
        """)
        cs = analyze_serializability(s)
        vs = analyze_view_serializability(s)
        assert cs.is_serializable is False
        assert vs.is_view_serializable is True
        assert ["T1", "T2", "T3"] in vs.equivalent_serial_orders

    def test_non_view_serializable_schedule(self):
        """
        Classic non-serializable cycle: neither conflict- nor view-serializable.

        Schedule: READ(T1,A), WRITE(T2,A), READ(T2,B), WRITE(T1,B)
        T1 must precede T2 (reads initial A) AND T2 must precede T1 (reads initial B).
        No serial order satisfies both -> NOT view-serializable.
        """
        s = parse_schedule("""
            START(T1)
            START(T2)
            READ(T1,A)
            WRITE(T2,A)
            READ(T2,B)
            WRITE(T1,B)
            COMMIT(T1)
            COMMIT(T2)
        """)
        vs = analyze_view_serializability(s)
        assert vs.is_view_serializable is False
        assert vs.equivalent_serial_orders == []

    def test_view_result_dataclass_fields(self):
        """ViewSerializabilityResult must have the expected fields and types."""
        s = parse_schedule("""
            START(T1)
            WRITE(T1,A)
            COMMIT(T1)
        """)
        r = analyze_view_serializability(s)
        assert isinstance(r, ViewSerializabilityResult)
        assert isinstance(r.is_view_serializable, bool)
        assert isinstance(r.equivalent_serial_orders, list)
        assert isinstance(r.explanation, list)
        assert len(r.explanation) > 0

    def test_analyze_report_includes_view_serializability(self):
        """Full analyze() must return an AnalysisReport with view_serializability field."""
        report = analyze("""
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
        """)
        assert hasattr(report, 'view_serializability')
        vs = report.view_serializability
        assert isinstance(vs, ViewSerializabilityResult)
        assert vs.is_view_serializable is True


# ─────────────────────────────────────────────────────────────────────────────
# INTEGRATION TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestIntegration:

    def test_full_analysis_non_serializable(self):
        report = analyze("""
            START(T1)
            START(T2)
            READ(T1,A)
            READ(T2,B)
            WRITE(T2,A)
            WRITE(T1,B)
            COMMIT(T1)
            COMMIT(T2)
        """)
        assert report.serializability.is_serializable is False
        assert report.recoverability.is_recoverable is True
        assert report.recoverability.is_aca is True

    def test_full_analysis_ideal_schedule(self):
        """Serial schedule → all properties satisfied."""
        report = analyze("""
            START(T1)
            READ(T1,A)
            WRITE(T1,A)
            COMMIT(T1)
            START(T2)
            READ(T2,A)
            WRITE(T2,B)
            COMMIT(T2)
        """)
        s  = report.serializability
        r  = report.recoverability
        vs = report.view_serializability
        assert s.is_serializable    is True
        assert r.is_recoverable     is True
        assert r.is_aca             is True
        assert r.is_strict          is True
        assert r.is_rigorous        is True
        assert vs.is_view_serializable is True
