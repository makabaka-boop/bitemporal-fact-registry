"""Naive history-snapshot oracle.

A deliberately simple, independent reference model of the same semantics the
database implements:

* The object's state at revision R is an immutable *snapshot*: a list of
  non-overlapping half-open segments ``[(from, to, value), ...]``.
* A correction (expected_revision == current head) replaces the snapshot with a
  brand-new list produced by splitting overlapping segments in pure Python.
* Old snapshots are never touched: querying revision R indexes straight into
  the frozen list ``history[R]``.

The FastAPI/PostgreSQL implementation must agree with this oracle after every
correction, at every (validAt, knownAtRevision) position. Two implementations
that share no code agreeing point-by-point is the check asked for.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

Segment = tuple[datetime, datetime, Any]


class NaiveBitemporalObject:
    def __init__(self) -> None:
        # history[0] is the empty world known before the first correction.
        self.history: list[list[Segment]] = [[]]

    @property
    def head(self) -> int:
        return len(self.history) - 1

    def correct(
        self,
        vfrom: datetime,
        vto: datetime,
        value: Any,
        expected_revision: int,
    ) -> int:
        if not vfrom < vto:
            raise ValueError("empty half-open interval")
        if expected_revision != self.head:
            raise RevisionConflict(expected_revision, self.head)

        new: list[Segment] = []
        for sf, st, sv in self.history[self.head]:
            if st <= vfrom or sf >= vto:
                new.append((sf, st, sv))           # untouched
                continue
            if sf < vfrom:
                new.append((sf, vfrom, sv))        # left remnant
            if vto < st:
                new.append((vto, st, sv))          # right remnant
            # the overlapping middle is gone in this snapshot
        new.append((vfrom, vto, value))
        new.sort(key=lambda seg: seg[0])
        # Assert the oracle's own no-overlap invariant.
        for (a_f, a_t, _), (b_f, b_t, _) in zip(new, new[1:]):
            assert a_t <= b_f, "oracle produced overlapping segments"
        self.history.append(new)
        return self.head

    def known_view(self, known_at_revision: int) -> list[Segment]:
        if not 0 <= known_at_revision <= self.head:
            raise IndexError("revision out of range")
        return self.history[known_at_revision]

    def fact_at(
        self, valid_at: datetime, known_at_revision: int
    ) -> Optional[Segment]:
        hits = [
            seg
            for seg in self.known_view(known_at_revision)
            if seg[0] <= valid_at < seg[1]
        ]
        assert len(hits) <= 1, "oracle produced two facts at one position"
        return hits[0] if hits else None


class RevisionConflict(Exception):
    def __init__(self, expected: int, actual: int):
        self.expected = expected
        self.actual = actual
        super().__init__(f"expected {expected}, head is {actual}")
