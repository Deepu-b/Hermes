# Compaction Policy

Compaction is the process of reducing WAL size by snapshotting.

---

## Why Compaction Is Needed

Without compaction:
- WAL grows forever
- recovery time degrades
- disk usage becomes unbounded

---

## Stop-the-World Tradeoff

Hermes pauses writes during compaction.

This is intentional:
- simpler correctness model
- easier reasoning

Real systems often accept this tradeoff.

---

## When Compaction Should Run

Hermes supports time-based compaction.

In real systems, compaction may be triggered by:
- WAL size thresholds
- memory pressure
- restart frequency
- operator action

Hermes keeps policy simple by design.

---

## Failure Handling

If compaction fails:
- WAL remains intact
- correctness is preserved
- snapshot is discarded

WAL is always the source of truth.
