# Motivation: measuring the naive approach

The Week 0 spike: the experiment that came before the design. Environment: Debian 13 (trixie) under
WSL2, native ext4 filesystem, Python 3.13.5, SQLite bundled with CPython.

## The naive implementation

`bench/naive_spike.py` — the implementation most people write first:

- A loop generates a deterministic reading (`step=n`, `value=float(n)`).
- Each reading is inserted into SQLite (default pragmas: `journal_mode=DELETE`,
  `synchronous=FULL`) and POSTed to a local HTTP endpoint **immediately**, before commit.
- Commits are batched every 15 rows — a realistic "optimization" once a beginner notices
  fsync-per-row is slow, without realizing what it does to durability.
- On restart, the loop resumes from `MAX(step)` in the committed DB.

## The crash harness

`tests/harness/crash.py` (`run_until_killed` / `run_crash_cycles`) launches the target as a
subprocess, sleeps a random duration, then `SIGKILL`s it — no clean shutdown, no signal
handling. `bench/run_naive_spike_crash_test.py` drives 100 such cycles against the same DB and
events file (simulating one long-lived deployment that keeps losing power), then compares:

- every `step` committed in the SQLite DB, against
- every `step` that arrived at the HTTP sink (recorded to `events.jsonl`)

A **lost** reading is committed but never delivered. A **duplicated** reading is delivered more
than once. **Corruption** is any `PRAGMA integrity_check` failure on the DB file.

## Results (100 cycles, seed=0)

| Metric | Value |
|---|---|
| Total committed readings | 5,596 |
| Lost | 0 (0.0%) |
| **Duplicated** | **734 (13.1%)** |
| Corrupted DB files | 0 |

## Reading the result

The numbers are not boring. Loss came out at zero and corruption at zero — SQLite's rollback
journal held up cleanly under repeated `SIGKILL` on this filesystem, and posting before commit
means an in-flight reading is never silently dropped. But that same ordering is exactly why
duplication is high: any reading POSTed in the batch window before the next commit gets rolled
back on crash, then re-read and re-POSTed after restart. **13.1% of all readings shipped
downstream were shipped at least twice**, silently, with no signal to the receiver that this
happened. A downstream consumer with no dedupe key would double-count for over one in eight
data points — the kind of bug that doesn't crash anything and doesn't show up until someone
audits the numbers months later.

This is the core problem statement: storage durability and delivery state are two different
facts, and treating either one as a proxy for the other produces a real, silent, high-rate
defect. That's the number the rest of the project is measured against — the identity/shipper
design (the sequence-number identity and the shipper) exists specifically to make "committed" and "delivered" two explicitly tracked
states instead of one inferred from the other.

## Rerun with SpoolPi (2026-09-27)

The same experiment, with SpoolPi in place of the naive loop:
- `bench/spoolpi_spike.py` runs the same fake sensor and the same 10 ms cadence, with the same
  resume rule (continue from the last committed step + 1). It commits in batches of 15 like the
  naive spike, and ships through SpoolPi's HTTP sink to a receiver that stays up across the kills.
- `bench/run_spoolpi_spike_crash_test.py` uses the same kill schedule: 100 SIGKILLs at seeded
  uniform 0.05–1.5 s. After the last kill, one uninterrupted `--drain` run lets everything
  committed reach the receiver.
- The naive spike was re-run on the same day, machine and seeds.

| Seed | Naive: committed | Naive: duplicated | SpoolPi: committed | SpoolPi: redelivered (same key) | SpoolPi: duplicates after dedupe |
|---|---|---|---|---|---|
| 0 | 6,496 | 706 (10.9%) | 7,253 | 15 | **0** |
| 1 | 5,656 | 717 (12.7%) | 6,201 | 31 | **0** |
| 2 | 6,166 | 686 (11.1%) | 6,819 | 1 | **0** |
| **Total** | **18,318** | **2,109 (11.5%)** | **20,273** | **47 (0.23%)** | **0** |

Neither design lost a committed reading or corrupted its database in any run. (The first
Week 0 run above measured 13.1% for the naive spike; today's three seeds put it at 10.9–12.7%.)

The difference is in what reaches the receiver:

- **Naive:** 11.5% of readings arrived twice, and nothing in the payload tells the receiver
  that the second copy is a repeat. The synthetic stream happens to count 0, 1, 2…, but a real
  temperature reading has no such key. These duplicates are *silent*.
- **SpoolPi:** 0.23% of records arrived more than once. That's at-least-once delivery doing its
  job: the process died after the receiver stored a batch but before the acknowledgement was
  recorded. Every repeat carries the same `(buffer_id, seq)` key, so a consumer removes it
  mechanically (the reference consumer's `ON CONFLICT DO NOTHING`). After that, **0
  duplicates and 0 losses**. No reading was ever shipped under two different keys, which is
  the kind of duplicate no consumer could detect.

SpoolPi's redelivery rate depends on how often a kill lands between "the receiver has it" and
"the ack is recorded". Here that window is small, because the receiver is local. A slow uplink
widens it, and raises the redelivery count, but never the post-dedupe count.

What neither design can do is keep readings that were sampled but not yet committed when the
process died. In both, that window is at most one commit batch (15 readings here). SpoolPi
documents it as its loss bound, and the buffer crash test checks it across 1,000 kills.

## Caveats

These experiments ran on WSL2, whose ext4 sits on a virtual disk backed by the host's NVMe SSD.
They show that this crash mechanism (`SIGKILL` on a fast disk) doesn't corrupt SQLite. They
don't show how an SD card behaves under power loss, or how many readings a cold boot stamps
wrongly. Both were measured afterwards on a Raspberry Pi Zero 2 W
([`hardware.md`](hardware.md)):
- **SD-card commit latency:** 3.8 ms per default commit (13 ms with the power-safe default), so
  the batch defaults stand.
- **Power cuts:** 25 simulated cuts and 5 real plug pulls lost no committed readings with
  `durability = "power"`.
- **A real NTP step after a cold boot:** 30 days, corrected to 3 µs. With the unsynced-reading
  hold, no pre-sync reading shipped with the wrong time.
