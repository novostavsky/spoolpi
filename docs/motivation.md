# Motivation: measuring the naive approach

Week 0 spike, per `spool_implementation-plan.md` §1. Environment: Debian 13 (trixie) under
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
design (M3/M5) exists specifically to make "committed" and "delivered" two explicitly tracked
states instead of one inferred from the other.

## Caveats / not yet measured

Two Week 0 outputs from the plan are **not** in this report and need real Raspberry Pi
hardware, not WSL2:

- **Fsync latency on a real SD card.** WSL2's ext4 runs on a virtual disk backed by the Windows
  host's NVMe/SSD, which is nothing like SD card write latency. The measured 0% loss/corruption
  rate here should not be read as "SQLite is safe on the target hardware" — only that this
  particular crash mechanism (`SIGKILL` on a fast disk) doesn't corrupt it. The batch-size
  default (plan §1) still needs a real SD card measurement.
- **NTP clock-step / 1970-timestamp count.** Requires booting a Pi with no network, sampling,
  then reconnecting and observing the step. Not reproducible in a WSL2 VM, which shares the
  Windows host clock and doesn't independently run `systemd-timesyncd` against a cold start.

Both are tracked as follow-ups before the M1 clock-anchor default is finalized.
