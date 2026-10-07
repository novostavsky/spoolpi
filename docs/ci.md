# CI

All of SpoolPi's CI is one script, `ci/run.sh`. Today it runs locally, in the Debian WSL2
distro that holds the repo. When the project moves to GitHub, each workflow job will call
one stage of the same script (see the end of this page), so nothing gets rewritten.

## Stages

```sh
ci/run.sh              # = all: lint, tests on 3.11/3.12/3.13, package, crash suites (~12 min)
ci/run.sh quick        # lint + tests on 3.13 (~1.5 min)
ci/run.sh lint         # ruff format --check, ruff check, mypy --strict on src/spoolpi
ci/run.sh test 3.11    # the fast suite on one Python version
ci/run.sh package      # build; twine check --strict; wheel contents; clean venv -> "only spoolpi installed" -> spoolpi check / run
ci/run.sh crash        # SIGKILL suites: 1,000 (buffer) / 100 (seq) / 300 (retention) cycles
ci/run.sh nightly      # crash suites at 10,000 cycles (~55 min, extrapolated from 2,000)
ci/run.sh last         # summary of the most recent run
```

- **Pinned tools.** Every stage installs from `uv.lock` with `uv sync --locked`. A change to
  `pyproject.toml` without `uv lock` fails CI instead of silently resolving new versions.
  Upgrading tools is a deliberate `uv lock --upgrade` commit. This matters because ruff's
  default rule set changes between versions.
- **Python versions.** Each version gets its own venv under `.ci/venvs/`. uv downloads the
  interpreters itself; no root needed.
- **No silent skips.** Stages set `SPOOLPI_REQUIRE_INTEGRATION=1`. With it, a test that would
  skip because mosquitto, PostgreSQL, openssl, unprivileged time namespaces or an optional
  package is missing *fails* instead (`tests/conftest.py`). A plain `pytest` on a machine
  without those tools still skips them.
- **Logs.** Each run writes one log per step under `.ci/logs/<time>-<stage>/`. The screen
  shows one line per step, plus the last 40 lines of a failing step.
- **Replaying a crash failure.** The summary records each crash suite's seed. Replay one with:

  ```sh
  SPOOLPI_CRASH_SEED=<seed> ci/run.sh crash
  ```

  `SPOOLPI_CRASH_CYCLES` changes the buffer suite's cycle count.
- **How the buffer crash suite checks.** After every cycle it checks only that cycle's rows,
  found through the `seq` index: gap-free, no committed reading lost, at most one batch of
  unreported ones. The full checks read the whole growing database: `integrity_check`, plus an
  audit that every earlier cycle still holds exactly its rows, once each.
  - **Up to 1,000 cycles** (every CI run), they also run after every cycle.
  - **Longer runs** do them every 100 cycles and after the last one. Otherwise the run is
    quadratic: 10,000 cycles took 7,042 s that way.
  - **Nothing escapes:** corruption and missing rows persist, so the next full check catches
    them. Replaying the seed finds the cycle that caused them.
  - The audit is itself tested with a planted missing row and a planted duplicate.

## What runs when

| When | What | How |
|---|---|---|
| every commit | `lint` (~10 s) | `.githooks/pre-commit` |
| every push | `quick` (~1.5 min) | `.githooks/pre-push`, active once a remote exists |
| before milestone commits | `all` (~12 min) | by hand |
| daily 03:00 | `nightly` (~55 min) | systemd user timer |

The hooks are enabled per clone with `git config core.hooksPath .githooks`. In an emergency,
`git commit --no-verify` skips them.

Install the nightly timer with `ci/run.sh install-nightly`. It uses `Persistent=true`: WSL only
runs while Windows is up and the distro is started, so a missed night runs at the next start.
It's "nightly when the machine is on", which is the honest limit of local CI and the main
reason to move to GitHub.

**Nothing notifies you when the nightly fails.** Check it with `ci/run.sh last`, or look under
`.ci/logs/*-nightly/`. The first two nightlies (09-28, 09-29) failed or were cut off, and it
went unnoticed for a day. The first green 10,000-cycle run was 09-29: seed 1125858416, 1,131,936
rows, 0 corruption, 0 lost commits.

## Test dependencies

The integration tests need a real MQTT broker and PostgreSQL. They look for them on `PATH`, then
in `/usr/lib/postgresql/*/bin` (the Debian/Ubuntu apt layout), then in the root-free copies
under `~/.local/mosquitto` and `~/.local/postgres`. Those copies were made with
`apt-get download` + `dpkg -x`, because `sudo` isn't available to the automation here.

Two probes also check the machine itself:
- the clock tests need unprivileged time namespaces (`unshare --user --time`);
- the HTTPS test needs `openssl`.

## Moving to GitHub Actions

The repo is a private GitHub repository (`novostavsky/spoolpi`, decided 2026-10-07), and the two
workflow files below are in `.github/workflows/`. The nightly runs weekly (Sunday 03:00 UTC).
Every job calls `ci/run.sh`, so the workflows only prepare the machine.

**`.github/workflows/ci.yml`**, on push and pull request:

| Job | Runs | Setup on `ubuntu-24.04` |
|---|---|---|
| lint | `ci/run.sh lint` | `astral-sh/setup-uv` |
| test (matrix 3.11, 3.12, 3.13) | `ci/run.sh test ${{ matrix.python }}` | setup-uv; `sudo apt-get install -y mosquitto postgresql`; `sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0` (Ubuntu 24.04 blocks the unprivileged user namespaces the time-namespace test needs) |
| package | `ci/run.sh package` | setup-uv |
| crash (needs test) | `ci/run.sh crash` | same as test |

**`.github/workflows/nightly.yml`:** `schedule: cron "0 3 * * *"` plus `workflow_dispatch`,
running `ci/run.sh nightly` with the test job's setup and `timeout-minutes: 120`. On failure,
upload `.ci/logs/` as an artifact; the crash seeds are in `summary.txt`.

**Minutes.** Public repositories get unlimited Actions minutes. On a private repository's free
plan (2,000 min/month), a push costs ~17 min and a nightly ~60 min, so the long run would move
to a weekly schedule.

**Later:**
- Publishing: a tag-triggered workflow that uploads to PyPI with trusted publishing, so no
  token is stored.
- The Raspberry Pi: a self-hosted ARM runner for the hardware benchmarks (fsync latency, RSS,
  NTP cold boot). Trigger it manually only: never let untrusted pull requests run on hardware
  you own.
