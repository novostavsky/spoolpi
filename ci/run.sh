#!/usr/bin/env bash
# Spool's CI, in one script. Locally it's run by hand, by git hooks and by a nightly
# systemd timer; on GitHub each workflow job will call one stage of it.
#
#   ci/run.sh [all]            lint, tests on 3.11/3.12/3.13, package, crash suites (~12 min)
#   ci/run.sh quick            lint + tests on 3.13 (~1.5 min; the pre-push hook)
#   ci/run.sh lint             ruff format --check, ruff check, mypy (the pre-commit hook)
#   ci/run.sh test [VERSION]   fast suite on one Python (default 3.13)
#   ci/run.sh package          build the wheel; install into a clean venv; zero-deps check
#   ci/run.sh crash            slow SIGKILL suites (SPOOL_CRASH_CYCLES, SPOOL_CRASH_SEED)
#   ci/run.sh nightly          crash suites at SPOOL_CRASH_CYCLES=10000 (~60 min)
#   ci/run.sh last             show the most recent run's summary
#   ci/run.sh install-nightly  install and start the systemd user timer (03:00 daily)
set -euo pipefail

export PATH="$HOME/.local/bin:$PATH"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CI="$REPO/.ci"
PYTHONS=(3.11 3.12 3.13)
MAIN_PY=3.13
cd "$REPO"

stage="${1:-all}"
shift || true

# --- bookkeeping ------------------------------------------------------------------------

if [[ "$stage" == last ]]; then
  if [[ -e "$CI/logs/last/summary.txt" ]]; then
    cat "$CI/logs/last/summary.txt"
    echo "logs: $(readlink -f "$CI/logs/last")"
    exit 0
  fi
  echo "no CI run recorded yet" >&2
  exit 1
fi

if [[ "$stage" == install-nightly ]]; then
  units="$HOME/.config/systemd/user"
  mkdir -p "$units"
  for f in spool-ci-nightly.service spool-ci-nightly.timer; do
    sed "s|@REPO@|$REPO|g" "$REPO/contrib/ci/$f" > "$units/$f"
  done
  systemctl --user daemon-reload
  systemctl --user enable --now spool-ci-nightly.timer
  systemctl --user list-timers spool-ci-nightly.timer --no-pager
  exit 0
fi

RUN="$CI/logs/$(date +%Y%m%d-%H%M%S)-$stage"
mkdir -p "$RUN"
ln -sfn "$RUN" "$CI/logs/last"
SUMMARY="$RUN/summary.txt"
echo "spool CI: $stage  ($(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo '?')$(git -C "$REPO" diff --quiet 2>/dev/null || echo ', uncommitted changes'))" | tee "$SUMMARY"

# step NAME CMD...: run CMD with output to $RUN/NAME.log; one line per step on screen.
step() {
  local name="$1"
  shift
  local log="$RUN/$name.log" start=$SECONDS
  printf '  %-22s ' "$name" | tee -a "$SUMMARY"
  if "$@" >"$log" 2>&1; then
    echo "ok    $((SECONDS - start))s" | tee -a "$SUMMARY"
  else
    echo "FAIL  $((SECONDS - start))s   (log: $log)" | tee -a "$SUMMARY"
    echo "---- last 40 lines of $name ----"
    tail -n 40 "$log"
    echo "RESULT: FAIL" | tee -a "$SUMMARY"
    exit 1
  fi
  # Crash suites print their seeds; keep them in the summary so failures can be replayed
  # (SPOOL_CRASH_SEED=<seed> ci/run.sh crash).
  grep -hE '^crash-seed |^seed=[0-9]+ cycles=|^distinct seqs=' "$log" 2>/dev/null | sed 's/^/      /' | tee -a "$SUMMARY" || true
}

# --- environments -------------------------------------------------------------------------

# A venv per Python version, pinned by uv.lock (fails if pyproject.toml wasn't re-locked).
venv_for() {
  local py="$1" venv="$CI/venvs/py$1"
  UV_PROJECT_ENVIRONMENT="$venv" uv sync --locked --extra dev --python "$py" --quiet
  echo "$venv"
}

# --- stages --------------------------------------------------------------------------------

do_lint() {
  local venv
  venv="$(venv_for "$MAIN_PY")"
  step ruff-format "$venv/bin/ruff" format --check src tests bench
  step ruff-check "$venv/bin/ruff" check src tests bench
  step mypy "$venv/bin/mypy" src/spool
}

do_test() {
  local py="${1:-$MAIN_PY}" venv
  venv="$(venv_for "$py")"
  step "tests-py$py" env SPOOL_REQUIRE_INTEGRATION=1 \
    "$venv/bin/python" -m pytest -q -p no:logging -p no:cacheprovider -rfE
}

package_check() {
  # Runs as an `if` condition inside step(), where bash ignores `set -e`,
  # hence the explicit `|| return 1` on every command.
  local dist="$CI/dist" clean="$CI/venvs/clean" tmp installed
  rm -rf "$dist" "$clean"
  uv build --quiet --out-dir "$dist" || return 1
  uv venv --quiet --python "$MAIN_PY" "$clean" || return 1
  uv pip install --quiet --python "$clean/bin/python" "$dist"/*.whl || return 1
  # Zero runtime dependencies: the clean venv must hold spool and nothing else.
  installed="$(uv pip list --python "$clean/bin/python" --format freeze | cut -d= -f1)" || return 1
  if [[ "$installed" != "spool" ]]; then
    echo "expected only spool in a clean install, found:"
    echo "$installed"
    return 1
  fi
  "$clean/bin/spool" --help >/dev/null || return 1
  tmp="$(mktemp -d)" || return 1
  printf '[buffer]\npath = "b.db"\n[retention]\npolicy = "drop_oldest"\nmax_rows = 1000\n[sink]\ntype = "jsonl"\npath = "out.jsonl"\n' >"$tmp/spool.toml" || return 1
  "$clean/bin/spool" check "$tmp/spool.toml" || return 1
  echo '{"sensor_id": "t1", "value": 1.5}' | "$clean/bin/spool" run "$tmp/spool.toml" || return 1
  grep -q '"sensor_id":"t1"' "$tmp/out.jsonl" || { echo "reading didn't reach the jsonl sink"; return 1; }
  rm -rf "$tmp"
}

do_package() {
  step package package_check
}

do_crash() {
  local venv
  venv="$(venv_for "$MAIN_PY")"
  step crash-suites env SPOOL_REQUIRE_INTEGRATION=1 \
    "$venv/bin/python" -m pytest -q -s -p no:logging -p no:cacheprovider -m slow -rfE
}

case "$stage" in
  lint) do_lint ;;
  test) do_test "${1:-$MAIN_PY}" ;;
  package) do_package ;;
  crash) do_crash ;;
  nightly)
    export SPOOL_CRASH_CYCLES="${SPOOL_CRASH_CYCLES:-10000}"
    echo "  (SPOOL_CRASH_CYCLES=$SPOOL_CRASH_CYCLES)" | tee -a "$SUMMARY"
    do_crash
    ;;
  quick)
    do_lint
    do_test "$MAIN_PY"
    ;;
  all)
    do_lint
    for py in "${PYTHONS[@]}"; do do_test "$py"; done
    do_package
    do_crash
    ;;
  *)
    sed -n '2,15p' "$0" >&2
    exit 2
    ;;
esac
echo "RESULT: ok" | tee -a "$SUMMARY"
