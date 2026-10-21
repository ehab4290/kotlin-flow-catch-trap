#!/usr/bin/env bash
# Runs the JVM checks behind the article, several rounds, and keeps each round's output so the rounds
# can be compared (median, lowest, highest) with aggregate.py.
#
#   ./run-jvm-suite.sh                         every check behind the article: 5 rounds, 500 runs per row, 4 at a time
#   ./run-jvm-suite.sh 3 300                   every check, 3 rounds, 300 runs per row
#   ./run-jvm-suite.sh 5 500 4 ways grid       only the checks you name
#   ./run-jvm-suite.sh 1 10000 1 retry         one check, many runs
#
# The checks:
#   android operators grid checks fixes extra ways msg placement cases collectorbug costs savepos snippet
#   notifystream oneach launchwork conflate latest whilesub statein handoff sharein retry buffer-between buffer-nowait buffer-ahead
# When no check is named, it also runs the checks that don't depend on timing, once: trace, shapes, circ,
# and the unit tests.
#
# Parallel: each check keeps 2 to 4 threads busy. On a 16-core machine, 4 at a time leaves cores free.
# Don't go much higher: the checks measure races between threads, and a check that waits for a free core
# changes who wins the race. On a machine with few cores, use 1.
#
# Results: results/<date>_<rounds>x<runs>/<check>/round-N.txt, plus progress.txt and machine.txt.
# progress.txt gets a line when the run starts, and when each check starts and finishes a round.
# To watch one check row by row: tail -f results/<that folder>/<check>/round-N.txt
# Then: python3 aggregate.py results/<that folder>

set -u
cd "$(dirname "$0")"

find_java() {
  if [ -n "${JAVA:-}" ]; then echo "$JAVA"; return; fi
  if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then echo "$JAVA_HOME/bin/java"; return; fi
  if /usr/libexec/java_home > /dev/null 2>&1; then echo "$(/usr/libexec/java_home)/bin/java"; return; fi
  for j in "/Applications/Android Studio.app/Contents/jbr/Contents/Home/bin/java" \
           "/Applications/IntelliJ IDEA.app/Contents/jbr/Contents/Home/bin/java" \
           "/Applications/IntelliJ IDEA CE.app/Contents/jbr/Contents/Home/bin/java"; do
    if [ -x "$j" ]; then echo "$j"; return; fi
  done
  echo java
}
JAVA=$(find_java)
CP="build/install/jvm-checks/lib/*"
P=catchtrap.standalone

main_class() {
  case $1 in
    android|operators|grid|checks|fixes|trace) echo "catchtrap.MainKt" ;;
    extra) echo "$P.extra.ExtraKt" ;;
    ways) echo "$P.ways.WaysKt" ;;
    msg) echo "$P.ways.MsgKt" ;;
    placement) echo "$P.placement.PlacementKt" ;;
    cases) echo "$P.cases.CasesKt" ;;
    collectorbug) echo "$P.collectorbug.CollectorBugKt" ;;
    costs) echo "$P.costs.CostsKt" ;;
    savepos) echo "$P.savepos.SavePositionKt" ;;
    shapes) echo "$P.shapes.ShapesKt" ;;
    circ) echo "$P.circ.CircKt" ;;
    conflate) echo "$P.conflate.ConfKt" ;;
    statein) echo "$P.statein.StateInCheckKt" ;;
    handoff) echo "$P.handoff.HandoffKt" ;;
    sharein) echo "$P.sharein.ShareInKt" ;;
    retry) echo "$P.retry.RetryCountKt" ;;
    latest) echo "$P.latest.LatestKt" ;;
    whilesub) echo "$P.whilesub.WhileSubscribedCheckKt" ;;
    notifystream) echo "$P.notifystream.NotifyStreamKt" ;;
    oneach) echo "$P.oneach.OnEachKt" ;;
    launchwork) echo "$P.launchwork.LaunchWorkKt" ;;
    buffer-between) echo "$P.buffer.between.BetweenKt" ;;
    buffer-nowait) echo "$P.buffer.nowait.NoWaitKt" ;;
    buffer-ahead) echo "$P.buffer.ahead.AheadKt" ;;
    *) echo "" ;;
  esac
}

# One check, one round. Called through xargs below.
if [ "${1:-}" = "--job" ]; then
  OUT=$2; RUNS=$3; p=$4; r=$5
  mkdir -p "$OUT/$p"; f="$OUT/$p/round-$r.txt"; t0=$(date +%s)
  echo "$(date +%H:%M:%S) round $r $p started" >> "$OUT/progress.txt"
  cls=$(main_class "$p")
  case $p in
    "") ;;
    android|operators|grid|checks|fixes|trace)
      "$JAVA" -cp "$CP" "$cls" "$p" "$RUNS" > "$f" 2>&1 ;;
    snippet)
      # The one-file snippet, 10 times, plus the fixed order.
      crash=0; fp=0; fixok=0
      for i in 1 2 3 4 5 6 7 8 9 10; do
        o=$("$JAVA" -cp "$CP" $P.snippet.CatchTrapSnippetKt 2>&1)
        echo "$o" | grep -q "^CRASH:" && crash=$((crash+1))
        echo "$o" | grep -q "Suppressed: kotlinx.coroutines.JobCancellationException" &&
          echo "$o" | grep -q "Caused by: \[CIRCULAR REFERENCE" && fp=$((fp+1))
        of=$("$JAVA" -cp "$CP" $P.snippetfix.CatchTrapSnippetFixKt 2>&1)
        echo "$of" | grep -q "^Logged:" && ! echo "$of" | grep -q "CRASH" && fixok=$((fixok+1))
      done
      echo "snippet: crashed $crash/10, trace had the Suppressed + CIRCULAR REFERENCE fingerprint $fp/10, fixed order logged the error without a crash $fixok/10" > "$f" ;;
    *)
      if [ -z "$cls" ]; then echo "unknown check: $p" > "$f"
      else RUNS=$RUNS "$JAVA" -cp "$CP" "$cls" > "$f" 2>&1; fi ;;
  esac
  echo "$(date +%H:%M:%S) round $r $p done in $(( $(date +%s) - t0 ))s" >> "$OUT/progress.txt"
  exit 0
fi

ROUNDS=${1:-5}; RUNS=${2:-500}; PARALLEL=${3:-4}
[ $# -ge 3 ] && shift 3 || shift $#
ALL="android operators grid checks fixes extra ways msg placement cases collectorbug costs savepos snippet"
ALL="$ALL notifystream oneach launchwork conflate latest whilesub statein handoff sharein retry buffer-between buffer-nowait buffer-ahead"
PROGRAMS=${*:-$ALL}

"$JAVA" -version > /dev/null 2>&1 || { echo "No Java found. Set JAVA_HOME or JAVA=/path/to/java (JDK 17 or later)"; exit 1; }
echo "Building..."
./gradlew -q installDist || exit 1

OUT="results/$(date +%Y-%m-%d_%H%M)_${ROUNDS}x${RUNS}"
mkdir -p "$OUT"
echo "Java: $("$JAVA" -version 2>&1 | head -1)" | tee "$OUT/machine.txt"
echo "OS: $(uname -sm)" | tee -a "$OUT/machine.txt"
echo "CPU cores: $(sysctl -n hw.ncpu 2>/dev/null || nproc)" | tee -a "$OUT/machine.txt"
echo "Checks: $PROGRAMS" | tee -a "$OUT/machine.txt"
echo "$(date +%H:%M:%S) started: $ROUNDS rounds, $RUNS runs per row, $PARALLEL at a time, checks: $PROGRAMS" > "$OUT/progress.txt"
echo "$ROUNDS rounds, $RUNS runs per row, $PARALLEL at a time. Started $(date +%H:%M). Progress: tail -f $OUT/progress.txt"

# Round 1 of every check first, then round 2, and so on, so an early stop still leaves whole rounds.
for r in $(seq 1 "$ROUNDS"); do
  for p in $PROGRAMS; do echo "$p $r"; done
done | xargs -P "$PARALLEL" -n 2 bash "$0" --job "$OUT" "$RUNS"

# The checks that don't depend on timing, once, after a full run.
if [ -z "${*:-}" ]; then
  mkdir -p "$OUT/once"
  "$JAVA" -cp "$CP" catchtrap.MainKt trace > "$OUT/once/trace.txt" 2>&1
  "$JAVA" -cp "$CP" $P.shapes.ShapesKt > "$OUT/once/shapes.txt" 2>&1
  "$JAVA" -cp "$CP" $P.circ.CircKt > "$OUT/once/circ.txt" 2>&1
  # -q prints nothing when the tests pass, so record the exit status too.
  if ./gradlew -q test > "$OUT/once/unittests.txt" 2>&1; then
    echo "unit tests: passed (exit 0)" >> "$OUT/once/unittests.txt"
  else
    echo "unit tests: FAILED (exit $?)" >> "$OUT/once/unittests.txt"
  fi
  tail -n 3 "$OUT/once/unittests.txt"
fi

echo "DONE" >> "$OUT/progress.txt"
echo "Done at $(date +%H:%M): $OUT"
echo "Medians: python3 aggregate.py $OUT"
