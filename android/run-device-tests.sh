#!/usr/bin/env bash
# Runs every on-device test behind the article, several rounds in a row, and keeps each round's
# results so the rounds can be compared (median, lowest, highest).
#
#   ./run-device-tests.sh            3 rounds, 100 runs per row
#   ./run-device-tests.sh 5 200      5 rounds, 200 runs per row
#   CLASS=me.elwan.catchtrap.LifecycleAwareOnDeviceTest ./run-device-tests.sh 3 50   one test class only
#
# Before you start: one phone or emulator connected (adb devices shows it), screen unlocked, and
# Developer options > Stay awake turned on. LifecycleAwareOnDeviceTest opens an activity, so the screen
# must stay on. With more than one device connected, choose one:
#   ANDROID_SERIAL=<serial from adb devices> ./run-device-tests.sh
#
# Results: device-results/<date>_<time>_<rounds>x<runs>/round-N.txt (the CatchTrap lines)
#          and round-N-tests.txt (which tests passed or failed).
set -euo pipefail

ROUNDS=${1:-3}
RUNS=${2:-100}
cd "$(dirname "$0")"

ADB=${ADB:-$(command -v adb || echo "$HOME/Library/Android/sdk/platform-tools/adb")}
OUT="device-results/$(date +%Y-%m-%d_%H%M)_${ROUNDS}x${RUNS}${CLASS:+_${CLASS##*.}}"
mkdir -p "$OUT"

"$ADB" get-state > /dev/null    # stops here if no device is connected
"$ADB" shell getprop ro.product.model > "$OUT/device.txt"
"$ADB" shell getprop ro.build.version.release >> "$OUT/device.txt"

echo "Building and installing the app and its tests..."
./gradlew -q :app:installDebug :app:installDebugAndroidTest

for r in $(seq 1 "$ROUNDS"); do
  echo "Round $r of $ROUNDS, $RUNS runs per row (started $(date +%H:%M))"
  START=$(date +%s)
  "$ADB" logcat -c
  "$ADB" logcat -v raw -s CatchTrap:I > "$OUT/round-$r.txt" &
  LOGCAT=$!
  # Each test class prints its name and one dot per finished test, so the progress shows as it runs.
  "$ADB" shell am instrument -w -e runs "$RUNS" ${CLASS:+-e class "$CLASS"} \
    me.elwan.catchtrap.test/androidx.test.runner.AndroidJUnitRunner | tee "$OUT/round-$r-tests.txt" || true
  sleep 2
  kill "$LOGCAT" 2> /dev/null || true
  wait "$LOGCAT" 2> /dev/null || true   # no "Terminated" message
  echo "Round $r took $(( ($(date +%s) - START) / 60 )) min. Counts: $OUT/round-$r.txt"
done

echo "Done: $OUT"
