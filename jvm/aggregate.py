"""Median across rounds, per number, keeping each report's own layout.

usage: python3 aggregate.py <suite-output-dir> [<more dirs>...] > report.txt

Every line of round-1 is printed with each number replaced by:
  N              when all rounds agree
  med N [lo-hi]  when they differ (median, lowest, highest across rounds)
Lines are matched across rounds by their text with the numbers blanked out (plus how many times that
text appeared before), so a line that only exists in some rounds is listed separately.
"""
import glob
import os
import re
import statistics
import sys

NUM = re.compile(r"\d+")
SKIP = re.compile(r"^\s+(at |\.\.\.)|@[0-9a-f]{6,}")


def keyed(lines):
    seen, out = {}, []
    for line in lines:
        if SKIP.search(line):
            continue
        tmpl = re.sub(r"\s+", " ", NUM.sub("#", line))
        n = seen.get(tmpl, 0)
        seen[tmpl] = n + 1
        out.append(((tmpl, n), [int(x) for x in NUM.findall(line)], line))
    return out


def fmt(values):
    if len(set(values)) == 1:
        return str(values[0])
    med = statistics.median(values)
    med = int(med) if med == int(med) else med
    return f"med {med} [{min(values)}-{max(values)}]"


def report(prog_dir):
    files = sorted(glob.glob(os.path.join(prog_dir, "round-*.txt")),
                   key=lambda f: int(re.search(r"round-(\d+)", f).group(1)))
    files = [f for f in files if not f.endswith("-tests.txt")]
    prog_file = os.path.join(os.path.dirname(prog_dir), "progress.txt")
    if os.path.exists(prog_file):  # only rounds the suite logged as finished
        # "round N check done in Xs" (older runs: "round N check Xs"); "started" lines don't count
        done = set(re.findall(r"round (\d+) (\S+) (?:done in )?\d+s", open(prog_file).read()))
        name = os.path.basename(prog_dir)
        files = [f for f in files if (re.search(r"round-(\d+)", f).group(1), name) in done]
    if not files:
        return
    rounds = [keyed(open(f, errors="replace").read().splitlines()) for f in files]
    maps = [{k: nums for k, nums, _ in r} for r in rounds]
    print(f"\n######## {os.path.basename(prog_dir)}  ({len(files)} rounds)")
    for k, nums, line in rounds[0]:
        if not all(k in m for m in maps):
            print(f"  [only in some rounds] {line}")
            continue
        cols = [[m[k][i] for m in maps] for i in range(len(nums))]
        parts = NUM.split(line)
        out = parts[0]
        for i, col in enumerate(cols):
            out += fmt(col) + parts[i + 1]
        print(out)
    for i, r in enumerate(rounds[1:], start=2):
        for k, _, line in r:
            if k not in maps[0]:
                print(f"  [only in round {i}] {line}")


for root in sys.argv[1:]:
    for d in sorted(glob.glob(os.path.join(root, "*"))):
        if os.path.isdir(d) and os.path.basename(d) != "once":
            report(d)
