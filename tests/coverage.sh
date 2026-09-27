#!/usr/bin/env bash
# tests/coverage.sh — the measured line coverage over `src/`, merged
# across the suites.
#
# `novo test --cov` measures one suite file at a time, and a suite's
# percentage counts the suite's own lines as well as the package's.  No
# single number it prints is the one `docs/publishing.md`
# § Test coverage asks for.  This merges the per-suite LCOV and reports
# this package's own `src/` alone, which is what a release is measured
# on; a dependency's sources, compiled in beside it, are left out.
#
# A line under a `// cov: skip — <reason>` marker is excused, together
# with the lines indented beneath it: the marker sits directly above the
# region it names, as docs/publishing.md § Test coverage describes.  A
# marker over a line that is in fact covered fails the run.
#
# Run from anywhere:  bash tests/coverage.sh
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG="$(cd "$HERE/.." && pwd)"
NOVO="${NOVO:-$HOME/.novo/bin/novo}"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/arrow-nv-cov.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

for suite in "$PKG"/tests/*_tests.nv; do
  base="$(basename "$suite" .nv)"
  ( cd "$PKG" && NOVO_LEAK_CHECK=0 timeout 1800 "$NOVO" test "tests/$base.nv" \
      --cov --report=lcov ) >"$WORK/$base.log" 2>&1
  if [ -f "$PKG/_novo/$base.lcov.info" ]; then
    cp "$PKG/_novo/$base.lcov.info" "$WORK/"
  else
    echo "  ✗ $base — no LCOV written; see $WORK/$base.log"
    exit 1
  fi
done

python3 - "$WORK" "$PKG" <<'PY'
import collections, glob, os, sys

work = sys.argv[1]
src_dir = os.path.join(sys.argv[2], 'src') + os.sep
cov = collections.defaultdict(dict)
for path in glob.glob(os.path.join(work, '*.lcov.info')):
    current = None
    for line in open(path):
        line = line.strip()
        if line.startswith('SF:'):
            current = line[3:]
        elif line.startswith('DA:'):
            n, hits = line[3:].split(',')[:2]
            n, hits = int(n), int(hits)
            cov[current][n] = cov[current].get(n, 0) + hits

def excused(path):
    out = set()
    try:
        lines = open(path).read().split('\n')
    except OSError:
        return out
    for i, text in enumerate(lines):
        if text.strip().startswith('// cov: skip'):
            j = i + 1
            if j >= len(lines):
                continue
            indent = len(lines[j]) - len(lines[j].lstrip())
            out.add(j + 1)
            k = j + 1
            while k < len(lines) and (lines[k].strip() == '' or
                                      len(lines[k]) - len(lines[k].lstrip()) > indent):
                out.add(k + 1)
                k += 1
    return out

total = covered = 0
bad_marker = False
print('')
for path in sorted(cov):
    if not os.path.abspath(path).startswith(src_dir):
        continue
    skip = excused(path)
    for n in sorted(skip):
        if cov[path].get(n, 0) > 0:
            print('  ✗ %s:%d is marked cov: skip but is covered' % (os.path.basename(path), n))
            bad_marker = True
    lines = {n: v for n, v in cov[path].items() if n not in skip}
    hit = sum(1 for v in lines.values() if v > 0)
    missed = sorted(n for n, v in lines.items() if v == 0)
    total += len(lines)
    covered += hit
    print('  %-20s %3d/%-3d  %s' % (
        os.path.basename(path), hit, len(lines),
        'uncovered: ' + ' '.join(str(n) for n in missed) if missed else '100%'))
pct = 100.0 * covered / total if total else 0.0
print('')
print('  src/ total: %d/%d lines — %.1f%%' % (covered, total, pct))
sys.exit(0 if covered == total and not bad_marker else 1)
PY
