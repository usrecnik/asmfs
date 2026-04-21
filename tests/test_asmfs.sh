#!/usr/bin/env bash
#
# test_asmfs.sh — integration test suite for ASMFS (read-only FUSE filesystem
# exposing Oracle ASM files).
#

set -u

usage() {
  cat <<'EOF'
Usage: test_asmfs.sh <ASM_PATH> [MOUNTPOINT] [TMPDIR]

Integration test suite for the ASMFS FUSE filesystem. Runs a battery of
read tests against files exposed through the FUSE mount, cross-checking
results against `asmcmd cp` output.

Arguments:
  ASM_PATH     ASM path to test, at any level of specificity:
                 +DATA
                 +DATA/MYDB
                 +DATA/MYDB/DATAFILE
                 +DATA/MYDB/DATAFILE/system.256.1234567890
               If it resolves to a directory on the FUSE mount, all files
               under it are tested recursively. The leading `+` is stripped
               and the remainder is mapped under MOUNTPOINT.
  MOUNTPOINT   Where asmfs is mounted (default: /mnt/asm).
  TMPDIR       Scratch directory (default: /tmp/asm_tests). Its ./asmfs and
               ./orig subfolders are wiped on start and on exit.

Environment:
  RUN_TESTS    Space-separated list of test numbers to run, e.g. "1 2 3".
               Defaults to all tests.

Tests:
  1 basic_copy        cp(FUSE) vs `asmcmd cp`, full-file checksum match.
  2 dd_block_aligned  dd bs=512 vs dd bs=4096 on FUSE, checksum match.
  3 dd_partial_read   dd skip=N count=M on FUSE vs the same region extracted
                      from an `asmcmd cp` of the full file.
  4 rsync_read        rsync(FUSE) vs `asmcmd cp`, plus a rsync no-op check on
                      a second run (exercises stable FUSE getattr/stat).

Exit codes:
  0  all tests passed or skipped
  1  one or more tests failed
  2  invalid arguments / usage
  3  no checksum tool available (sha256sum or md5sum)
  4  resolved path not found under the FUSE mount
EOF
}

case "${1:-}" in
  -h|--help) usage; exit 0 ;;
esac

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 2
fi

ASM_ARG="$1"
MOUNTPOINT="${2:-/mnt/asm}"
TMPDIR_ARG="${3:-/tmp/asm_tests}"

MOUNTPOINT="${MOUNTPOINT%/}"
TMPDIR_ARG="${TMPDIR_ARG%/}"

# Resolve FUSE path from ASM path.
if [[ "$ASM_ARG" == /* ]]; then
  FUSE_ROOT="$ASM_ARG"
else
  NORM="${ASM_ARG#+}"
  NORM="${NORM%/}"
  FUSE_ROOT="$MOUNTPOINT/$NORM"
fi
FUSE_ROOT="${FUSE_ROOT%/}"

# Pick checksum command.
if command -v sha256sum >/dev/null 2>&1; then
  CKSUM=sha256sum
elif command -v md5sum >/dev/null 2>&1; then
  CKSUM=md5sum
  echo "WARN: sha256sum not found, falling back to md5sum" >&2
else
  echo "ERROR: neither sha256sum nor md5sum found in PATH" >&2
  exit 3
fi

# Feature detection.
HAS_ASMCMD=0; command -v asmcmd >/dev/null 2>&1 && HAS_ASMCMD=1
HAS_RSYNC=0;  command -v rsync  >/dev/null 2>&1 && HAS_RSYNC=1
[[ $HAS_ASMCMD -eq 0 ]] && echo "WARN: asmcmd not in PATH — asmcmd-dependent tests will be skipped" >&2
[[ $HAS_RSYNC  -eq 0 ]] && echo "WARN: rsync not in PATH — rsync tests will be skipped" >&2

# Set up scratch directories.
ASMFS_DIR="$TMPDIR_ARG/asmfs"
ORIG_DIR="$TMPDIR_ARG/orig"
rm -rf "$ASMFS_DIR" "$ORIG_DIR"
mkdir -p "$ASMFS_DIR" "$ORIG_DIR"

cleanup() {
  rm -rf "$ASMFS_DIR" "$ORIG_DIR"
}
trap cleanup EXIT

# Check FUSE mount resolves.
if [[ ! -e "$FUSE_ROOT" ]]; then
  echo "ERROR: path does not exist under FUSE mount: $FUSE_ROOT" >&2
  exit 4
fi

# Enumerate files.
if [[ -d "$FUSE_ROOT" ]]; then
  mapfile -t FILES < <(find "$FUSE_ROOT" -type f 2>/dev/null | LC_ALL=C sort)
  if [[ ${#FILES[@]} -eq 0 ]]; then
    echo "WARN: directory contains no files: $FUSE_ROOT" >&2
    exit 0
  fi
else
  FILES=("$FUSE_ROOT")
fi

# Convert a FUSE path back to its ASM logical form (+DISKGROUP/...).
fuse_to_asm() {
  local p="$1"
  local rel="${p#"$MOUNTPOINT/"}"
  printf '+%s\n' "$rel"
}

checksum() {
  "$CKSUM" "$1" | awk '{print $1}'
}

# Derive a safe filename tag from a path (used to namespace scratch files).
safe_tag() {
  printf '%s' "$1" | tr -c 'A-Za-z0-9._+' '_'
}

# Test functions set REASON to describe SKIP/FAIL outcomes.
REASON=""

# ----- Tests ------------------------------------------------------------------
# Each test takes $1=FUSE_FILE, $2=ASM_LOGICAL and returns:
#   0 PASS
#   1 FAIL
#   2 SKIP

test_1_basic_copy() {
  local fuse="$1" asm="$2"
  if [[ $HAS_ASMCMD -eq 0 ]]; then
    REASON="asmcmd not in PATH"; return 2
  fi
  local tag; tag=$(safe_tag "$asm")
  local dst_f="$ASMFS_DIR/${tag}.t1"
  local dst_o="$ORIG_DIR/${tag}.t1"
  rm -f "$dst_f" "$dst_o"
  if ! cp -- "$fuse" "$dst_f" 2>/dev/null; then
    REASON="cp from FUSE failed"; return 1
  fi
  if ! asmcmd cp -- "$asm" "$dst_o" >/dev/null 2>&1; then
    REASON="asmcmd cp failed"; return 1
  fi
  local a b
  a=$(checksum "$dst_f") || { REASON="checksum(FUSE) failed"; return 1; }
  b=$(checksum "$dst_o") || { REASON="checksum(asmcmd) failed"; return 1; }
  if [[ "$a" != "$b" ]]; then
    REASON="checksum mismatch (cp vs asmcmd)"; return 1
  fi
  return 0
}

test_2_dd_block_aligned() {
  local fuse="$1" asm="$2"
  local tag; tag=$(safe_tag "$asm")
  local dst_a="$ASMFS_DIR/${tag}.t2.512"
  local dst_b="$ASMFS_DIR/${tag}.t2.4k"
  rm -f "$dst_a" "$dst_b"
  if ! dd if="$fuse" of="$dst_a" bs=512  status=none 2>/dev/null; then
    REASON="dd bs=512 failed";  return 1
  fi
  if ! dd if="$fuse" of="$dst_b" bs=4096 status=none 2>/dev/null; then
    REASON="dd bs=4096 failed"; return 1
  fi
  local a b
  a=$(checksum "$dst_a")
  b=$(checksum "$dst_b")
  if [[ "$a" != "$b" ]]; then
    REASON="checksum mismatch (bs=512 vs bs=4096)"; return 1
  fi
  return 0
}

test_3_dd_partial_read() {
  local fuse="$1" asm="$2"
  if [[ $HAS_ASMCMD -eq 0 ]]; then
    REASON="asmcmd not in PATH"; return 2
  fi
  local size
  size=$(stat -c '%s' "$fuse" 2>/dev/null) || { REASON="stat failed"; return 1; }
  local bs=4096
  local total=$(( size / bs ))
  if [[ $total -lt 2 ]]; then
    REASON="file too small"; return 2
  fi
  local skip_n=$(( total / 4 ))
  [[ $skip_n -lt 1 ]] && skip_n=1
  local count_n=$(( total / 2 ))
  [[ $count_n -gt 2048 ]] && count_n=2048
  [[ $count_n -lt 1 ]]    && count_n=1
  if (( skip_n + count_n > total )); then
    count_n=$(( total - skip_n ))
  fi

  local tag; tag=$(safe_tag "$asm")
  local dst_f="$ASMFS_DIR/${tag}.t3.part"
  local dst_full="$ORIG_DIR/${tag}.t3.full"
  local dst_part="$ORIG_DIR/${tag}.t3.part"
  rm -f "$dst_f" "$dst_full" "$dst_part"

  if ! dd if="$fuse" of="$dst_f" bs=$bs skip=$skip_n count=$count_n status=none 2>/dev/null; then
    REASON="dd partial read (FUSE) failed"; return 1
  fi
  if ! asmcmd cp -- "$asm" "$dst_full" >/dev/null 2>&1; then
    REASON="asmcmd cp failed"; return 1
  fi
  if ! dd if="$dst_full" of="$dst_part" bs=$bs skip=$skip_n count=$count_n status=none 2>/dev/null; then
    REASON="dd partial read (asmcmd copy) failed"; return 1
  fi
  local a b
  a=$(checksum "$dst_f")
  b=$(checksum "$dst_part")
  if [[ "$a" != "$b" ]]; then
    REASON="checksum mismatch (FUSE partial vs asmcmd extracted)"; return 1
  fi
  return 0
}

test_4_rsync_read() {
  local fuse="$1" asm="$2"
  if [[ $HAS_RSYNC -eq 0 ]]; then
    REASON="rsync not in PATH"; return 2
  fi
  if [[ $HAS_ASMCMD -eq 0 ]]; then
    REASON="asmcmd not in PATH"; return 2
  fi
  local tag; tag=$(safe_tag "$asm")
  local dst_r="$ASMFS_DIR/${tag}.t4.rsync"
  local dst_o="$ORIG_DIR/${tag}.t4.ref"
  rm -f "$dst_r" "$dst_o"

  if ! rsync -a -- "$fuse" "$dst_r" 2>/dev/null; then
    REASON="rsync first pass failed"; return 1
  fi
  if ! asmcmd cp -- "$asm" "$dst_o" >/dev/null 2>&1; then
    REASON="asmcmd cp failed"; return 1
  fi
  local a b
  a=$(checksum "$dst_r")
  b=$(checksum "$dst_o")
  if [[ "$a" != "$b" ]]; then
    REASON="checksum mismatch (rsync vs asmcmd)"; return 1
  fi

  # Second run must be a no-op — exercises FUSE getattr/stat consistency
  # (rsync's quick-check uses size+mtime).
  local stats
  if ! stats=$(rsync -a --stats -- "$fuse" "$dst_r" 2>&1); then
    REASON="rsync second pass errored"; return 1
  fi
  local xferred
  xferred=$(printf '%s\n' "$stats" | awk -F': *' '
    /Number of regular files transferred/ { gsub(/,/,"",$2); print $2; exit }
    /Number of files transferred/         { gsub(/,/,"",$2); print $2; exit }
  ')
  if [[ -z "$xferred" ]]; then
    REASON="could not parse rsync --stats output"; return 1
  fi
  if [[ "$xferred" != "0" ]]; then
    REASON="rsync second run re-transferred $xferred file(s); expected 0 (FUSE getattr instability?)"
    return 1
  fi
  return 0
}

# ----- Dispatch ---------------------------------------------------------------

# num | name | function
TESTS=(
  "1|basic_copy|test_1_basic_copy"
  "2|dd_block_aligned|test_2_dd_block_aligned"
  "3|dd_partial_read|test_3_dd_partial_read"
  "4|rsync_read|test_4_rsync_read"
)

if [[ -n "${RUN_TESTS:-}" ]]; then
  # shellcheck disable=SC2206
  SELECTED=(${RUN_TESTS})
else
  SELECTED=(1 2 3 4)
fi

is_selected() {
  local n="$1" s
  for s in "${SELECTED[@]}"; do
    [[ "$s" == "$n" ]] && return 0
  done
  return 1
}

FILES_TESTED=0
TESTS_RUN=0
PASSED=0
FAILED=0
SKIPPED=0
FAILURES=()

for fuse in "${FILES[@]}"; do
  asm=$(fuse_to_asm "$fuse")
  echo "=== $asm ==="
  FILES_TESTED=$(( FILES_TESTED + 1 ))

  for entry in "${TESTS[@]}"; do
    IFS='|' read -r num tname func <<<"$entry"
    is_selected "$num" || continue
    TESTS_RUN=$(( TESTS_RUN + 1 ))
    REASON=""
    t0=$SECONDS
    "$func" "$fuse" "$asm"
    rc=$?
    dur=$(( SECONDS - t0 ))
    printf "[TEST %s] %-18s ... " "$num" "$tname"
    case $rc in
      0) printf "PASS (%ds)\n" "$dur"
         PASSED=$(( PASSED + 1 )) ;;
      2) printf "SKIP — %s\n" "${REASON:-(no reason)}"
         SKIPPED=$(( SKIPPED + 1 )) ;;
      *) printf "FAIL — %s (%ds)\n" "${REASON:-unknown}" "$dur"
         FAILED=$(( FAILED + 1 ))
         FAILURES+=("$asm — TEST $num: ${REASON:-unknown}") ;;
    esac
  done
done

echo "=========================================="
echo "FILES TESTED : $FILES_TESTED"
echo "TESTS RUN    : $TESTS_RUN"
echo "PASSED       : $PASSED"
echo "FAILED       : $FAILED"
echo "SKIPPED      : $SKIPPED"
echo "=========================================="
if [[ ${#FAILURES[@]} -gt 0 ]]; then
  echo "Failed tests:"
  for f in "${FAILURES[@]}"; do
    echo "  $f"
  done
fi

[[ $FAILED -gt 0 ]] && exit 1
exit 0
