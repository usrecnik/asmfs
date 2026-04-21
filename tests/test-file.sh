#!/usr/bin/env bash
#
# test_asmfs.sh — integration tests for a single file on the ASMFS FUSE mount.
# Designed to be called per-file (e.g. from test-fs.sh's find -exec loop).
#

set -u

usage() {
  cat <<'EOF'
Usage: test_asmfs.sh <FUSE_FILE> [TMPDIR]

Run the ASMFS integration test battery against a single file on the
FUSE mount, cross-checking results against `asmcmd cp`.

Arguments:
  FUSE_FILE    Absolute path to a regular file on the FUSE mount. The
               ASM logical name is derived by taking the substring from
               the first `+` onward, e.g.
                 /mnt/asm/+DATA/MYDB/DATAFILE/system.256.1234567890
                   →  +DATA/MYDB/DATAFILE/system.256.1234567890
  TMPDIR       Scratch directory (default: /tmp/asm_tests). Per-file
               scratch files are tagged and removed on exit; the
               directory itself is not wiped, so concurrent per-file
               invocations don't stomp on each other.

Environment:
  RUN_TESTS    Space-separated list of test numbers to run, e.g. "1 2 3".
               Defaults to all tests.

Tests:
  1 basic_copy        cp(FUSE) vs `asmcmd cp`, full-file checksum match.
  2 dd_block_aligned  dd bs=512 vs dd bs=4096 on FUSE, checksum match.
  3 dd_partial_read   dd skip=N count=M on FUSE vs the same region
                      extracted from an `asmcmd cp` of the full file.
  4 rsync_read        rsync(FUSE) vs `asmcmd cp`, plus a rsync no-op
                      check on a second run (exercises stable FUSE
                      getattr/stat).

Exit codes:
  0  all tests passed or skipped
  1  one or more tests failed
  2  invalid arguments / usage
  3  no checksum tool available (sha256sum or md5sum)
  4  FUSE_FILE not a regular file, or ASM logical name not derivable
EOF
}

case "${1:-}" in
  -h|--help) usage; exit 0 ;;
esac

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 2
fi

FUSE_FILE="$1"
TMPDIR_ARG="${2:-/tmp/asm_tests}"
TMPDIR_ARG="${TMPDIR_ARG%/}"

if [[ ! -f "$FUSE_FILE" ]]; then
  echo "ERROR: not a regular file: $FUSE_FILE" >&2
  exit 4
fi

if [[ "$FUSE_FILE" != *+* ]]; then
  echo "ERROR: cannot derive ASM logical name from: $FUSE_FILE" >&2
  echo "       (expected path to contain '+DISKGROUP/...')" >&2
  exit 4
fi
ASM_LOGICAL="+${FUSE_FILE#*+}"

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

# Set up scratch directories (shared across invocations; cleanup is per-tag).
ASMFS_DIR="$TMPDIR_ARG/asmfs"
ORIG_DIR="$TMPDIR_ARG/orig"
mkdir -p "$ASMFS_DIR" "$ORIG_DIR"

# Derive a safe filename tag from a path (used to namespace scratch files).
safe_tag() {
  printf '%s' "$1" | tr -c 'A-Za-z0-9._+' '_'
}
TAG=$(safe_tag "$ASM_LOGICAL")

cleanup() {
  rm -f "$ASMFS_DIR/${TAG}."* "$ORIG_DIR/${TAG}."*
}
trap cleanup EXIT

checksum() {
  "$CKSUM" "$1" | awk '{print $1}'
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
  local dst_f="$ASMFS_DIR/${TAG}.t1"
  local dst_o="$ORIG_DIR/${TAG}.t1"
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
  local dst_a="$ASMFS_DIR/${TAG}.t2.512"
  local dst_b="$ASMFS_DIR/${TAG}.t2.4k"
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

  local dst_f="$ASMFS_DIR/${TAG}.t3.part"
  local dst_full="$ORIG_DIR/${TAG}.t3.full"
  local dst_part="$ORIG_DIR/${TAG}.t3.part"
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
  local dst_r="$ASMFS_DIR/${TAG}.t4.rsync"
  local dst_o="$ORIG_DIR/${TAG}.t4.ref"
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

FAILED=0

echo "=== $ASM_LOGICAL ==="

for entry in "${TESTS[@]}"; do
  IFS='|' read -r num tname func <<<"$entry"
  is_selected "$num" || continue
  REASON=""
  t0=$SECONDS
  "$func" "$FUSE_FILE" "$ASM_LOGICAL"
  rc=$?
  dur=$(( SECONDS - t0 ))
  printf "[TEST %s] %-18s ... " "$num" "$tname"
  case $rc in
    0) printf "PASS (%ds)\n" "$dur" ;;
    2) printf "SKIP — %s\n" "${REASON:-(no reason)}" ;;
    *) printf "FAIL — %s (%ds)\n" "${REASON:-unknown}" "$dur"
       FAILED=$(( FAILED + 1 )) ;;
  esac
done

[[ $FAILED -gt 0 ]] && exit 1
exit 0
