#!/usr/bin/env bash
#
# compare.sh — compare two files block-by-block using md5sum.
#

set -u

usage() {
  cat <<'EOF'
Usage: compare.sh <FILE1> <FILE2> [BLOCK_SIZE]

Compare two files block-by-block. Each block is read with dd and
hashed with md5sum; blocks that match print ✓, blocks that differ
print ✗.

Arguments:
  FILE1, FILE2  Files to compare.
  BLOCK_SIZE    Block size in bytes (default 8192).

Exit codes:
  0  all blocks match
  1  one or more blocks differ
  2  invalid arguments / usage
EOF
}

case "${1:-}" in
  -h|--help) usage; exit 0 ;;
esac

if [[ $# -lt 2 || $# -gt 3 ]]; then
  usage >&2
  exit 2
fi

F1="$1"
F2="$2"
BS="${3:-8192}"

for f in "$F1" "$F2"; do
  if [[ ! -r "$f" ]]; then
    echo "ERROR: cannot read: $f" >&2
    exit 2
  fi
done

if ! [[ "$BS" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: block size must be a positive integer: $BS" >&2
  exit 2
fi

size1=$(stat -c '%s' "$F1")
size2=$(stat -c '%s' "$F2")
max_size=$(( size1 > size2 ? size1 : size2 ))
blocks=$(( (max_size + BS - 1) / BS ))

CHECK=$'\xe2\x9c\x93'   # ✓
CROSS=$'\xe2\x9c\x97'   # ✗

if [[ -t 1 ]]; then
  GREEN=$'\033[32m'
  RED=$'\033[31m'
  RESET=$'\033[0m'
else
  GREEN=""; RED=""; RESET=""
fi

printf '%-10s %-8s %s\n' "offset" "result" "(block size = $BS)"
printf '%-10s %-8s\n'    "------" "------"

diffs=0
for (( i = 0; i < blocks; i++ )); do
  h1=$(dd if="$F1" bs="$BS" skip="$i" count=1 status=none 2>/dev/null | md5sum | awk '{print $1}')
  h2=$(dd if="$F2" bs="$BS" skip="$i" count=1 status=none 2>/dev/null | md5sum | awk '{print $1}')
  if [[ "$h1" == "$h2" ]]; then
    printf '%-10d %s%s ok%s\n' "$i" "$GREEN" "$CHECK" "$RESET"
  else
    printf '%-10d %s%s diff%s\n' "$i" "$RED" "$CROSS" "$RESET"
    diffs=$(( diffs + 1 ))
  fi
done

if (( diffs > 0 )); then
  echo "---"
  echo "$diffs / $blocks block(s) differ"
  exit 1
fi
exit 0
