#!/bin/bash -eu
#

CFG_FIRST="$1"
CFG_SECOND="$2"
CFG_SKIP="$3"
CFG_BLOCK_SIZE="${4:-8192}"

echo "== $CFG_FIRST =="
dd if=$CFG_FIRST bs=$CFG_BLOCK_SIZE skip=$CFG_SKIP count=1 2>/dev/null | hexdump

echo ' '
echo "== $CFG_SECOND =="
dd if=$CFG_SECOND bs=$CFG_BLOCK_SIZE skip=$CFG_SKIP count=1 2>/dev/null | hexdump
echo ' '

