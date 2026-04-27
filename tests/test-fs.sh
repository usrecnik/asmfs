#!/bin/bash -eu
#

G_TEST="${1:-/mnt/asm/+DATA}"
G_TEMP="${2:-/mnt/ramdisk}"

find "$G_TEST" -type f -exec ./test-file.sh {} "$G_TEMP" \;

echo "(all tests finished)"

