#!/bin/bash -eu
#

G_TEST="${1:-/mnt/asm/+DATA}"

find "$G_TEST" -type f -exec ./test-file.sh {} \;

echo "(all tests finished)"

