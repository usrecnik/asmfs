#!/bin/bash -eu
#
# test-file.sh - integration tests for a single file on the ASMFS FUSE mount.
# Designed to be called per-file (e.g. from test-fs.sh's find -exec loop).
#

CFG_FILE="$1"
CFG_TMPDIR="${2:-/tmp/asmfs}"
CFG_ASM_FILE="+${CFG_FILE#*+}"

echo ' '
echo -e "##\n## $CFG_FILE \n##"

if [[ ! -f "$CFG_FILE" ]]; then
  echo "ERROR: not a regular file: $FUSE_FILE" >&2
  exit 4
fi

if [[ "$CFG_ASM_FILE" != +* ]]; then
  echo "ERROR: cannot derive ASM logical name from: $FUSE_FILE" >&2
  echo "       (expected path to contain '+DISKGROUP/...')" >&2
  exit 4
fi

# Feature detection.
HAS_ASMCMD=0; command -v asmcmd >/dev/null 2>&1 && HAS_ASMCMD=1
HAS_RSYNC=0;  command -v rsync  >/dev/null 2>&1 && HAS_RSYNC=1
[[ $HAS_ASMCMD -eq 0 ]] && echo "WARN: asmcmd not in PATH" >&2
[[ $HAS_RSYNC  -eq 0 ]] && echo "WARN: rsync not in PATH" >&2

# Setup
rm -fv "$CFG_TMPDIR/asmcmd-cp.file"
asmcmd cp "$CFG_ASM_FILE" "$CFG_TMPDIR/asmcmd-cp.file"
if [ ! -f "$CFG_TMPDIR/asmcmd-cp.file" ]; then
    echo "ERROR: Unable to get asmcmd cp copy of file to '$CFG_TMPDIR/asmcmd.t1'" >&2
    exit 1
fi


# ----- Tests ------------------------------------------------------------------
test_checksum() {
    local a_file="$1"
    local b_file="$2"
    local a_csum="$(md5sum "$a_file" | awk '{print $1}')"
    local b_csum="$(md5sum "$b_file" | awk '{print $1}')"
    if [ "$a_csum" != "$b_csum" ]
    then
        echo "checksum mismatch ($CFG_FILE)" >&2
        echo "$a_file: $a_csum" >&2
        echo "$b_file: $b_csum" >&2
        exit 1
    else
        echo "Checksum ok (both $a_csum | $b_csum)"
    fi
}

test_1_basic_copy() {
    rm -fv "$CFG_TMPDIR/fs.t1"
    if ! cp -v "${CFG_FILE}" "$CFG_TMPDIR/fs1.t1"; then
        echo "cp from FUSE failed" >&2
        exit 1
    fi
    test_checksum "${CFG_TMPDIR}/asmcmd-cp.file" "${CFG_TMPDIR}/fs1.t1"
    rm -fv "$CFG_TMPDIR/fs.t1"
}

test_2_rsync_read() {
    rm -fv "$CFG_TMPDIR/fs.t1"
    if ! rsync -a "${CFG_FILE}" "$CFG_TMPDIR/fs1.t1"
    then
        echo "rsync from FUSE failed" >&2
        exit 1
    fi
    test_checksum "${CFG_TMPDIR}/asmcmd-cp.file" "${CFG_TMPDIR}/fs1.t1"

    # Second run must be a no-op — exercises FUSE getattr/stat consistency
    # (rsync's quick-check uses size+mtime).
    local stats
    if ! stats=$(rsync -a --stats "$CFG_FILE" "${CFG_TMPDIR}/fs1.t1" 2>&1); then
        REASON="rsync second pass errored"; return 1
    fi
    local xferred=$(printf '%s\n' "$stats" | awk -F': *' '
    /Number of regular files transferred/ { gsub(/,/,"",$2); print $2; exit }
    /Number of files transferred/         { gsub(/,/,"",$2); print $2; exit }
  ')
    if [[ -z "$xferred" ]]
    then
        echo "could not parse rsync --stats output" >&2
        exit 1
    fi
    if [[ "$xferred" != "0" ]]; then
        echo "rsync second run re-transferred $xferred file(s); expected 0 (FUSE getattr instability?)" >&2
        exit 1
    fi
}

# ----- Dispatch ---------------------------------------------------------------

test_1_basic_copy
test_2_rsync_read

