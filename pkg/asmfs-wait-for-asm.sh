#!/bin/bash -eu
#
# This script is supposed to be run as root from systemd unit file.
# It outputs 'READY' when asm instance is started and ready enough to be mounted via asmfs.
#

G_SCRIPT_FILE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/$(basename --  "${BASH_SOURCE[0]}")"

if [ "$#" -eq 0 ]
then

    while true
    do
        set +e
        l_ps_full="$(ps -eo args=,user=,pid= | grep '^asm_pmon_+ASM')"
        l_user="$(echo "$l_ps_full" | awk '{print $2}')"
        l_pid="$(echo "$l_ps_full" | awk '{print $3}')"
        l_sid="$(echo "$l_ps_full" | awk '{print $1}' | cut -d'_' -f3)"
        set -e

        if [ -z "$l_user" ] || [ -z "$l_pid" ] || [ -z "$l_sid" ]
        then
            echo 'asm pmon is not yet running.'
            sleep 10
            continue
            # no exit, retry the loop
        fi

        l_exe="$(readlink -e -- "/proc/$l_pid/exe")"
        l_home="${l_exe%/bin/oracle}"

        if runuser -u "$l_user" -- "$G_SCRIPT_FILE" "$l_sid" "$l_home"
        then
            echo "READY"
            exit 0
        fi

        sleep 10

    done

else
    export ORACLE_SID="$1"
    export ORACLE_HOME="$2"
    export LD_LIBRARY_PATH="$2/lib:${LD_LIBRARY_PATH:-}"
    export PATH="$ORACLE_HOME/bin:$PATH"

    l_open_mode="$(sqlplus -S / as sysasm << eof
        whenever oserror exit failure
        whenever sqlerror exit failure
        set head off;
        set feed off;
        set trim on;
        set pages 0;
        set verify off;
        set echo off;
        select status from v\$instance;
eof
)"

    if [ "$l_open_mode" == 'STARTED' ]
    then
        exit 0
    else
        echo 'waiting for asm'
        exit 1
    fi
fi
