#!/usr/bin/env bash
set -euo pipefail

CPUS=""
DROP=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --cpus)
            CPUS="${2:?--cpus needs a CPU list}"
            shift 2
            ;;
        --drop-caches)
            DROP=1
            shift
            ;;
        --)
            shift
            break
            ;;
        *)
            break
            ;;
    esac
done

sync || true

if [[ "$DROP" -eq 1 ]]; then
    echo 3 >/proc/sys/vm/drop_caches 2>/dev/null || true
fi

shopt -s nullglob
for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    echo performance >"$gov" 2>/dev/null || true
done
shopt -u nullglob

if [[ -n "$CPUS" ]]; then
    exec taskset -c "$CPUS" "$@"
fi
exec "$@"
