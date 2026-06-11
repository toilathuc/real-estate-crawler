#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="${VMSTAT_LOG:-/tmp/spark_resource_log.txt}"
VMSTAT_PID=""
STATUS=0

mkdir -p "$(dirname "$LOG_FILE")"

vmstat 2 | awk 'NR==1{print "timestamp " $0} NR>2{print strftime("%H:%M:%S"), $0}' > "$LOG_FILE" &
VMSTAT_PID=$!

cleanup() {
  if [[ -n "$VMSTAT_PID" ]]; then
    kill "$VMSTAT_PID" 2>/dev/null || true
    wait "$VMSTAT_PID" 2>/dev/null || true
    VMSTAT_PID=""
  fi
}

trap cleanup EXIT

if [[ -n "${VM_MEASURE_CMD:-}" ]]; then
  bash -lc "$VM_MEASURE_CMD"
  STATUS=$?
else
  echo "No VM_MEASURE_CMD provided; running the default Spark Gold job with 4g memory."
  PYTHONPATH=src SPARK_EVENTLOG_ENABLED=true SPARK_EVENTLOG_DIR=/tmp/spark-events SPARK_DRIVER_MEMORY=4g \
    .venv/bin/python -m transform.silver_to_gold
  STATUS=$?
fi

cleanup
trap - EXIT

.venv/bin/python tests/perf/parse_vmstat_log.py --log-file "$LOG_FILE"

exit "$STATUS"