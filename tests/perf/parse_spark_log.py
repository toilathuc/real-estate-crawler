from __future__ import annotations

import json
import subprocess
from pathlib import Path

LOG_DIR = Path("/tmp/spark-events")


def parse_logs(log_dir: Path = LOG_DIR) -> None:
    stages: list[tuple[int, str, int]] = []
    peak_mems: list[int] = []
    shuffle_bytes_total = 0

    for path in sorted(log_dir.rglob("*")):
        if path.is_dir():
            continue
        if path.suffix == ".zstd":
            stream = subprocess.Popen(
                ["zstd", "-dc", str(path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                encoding="utf-8",
                errors="ignore",
            )
            handle = stream.stdout
        else:
            handle = path.open(encoding="utf-8", errors="ignore")

        if handle is None:
            continue

        try:
            for line in handle:
                try:
                    event = json.loads(line)
                except Exception:
                    continue

                if not isinstance(event, dict):
                    continue

                if event.get("Event") == "SparkListenerStageCompleted":
                    info = event.get("Stage Info", {})
                    submission_time = info.get("Submission Time")
                    completion_time = info.get("Completion Time")
                    if submission_time is not None and completion_time is not None:
                        stages.append(
                            (
                                int(info.get("Stage ID", -1)),
                                str(info.get("Stage Name", "")),
                                int(completion_time - submission_time),
                            )
                        )

                if event.get("Event") == "SparkListenerTaskEnd":
                    metrics = event.get("Task Metrics", {})
                    peak = metrics.get("Peak Execution Memory", 0) or 0
                    shuffle_write = metrics.get("Shuffle Write Metrics", {}).get(
                        "Shuffle Bytes Written",
                        0,
                    ) or 0
                    if peak > 0:
                        peak_mems.append(int(peak))
                    shuffle_bytes_total += int(shuffle_write)
        finally:
            if path.suffix == ".zstd":
                if handle is not None:
                    handle.close()
                if stream.stdout is not None:
                    stream.stdout.close()
                stream.wait()
            else:
                handle.close()

    print("\n=== Stage Durations ===")
    for stage_id, stage_name, duration_ms in sorted(stages):
        print(f" Stage {stage_id:>3} {duration_ms:>7}ms {stage_name[:60]}")

    if peak_mems:
        print("\n=== Resource Metrics ===")
        print(f" Peak Memory Max: {max(peak_mems) / 1e6:.1f} MB")
        print(f" Total Shuffle Written: {shuffle_bytes_total / 1e6:.1f} MB")
    else:
        print("\nNo Spark task metrics found in event logs.")


if __name__ == "__main__":
    parse_logs()
