from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path

import plotly.graph_objects as go
from plotly.subplots import make_subplots


BRONZE_ROW_RE = re.compile(
    r"^\s*(?P<n_records>\d+)\s*\|\s*(?P<time_s>[\d.]+)\s*\|\s*(?P<rec_per_s>[\d.]+)\s*\|\s*(?P<peak_mb>[\d.]+)\s*\|\s*(?P<ok>\d+)\s*\|\s*(?P<fail>\d+)\s*$"
)
SPARK_STAGE_RE = re.compile(r"^\s*Stage\s+(?P<stage_id>\d+)\s+(?P<duration_ms>\d+)ms\s+(?P<stage_name>.+)$")
SPARK_PEAK_RE = re.compile(r"^\s*Peak Memory Max:\s*(?P<value>[\d.]+)\s*MB\s*$")
SPARK_SHUFFLE_RE = re.compile(r"^\s*Total Shuffle Written:\s*(?P<value>[\d.]+)\s*MB\s*$")
SPARK_ELAPSED_RE = re.compile(r"^elapsed=(?P<value>[\d.]+)\s*$")
VM_IDLE_RE = re.compile(r"^\s*Avg CPU idle\s*:\s*(?P<value>[\d.]+)%\s*$")
VM_SWAP_IN_RE = re.compile(r"^\s*Avg swap in\s*:\s*(?P<value>[\d.]+)\s*$")
VM_SWAP_OUT_RE = re.compile(r"^\s*Avg swap out\s*:\s*(?P<value>[\d.]+)\s*$")
VM_MAX_SWAP_IN_RE = re.compile(r"^\s*Max swap in\s*:\s*(?P<value>[\d.]+)\s*$")
VM_MAX_SWAP_OUT_RE = re.compile(r"^\s*Max swap out\s*:\s*(?P<value>[\d.]+)\s*$")


@dataclass
class BronzeResult:
    n_records: int
    time_s: float
    rec_per_s: float
    peak_mb: float
    ok: int
    fail: int


@dataclass
class SparkResult:
    driver_memory: str
    elapsed_s: float | None
    peak_memory_mb: float | None
    shuffle_written_mb: float | None
    stage_rows: list[tuple[int, int, str]]


@dataclass
class VmResult:
    driver_memory: str
    avg_cpu_idle: float | None
    avg_swap_in: float | None
    avg_swap_out: float | None
    max_swap_in: float | None
    max_swap_out: float | None


def parse_bronze_log(path: Path) -> list[BronzeResult]:
    results: list[BronzeResult] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        match = BRONZE_ROW_RE.match(line)
        if not match:
            continue
        results.append(
            BronzeResult(
                n_records=int(match.group("n_records")),
                time_s=float(match.group("time_s")),
                rec_per_s=float(match.group("rec_per_s")),
                peak_mb=float(match.group("peak_mb")),
                ok=int(match.group("ok")),
                fail=int(match.group("fail")),
            )
        )
    if not results:
        raise ValueError(f"No Bronze benchmark rows found in {path}")
    return results


def parse_spark_log(path: Path, driver_memory: str) -> SparkResult:
    elapsed_s = None
    peak_memory_mb = None
    shuffle_written_mb = None
    stage_rows: list[tuple[int, int, str]] = []

    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = SPARK_ELAPSED_RE.match(line)
        if match:
            elapsed_s = float(match.group("value"))
            continue
        match = SPARK_STAGE_RE.match(line)
        if match:
            stage_rows.append(
                (int(match.group("stage_id")), int(match.group("duration_ms")), match.group("stage_name"))
            )
            continue
        match = SPARK_PEAK_RE.match(line)
        if match:
            peak_memory_mb = float(match.group("value"))
            continue
        match = SPARK_SHUFFLE_RE.match(line)
        if match:
            shuffle_written_mb = float(match.group("value"))

    if not stage_rows and peak_memory_mb is None and shuffle_written_mb is None and elapsed_s is None:
        raise ValueError(f"No Spark metrics found in {path}")

    return SparkResult(
        driver_memory=driver_memory,
        elapsed_s=elapsed_s,
        peak_memory_mb=peak_memory_mb,
        shuffle_written_mb=shuffle_written_mb,
        stage_rows=stage_rows,
    )


def parse_vm_log(path: Path, driver_memory: str) -> VmResult:
    avg_cpu_idle = None
    avg_swap_in = None
    avg_swap_out = None
    max_swap_in = None
    max_swap_out = None

    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if match := VM_IDLE_RE.match(line):
            avg_cpu_idle = float(match.group("value"))
        elif match := VM_SWAP_IN_RE.match(line):
            avg_swap_in = float(match.group("value"))
        elif match := VM_SWAP_OUT_RE.match(line):
            avg_swap_out = float(match.group("value"))
        elif match := VM_MAX_SWAP_IN_RE.match(line):
            max_swap_in = float(match.group("value"))
        elif match := VM_MAX_SWAP_OUT_RE.match(line):
            max_swap_out = float(match.group("value"))

    return VmResult(
        driver_memory=driver_memory,
        avg_cpu_idle=avg_cpu_idle,
        avg_swap_in=avg_swap_in,
        avg_swap_out=avg_swap_out,
        max_swap_in=max_swap_in,
        max_swap_out=max_swap_out,
    )


def write_markdown_table(headers: list[str], rows: list[list[str]], title: str) -> str:
    lines = [f"### {title}", "", "| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def figure_bronze(bronze_rows: list[BronzeResult], output_dir: Path) -> None:
    bronze_rows = sorted(bronze_rows, key=lambda row: row.n_records)
    x = [row.n_records for row in bronze_rows]
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(x=x, y=[row.rec_per_s for row in bronze_rows], mode="lines+markers", name="Throughput (rec/s)"), secondary_y=False)
    fig.add_trace(go.Scatter(x=x, y=[row.peak_mb for row in bronze_rows], mode="lines+markers", name="Peak Memory (MB)"), secondary_y=True)
    fig.update_layout(title="Bronze -> Silver Scalability", xaxis_title="Records")
    fig.update_yaxes(title_text="Throughput (rec/s)", secondary_y=False)
    fig.update_yaxes(title_text="Peak Memory (MB)", secondary_y=True)
    fig.write_html(output_dir / "bronze_scalability.html")


def figure_spark(spark_rows: list[SparkResult], output_dir: Path) -> None:
    labels = [row.driver_memory for row in spark_rows]
    elapsed = [row.elapsed_s or 0.0 for row in spark_rows]
    peak_mem = [row.peak_memory_mb or 0.0 for row in spark_rows]
    shuffle = [row.shuffle_written_mb or 0.0 for row in spark_rows]

    fig = make_subplots(rows=1, cols=3, subplot_titles=("Runtime", "Peak Execution Memory", "Shuffle Written"))
    fig.add_trace(go.Bar(x=labels, y=elapsed, name="Runtime (s)"), row=1, col=1)
    fig.add_trace(go.Bar(x=labels, y=peak_mem, name="Peak Memory (MB)"), row=1, col=2)
    fig.add_trace(go.Bar(x=labels, y=shuffle, name="Shuffle Written (MB)"), row=1, col=3)
    fig.update_layout(title="Silver -> Gold Comparison", showlegend=False)
    fig.write_html(output_dir / "spark_comparison.html")

    stage_names = []
    stage_durations = []
    for row in spark_rows:
        if row.stage_rows:
            stage_id, duration_ms, stage_name = max(row.stage_rows, key=lambda item: item[1])
            stage_names.append(f"{row.driver_memory}: {stage_name[:35]}")
            stage_durations.append(duration_ms / 1000.0)
    if stage_names:
        fig2 = go.Figure(data=[go.Bar(x=stage_names, y=stage_durations)])
        fig2.update_layout(title="Longest Spark Stage per Run", xaxis_title="Run / Stage", yaxis_title="Seconds")
        fig2.write_html(output_dir / "spark_longest_stage.html")


def figure_vm(vm_rows: list[VmResult], output_dir: Path) -> None:
    labels = [row.driver_memory for row in vm_rows]
    fig = make_subplots(rows=1, cols=2, subplot_titles=("CPU Idle", "Swap Activity"))
    fig.add_trace(go.Bar(x=labels, y=[row.avg_cpu_idle or 0.0 for row in vm_rows], name="Avg CPU idle"), row=1, col=1)
    fig.add_trace(go.Bar(x=labels, y=[row.avg_swap_in or 0.0 for row in vm_rows], name="Avg swap in"), row=1, col=2)
    fig.add_trace(go.Bar(x=labels, y=[row.avg_swap_out or 0.0 for row in vm_rows], name="Avg swap out"), row=1, col=2)
    fig.update_layout(title="VM Resource Comparison", showlegend=True)
    fig.write_html(output_dir / "vm_comparison.html")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-log", default="/tmp/bronze_silver_benchmark.log")
    parser.add_argument("--spark-4g-log", default="/tmp/spark_4g_run.log")
    parser.add_argument("--spark-2g-log", default="/tmp/spark_2g_run.log")
    parser.add_argument("--vm-4g-log", default="/tmp/vm_4g_metrics.log")
    parser.add_argument("--vm-2g-log", default="/tmp/vm_2g_metrics.log")
    parser.add_argument("--output-dir", default="data/reports/performance")
    args = parser.parse_args()

    bronze_log = Path(args.bronze_log)
    spark_4g_log = Path(args.spark_4g_log)
    spark_2g_log = Path(args.spark_2g_log)
    vm_4g_log = Path(args.vm_4g_log)
    vm_2g_log = Path(args.vm_2g_log)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    bronze_rows = parse_bronze_log(bronze_log)
    spark_rows = [
        parse_spark_log(spark_4g_log, "4GB"),
        parse_spark_log(spark_2g_log, "2GB"),
    ]
    vm_rows = [
        parse_vm_log(vm_4g_log, "4GB"),
        parse_vm_log(vm_2g_log, "2GB"),
    ]

    figure_bronze(bronze_rows, output_dir)
    figure_spark(spark_rows, output_dir)
    figure_vm(vm_rows, output_dir)

    report_lines = [
        "# Performance Evaluation Report",
        "",
        "Generated from benchmark logs saved with `tee`.",
        "",
        write_markdown_table(
            ["Records", "Time (s)", "Throughput (rec/s)", "Peak Memory (MB)", "OK", "Fail"],
            [
                [
                    str(row.n_records),
                    f"{row.time_s:.1f}",
                    f"{row.rec_per_s:.1f}",
                    f"{row.peak_mb:.1f}",
                    str(row.ok),
                    str(row.fail),
                ]
                for row in bronze_rows
            ],
            "Bronze -> Silver",
        ),
        "",
        write_markdown_table(
            ["Driver memory", "Elapsed (s)", "Peak execution memory (MB)", "Shuffle written (MB)", "Longest stage"],
            [
                [
                    row.driver_memory,
                    f"{row.elapsed_s:.1f}" if row.elapsed_s is not None else "n/a",
                    f"{row.peak_memory_mb:.1f}" if row.peak_memory_mb is not None else "n/a",
                    f"{row.shuffle_written_mb:.1f}" if row.shuffle_written_mb is not None else "n/a",
                    max(row.stage_rows, key=lambda item: item[1])[2][:55] if row.stage_rows else "n/a",
                ]
                for row in spark_rows
            ],
            "Silver -> Gold",
        ),
        "",
        write_markdown_table(
            ["Driver memory", "Avg CPU idle (%)", "Avg swap in", "Avg swap out", "Max swap in", "Max swap out"],
            [
                [
                    row.driver_memory,
                    f"{row.avg_cpu_idle:.1f}" if row.avg_cpu_idle is not None else "n/a",
                    f"{row.avg_swap_in:.1f}" if row.avg_swap_in is not None else "n/a",
                    f"{row.avg_swap_out:.1f}" if row.avg_swap_out is not None else "n/a",
                    f"{row.max_swap_in:.1f}" if row.max_swap_in is not None else "n/a",
                    f"{row.max_swap_out:.1f}" if row.max_swap_out is not None else "n/a",
                ]
                for row in vm_rows
            ],
            "VM Resources",
        ),
        "",
        "## Outputs",
        "",
        "- `bronze_scalability.html`",
        "- `spark_comparison.html`",
        "- `spark_longest_stage.html`",
        "- `vm_comparison.html`",
    ]

    report_path = output_dir / "performance_report.md"
    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    csv_path = output_dir / "bronze_benchmark.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["n_records", "time_s", "rec_per_s", "peak_mb", "ok", "fail"])
        for row in bronze_rows:
            writer.writerow([row.n_records, row.time_s, row.rec_per_s, row.peak_mb, row.ok, row.fail])

    print(f"Wrote report: {report_path}")
    print(f"Wrote CSV: {csv_path}")
    print(f"Wrote charts to: {output_dir}")


if __name__ == "__main__":
    main()