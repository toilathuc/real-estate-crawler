from __future__ import annotations

import argparse
from pathlib import Path


def parse_vmstat(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"VMStat log not found: {path}")

    idle_values: list[float] = []
    swap_in_values: list[float] = []
    swap_out_values: list[float] = []

    with path.open(encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if not line[:2].isdigit():
                continue

            parts = line.split()
            if len(parts) < 19:
                continue

            try:
                swap_in_values.append(float(parts[8]))
                swap_out_values.append(float(parts[9]))
                idle_values.append(float(parts[16]))
            except ValueError:
                continue

    print(f"\n=== VM Resource Summary ({path}) ===")

    if not idle_values:
        print("No vmstat samples were parsed.")
        return

    avg_idle = sum(idle_values) / len(idle_values)
    avg_swap_in = sum(swap_in_values) / len(swap_in_values) if swap_in_values else 0.0
    avg_swap_out = sum(swap_out_values) / len(swap_out_values) if swap_out_values else 0.0

    print(f" Samples        : {len(idle_values)}")
    print(f" Avg CPU idle   : {avg_idle:.1f}%")
    print(f" Avg swap in    : {avg_swap_in:.1f}")
    print(f" Avg swap out   : {avg_swap_out:.1f}")
    print(f" Max swap in    : {max(swap_in_values):.1f}" if swap_in_values else " Max swap in    : 0.0")
    print(f" Max swap out   : {max(swap_out_values):.1f}" if swap_out_values else " Max swap out   : 0.0")
    print("\nLowest CPU idle samples:")
    for value in sorted(idle_values)[:5]:
        print(f" {value:.1f}%")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-file", default="/tmp/spark_resource_log.txt")
    args = parser.parse_args()
    parse_vmstat(Path(args.log_file))


if __name__ == "__main__":
    main()