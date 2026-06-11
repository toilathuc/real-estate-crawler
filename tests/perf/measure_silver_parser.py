from __future__ import annotations

import json
import random
import sys
import time
import tracemalloc
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from parsing import extract_features, parse_listing
from parsing.quality import apply_quality_flags


BRONZE_ROOT = Path("data/bronze")
LIMITS = [1900, 10000, 50000]
PARSER_VERSION = "perf_benchmark_v1"
RANDOM_SEED = 42


def read_text_file(path_value: str | None) -> str:
    if not path_value:
        return ""
    path = Path(path_value)
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="ignore")


def discover_metadata_files(bronze_root: Path) -> list[Path]:
    candidates = list(bronze_root.rglob("metadata/*.json"))
    if candidates:
        return sorted(candidates)
    return sorted(bronze_root.rglob("*.json"))


def benchmark(bronze_root: Path = BRONZE_ROOT, limits: list[int] = LIMITS) -> None:
    metadata_files = discover_metadata_files(bronze_root)
    print(f"Total Bronze metadata files available: {len(metadata_files)}\n")
    print(f"{'n_records':>10} | {'time_s':>8} | {'rec_per_s':>10} | {'peak_mb':>10} | {'ok':>6} | {'fail':>6}")
    print("-" * 68)

    if not metadata_files:
        raise FileNotFoundError(f"No Bronze metadata files found under {bronze_root}")

    rng = random.Random(RANDOM_SEED)

    for n_records in limits:
        sample = rng.choices(metadata_files, k=n_records)

        tracemalloc.start()
        t0 = time.perf_counter()

        ok = 0
        fail = 0

        for metadata_path in sample:
            try:
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
                raw_html = read_text_file(metadata.get("raw_html_path"))
                raw_text = read_text_file(metadata.get("raw_text_path"))
                parsed = parse_listing(
                    raw_html=raw_html,
                    raw_text=raw_text,
                    metadata=metadata,
                    parser_version=PARSER_VERSION,
                )
                if parsed.get("parse_status") == "failed":
                    fail += 1
                    continue
                apply_quality_flags(extract_features(parsed))
                ok += 1
            except Exception:
                fail += 1

        elapsed = time.perf_counter() - t0
        peak_bytes = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()

        rec_per_s = ok / elapsed if elapsed > 0 else 0.0
        print(
            f"{n_records:>10} | {elapsed:>8.1f} | {rec_per_s:>10.1f} | {peak_bytes / 1e6:>10.1f} | {ok:>6} | {fail:>6}"
        )


if __name__ == "__main__":
    benchmark()
