import os
import statistics
from datetime import datetime

import requests

# --- AIRFLOW CONFIG ---
# Ensure Airflow Webserver is running on port 8080 before running this script.
AIRFLOW_BASE = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080/api/v1")
DAG_ID = os.getenv("AIRFLOW_DAG_ID", "daily_real_estate_lakehouse")
AUTH = (
    os.getenv("AIRFLOW_USERNAME", "admin"),
    os.getenv("AIRFLOW_PASSWORD", "admin"),
)


def fetch_dag_runs(limit: int = 20):
    """Fetch the most recent DAG runs."""
    url = f"{AIRFLOW_BASE}/dags/{DAG_ID}/dagRuns"
    params = {"limit": limit, "order_by": "-execution_date"}
    response = requests.get(url, params=params, auth=AUTH, timeout=30)
    response.raise_for_status()
    return response.json()["dag_runs"]


def fetch_task_instances(dag_run_id: str):
    """Fetch task instances for a single DAG run."""
    url = f"{AIRFLOW_BASE}/dags/{DAG_ID}/dagRuns/{dag_run_id}/taskInstances"
    response = requests.get(url, auth=AUTH, timeout=30)
    response.raise_for_status()
    return response.json()["task_instances"]


def percentile(sorted_values, percentile_value: float):
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    index = int((len(sorted_values) - 1) * percentile_value)
    return sorted_values[index]


def main():
    print("Đang truy xuất dữ liệu từ Airflow API...")
    print(f"Airflow base: {AIRFLOW_BASE}")
    print(f"DAG_ID      : {DAG_ID}")
    print(f"AUTH user   : {AUTH[0]}")

    runs = fetch_dag_runs(limit=30)

    task_times = {}
    dag_totals = []

    valid_runs = 0
    target_runs = 14

    for run in runs:
        # Only include successful runs.
        if run.get("state") != "success":
            continue

        start_str = run["start_date"].replace("Z", "+00:00")
        end_str = run["end_date"].replace("Z", "+00:00")

        start_dt = datetime.fromisoformat(start_str)
        end_dt = datetime.fromisoformat(end_str)

        total_minutes = (end_dt - start_dt).total_seconds() / 60
        dag_totals.append(total_minutes)

        tis = fetch_task_instances(run["dag_run_id"])
        for task_instance in tis:
            if task_instance.get("state") != "success":
                continue
            duration = task_instance.get("duration")
            if duration is None:
                continue
            task_duration_minutes = duration / 60
            task_times.setdefault(task_instance["task_id"], []).append(task_duration_minutes)

        valid_runs += 1
        if valid_runs >= target_runs:
            break

    if not dag_totals:
        print("\n[Cảnh báo] Không tìm thấy DAG run nào có trạng thái 'success'!")
        print("Hãy đảm bảo DAG đã chạy thành công ít nhất một lần.")
        return

    dag_totals.sort()
    print(f"\n=== DAG End-to-End Duration ({len(dag_totals)} runs) ===")
    print(f" Min  : {min(dag_totals):.1f} min")
    print(f" P50  : {percentile(dag_totals, 0.50):.1f} min")
    print(f" P95  : {percentile(dag_totals, 0.95):.1f} min")
    print(f" Max  : {max(dag_totals):.1f} min")

    print("\n=== Per-task Duration (minutes) ===")
    print(f"{'Task':<32} {'P95':>6} {'Max':>6} {'Runs':>5}")
    print("-" * 55)

    for task_id, durations in sorted(task_times.items(), key=lambda item: max(item[1]), reverse=True):
        durations.sort()
        run_count = len(durations)
        task_p95 = percentile(durations, 0.95)
        task_max = max(durations)
        print(f"{task_id:<32} {task_p95:>6.1f} {task_max:>6.1f} {run_count:>5}")


if __name__ == "__main__":
    main()
