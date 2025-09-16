import json
from datetime import datetime
import numpy as np
import requests


def _dt(ts):
    # Accept both with/without GMT
    return int(
        datetime.strptime(ts.replace("GMT", ""), "%Y-%m-%dT%H:%M:%S.%f").timestamp()
        * 1000
    )


# app_end_time = "2025-08-26T10:49:39.547GMT"  # <== fill this from your /applications API or selected app JSON
# app_end_time_ms = _dt(app_end_time)

# app_id = "application_1756176332935_0487"
app_id = "application_1756268645419_0648"
base_url = "http://localhost:18080/api/v1"
app_details = requests.get(f"{base_url}/applications/{app_id}", verify=False).json()
app_end_time = _dt(app_details["attempts"][0]["endTime"])


def parse_executor_events(executors):
    # Yields (start, end, cores) for each executor
    periods = []
    for ex in executors:
        start = _dt(ex["addTime"])
        end = (
            _dt(ex["removeTime"])
            if "removeTime" in ex and ex["removeTime"]
            else app_end_time
        )
        slots = ex.get("maxTasks", ex.get("totalCores", 1))
        periods.append((start, end, slots))
    return periods


def max_concurrent_executors(periods):
    events = []
    for start, end, _ in periods:
        events.append((start, +1))
        events.append((end, -1))
    events.sort()
    current, peak, times, values = 0, 0, [], []
    for ts, change in events:
        current += change
        times.append(ts)
        values.append(current)
        if current > peak:
            peak = current
    return times, values, peak


def percentile_concurrency(values, perc=95):
    return int(np.percentile(values, perc))


def analyze_idle_time(executors):
    # Each executor record in the REST API output has:
    # addTime, removeTime, totalCores, totalDuration
    # totalDuration is the sum of task run time on this executor (ms)
    rows = []
    for ex in executors:
        start = _dt(ex["addTime"])
        end = (
            _dt(ex["removeTime"])
            if "removeTime" in ex and ex["removeTime"]
            else app_end_time
        )
        slots = ex.get("maxTasks", ex.get("totalCores", 1))
        uptime_ms = end - start
        slot_time_ms = uptime_ms * slots
        total_task_time_ms = ex.get("totalDuration", 0)
        idle_time_ms = slot_time_ms - total_task_time_ms
        idle_pct = (idle_time_ms / slot_time_ms) * 100 if slot_time_ms > 0 else 0
        rows.append(
            {
                "id": ex.get("id", None),
                "uptime_sec": uptime_ms / 1000,
                "slots": slots,
                "slot_time_sec": slot_time_ms / 1000,
                "task_time_sec": total_task_time_ms / 1000,
                "idle_time_sec": idle_time_ms / 1000,
                "idle_pct": idle_pct,
            }
        )
    return rows


def recommend_max_executors(concurrency_curve, idle_rows, target_idle_pct=15):
    # We want to find maxExecutors where the overall idle % gets into target_band (default: 15%)
    max_executors_p95 = percentile_concurrency(concurrency_curve, 95)
    avg_idle_pct = np.mean([r["idle_pct"] for r in idle_rows])
    p95_idle_pct = np.percentile([r["idle_pct"] for r in idle_rows], 95)

    adjustment_factor = 1 - (avg_idle_pct / 100.0)
    # Only adjust if idle is above target band. If below, recommend current value.
    if avg_idle_pct > target_idle_pct:
        recommended_max = max(int(max_executors_p95 * adjustment_factor), 1)
    else:
        recommended_max = max_executors_p95

    return {
        "current_p95_maxExecutors": max_executors_p95,
        "avg_idle_pct": avg_idle_pct,
        "p95_idle_pct": p95_idle_pct,
        "recommended_maxExecutors": recommended_max,
        "target_idle_pct": target_idle_pct,
    }


# ---- LOAD & PROCESS ----
# with open("allexecutors.json") as f:
#     executors = json.load(f)

executors_url = f"{base_url}/applications/{app_id}/1/allexecutors"
executors = requests.get(executors_url, verify=False, timeout=10).json()

# Optimal executor/slot concurrency (as before)
periods = parse_executor_events(executors)
times, exe_curve, peak = max_concurrent_executors(periods)
p95 = percentile_concurrency(exe_curve, 95)
slots_per_executor = periods[0][2] if periods else 1
print(f"Peak concurrent executors: {peak}")
print(f"95th percentile concurrent executors: {p95}")

idle_rows = analyze_idle_time(executors)
avg_idle = np.mean([r["idle_pct"] for r in idle_rows])
print(f"Average Executor Idle %: {avg_idle:.1f} %")

recommendation = recommend_max_executors(exe_curve, idle_rows, target_idle_pct=15)
print(f"\nSuggested dynamicAllocation.maxExecutors based on idle slot metrics:")
print(f"  Current observed max (p95): {recommendation['current_p95_maxExecutors']}")
print(f"  Average Executor Idle Percentage: {recommendation['avg_idle_pct']:.2f}%")
print(
    f"  95th Percentile Executor Idle Percentage: {recommendation['p95_idle_pct']:.2f}%"
)
print(f"  Target max idle for tuning: {recommendation['target_idle_pct']}%")
print(
    f"  ---> Recommended spark.dynamicAllocation.maxExecutors: {recommendation['recommended_maxExecutors']}"
)

# Optional: Save CSV for graphing if needed
import csv

with open("executor_idle_report.csv", "w") as out:
    writer = csv.DictWriter(out, fieldnames=idle_rows[0].keys())
    writer.writeheader()
    for r in idle_rows:
        writer.writerow(r)

import pandas as pd
import matplotlib.pyplot as plt

# Load the output CSV generated previously
df = pd.read_csv("executor_idle_report.csv")

# Plot Idle Percentage per Executor
plt.figure(figsize=(12, 4))
plt.bar(df["id"], df["idle_pct"], color="orange")
plt.xlabel("Executor ID")
plt.ylabel("Idle %")
plt.title("Executor Idle Slot Percentage")
plt.tight_layout()
plt.show()

# Plot Stacked Bar: Task vs Idle Slot-seconds
plt.figure(figsize=(12, 5))
plt.bar(df["id"], df["task_time_sec"], label="Active Task Seconds", color="blue")
plt.bar(
    df["id"],
    df["idle_time_sec"],
    bottom=df["task_time_sec"],
    label="Idle Slot Seconds",
    color="orange",
)
plt.xlabel("Executor ID")
plt.ylabel("Seconds (slot-time)")
plt.title("Active vs Idle Slot Seconds per Executor")
plt.legend()
plt.tight_layout()
plt.show()
