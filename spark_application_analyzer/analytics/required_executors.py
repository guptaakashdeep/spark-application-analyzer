import numpy as np
from typing import List
from models.executor_metrics import ExecutorMetrics
from utils.cli_colors import Colors


def _parse_executor_events(executors):
    # Yields (start, end, cores) for each executor
    periods = []
    for ex in executors:
        start = ex.add_time
        end = ex.remove_time
        cores = ex.max_tasks if ex.max_tasks else ex.total_cores
        periods.append((start, end, cores))
    return periods


def _max_concurrent_executors(periods):
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


def _percentile_concurrency(values, perc=95):
    return int(np.percentile(values, perc))


def _analyze_idle_time(executors):
    # Each executor record in the REST API output has:
    # addTime, removeTime, totalCores, totalDuration
    # totalDuration is the sum of task run time on this executor (ms)
    rows = []
    for ex in executors:
        start = ex.add_time
        end = ex.remove_time
        cores = ex.max_tasks if ex.max_tasks else ex.total_cores
        uptime_ms = end - start
        slot_time_ms = uptime_ms * cores
        total_task_time_ms = ex.total_duration if ex.total_duration else 0
        idle_time_ms = slot_time_ms - total_task_time_ms
        idle_pct = (idle_time_ms / slot_time_ms) * 100 if slot_time_ms > 0 else 0
        rows.append(
            {
                "id": ex.id,
                "uptime_sec": uptime_ms / 1000,
                "cores": cores,
                "slot_time_sec": slot_time_ms / 1000,
                "task_time_sec": total_task_time_ms / 1000,
                "idle_time_sec": idle_time_ms / 1000,
                "idle_pct": idle_pct,
            }
        )
    return rows


def _recommend_max_executors(concurrency_curve, idle_rows, target_idle_pct=15):
    # We want to find maxExecutors where the overall idle % gets into target_band (default: 15%)
    max_executors_p95 = _percentile_concurrency(concurrency_curve, 100)
    avg_idle_pct = np.mean([r["idle_pct"] for r in idle_rows])
    p95_idle_pct = np.percentile([r["idle_pct"] for r in idle_rows], 95)
    p50_idle_pct = np.percentile([r["idle_pct"] for r in idle_rows], 50)

    print(f"{Colors.YELLOW}{Colors.BOLD}p50 idle percentage: {p95_idle_pct}")
    print(f"{Colors.YELLOW}{Colors.BOLD}avg idle percentage: {avg_idle_pct}")

    adjustment_factor = 1 - (avg_idle_pct / 100.0)
    # Only adjust if idle is above target band. If below, recommend current value.
    if avg_idle_pct > target_idle_pct:
        print(
            f"{Colors.RED}{Colors.BOLD} Idle percentage is above {target_idle_pct}% {Colors.END}"
        )
        recommended_max = max(int(max_executors_p95 * adjustment_factor), 1)
    else:
        recommended_max = max_executors_p95

    return {
        "current_p95_maxExecutors": max_executors_p95,
        "avg_idle_pct": avg_idle_pct,
        "p95_idle_pct": p95_idle_pct,
        "recommended_maxExecutors": recommended_max,
        "target_idle_pct": target_idle_pct,
        # "p50_idle_pct": p50_idle_pct
    }


def recommend_num_executors(executor_metrics: List[ExecutorMetrics]):
    # executors = executor_metrics.to_dict()
    # Optimal executor/core concurrency (as before)
    periods = _parse_executor_events(executor_metrics)
    times, exe_curve, peak = _max_concurrent_executors(periods)
    p95 = _percentile_concurrency(exe_curve, 99)
    cores_per_executor = periods[0][2] if periods else 1
    print(f"Peak concurrent executors: {peak}")
    print(f"95th percentile concurrent executors: {p95}")

    idle_rows = _analyze_idle_time(executor_metrics)
    avg_idle = np.mean([r["idle_pct"] for r in idle_rows])
    print(f"Average Executor Idle %: {avg_idle:.1f} %")

    # Recommendation calculation
    recommendation = _recommend_max_executors(exe_curve, idle_rows, target_idle_pct=15)
    print(
        f"{Colors.GREEN}{Colors.BOLD}Suggested dynamicAllocation.maxExecutors based on idle slot metrics:"
    )
    print(f"  Current observed max (p95): {recommendation['current_p95_maxExecutors']}")
    print(f"  Average Executor Idle Percentage: {recommendation['avg_idle_pct']:.2f}%")
    print(
        f"  95th Percentile Executor Idle Percentage: {recommendation['p95_idle_pct']:.2f}%"
    )
    print(f"  Target max idle for tuning: {recommendation['target_idle_pct']}%")
    print(
        f"  ---> Recommended spark.dynamicAllocation.maxExecutors: {recommendation['recommended_maxExecutors']} {Colors.END}"
    )
    return recommendation
