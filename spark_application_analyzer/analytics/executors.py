import numpy as np
from typing import List, Dict, Any
from .base import BaseExecutorStrategy
from spark_application_analyzer.models.executor_metrics import ExecutorMetrics
from spark_application_analyzer.utils.cli_colors import Colors


class IdleTimeStrategy(BaseExecutorStrategy):
    """
    Recommends the number of executors based on executor idle time.
    """

    def _parse_executor_events(self, executors: List[ExecutorMetrics]):
        periods = []
        for ex in executors:
            start = ex.add_time
            end = ex.remove_time
            cores = ex.max_tasks if ex.max_tasks else ex.total_cores
            periods.append((start, end, cores))
        return periods

    def _max_concurrent_executors(self, periods):
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

    def _percentile_concurrency(self, values, perc=95):
        return int(np.percentile(values, perc))

    def _analyze_idle_time(self, executors: List[ExecutorMetrics]):
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

    def _recommend_max_executors(
        self, concurrency_curve, idle_rows, target_idle_pct=15
    ):
        max_executors_p95 = self._percentile_concurrency(concurrency_curve, 95)
        avg_idle_pct = np.mean([r["idle_pct"] for r in idle_rows])
        p95_idle_pct = np.percentile([r["idle_pct"] for r in idle_rows], 95)

        print(f"{Colors.YELLOW}{Colors.BOLD}p95 idle percentage: {p95_idle_pct}")
        print(f"{Colors.YELLOW}{Colors.BOLD}avg idle percentage: {avg_idle_pct}")

        adjustment_factor = 1 - (avg_idle_pct / 100.0)
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
        }

    def generate_recommendation(
        self, executor_metrics: List[ExecutorMetrics]
    ) -> Dict[str, Any]:
        if not executor_metrics:
            return {}
        periods = self._parse_executor_events(executor_metrics)
        times, exe_curve, peak = self._max_concurrent_executors(periods)
        p95 = self._percentile_concurrency(exe_curve, 99)
        print(f"Peak concurrent executors: {peak}")
        print(f"95th percentile concurrent executors: {p95}")

        idle_rows = self._analyze_idle_time(executor_metrics)
        avg_idle = np.mean([r["idle_pct"] for r in idle_rows])
        print(f"Average Executor Idle %: {avg_idle:.1f} %")

        recommendation = self._recommend_max_executors(
            exe_curve, idle_rows, target_idle_pct=15
        )
        print(
            f"{Colors.GREEN}{Colors.BOLD}Suggested dynamicAllocation.maxExecutors based on idle slot metrics:"
        )
        print(
            f"  Current observed max (p95): {recommendation['current_p95_maxExecutors']}"
        )
        print(
            f"  Average Executor Idle Percentage: {recommendation['avg_idle_pct']:.2f}%"
        )
        print(
            f"  95th Percentile Executor Idle Percentage: {recommendation['p95_idle_pct']:.2f}%"
        )
        print(f"  Target max idle for tuning: {recommendation['target_idle_pct']}%")
        print(
            f"  ---> Recommended spark.dynamicAllocation.maxExecutors: {recommendation['recommended_maxExecutors']} {Colors.END}"
        )
        return recommendation


# ------- REVISIT LATER --------
# Optional: Save CSV for graphing if needed
# import csv

# with open("executor_idle_report.csv", "w") as out:
#     writer = csv.DictWriter(out, fieldnames=idle_rows[0].keys())
#     writer.writeheader()
#     for r in idle_rows:
#         writer.writerow(r)

# import pandas as pd
# import matplotlib.pyplot as plt

# # Load the output CSV generated previously
# df = pd.read_csv("executor_idle_report.csv")

# # Plot Idle Percentage per Executor
# plt.figure(figsize=(12, 4))
# plt.bar(df["id"], df["idle_pct"], color="orange")
# plt.xlabel("Executor ID")
# plt.ylabel("Idle %")
# plt.title("Executor Idle Slot Percentage")
# plt.tight_layout()
# plt.show()

# # Plot Stacked Bar: Task vs Idle Slot-seconds
# plt.figure(figsize=(12, 5))
# plt.bar(df["id"], df["task_time_sec"], label="Active Task Seconds", color="blue")
# plt.bar(
#     df["id"],
#     df["idle_time_sec"],
#     bottom=df["task_time_sec"],
#     label="Idle Slot Seconds",
#     color="orange",
# )
# plt.xlabel("Executor ID")
# plt.ylabel("Seconds (slot-time)")
# plt.title("Active vs Idle Slot Seconds per Executor")
# plt.legend()
# plt.tight_layout()
# plt.show()
