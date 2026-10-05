"""Shared definitions used by BOTH spark_clean.py and ray_clean.py.

Keeping the column list, filters and the Python UDF in one file guarantees
the two pipelines apply exactly the same logic (pipeline parity).
"""
import json
import os
import threading
import time
from datetime import datetime

import psutil

# Columns we keep from the raw NYC Yellow Taxi files
RAW_COLS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
]

# Only keep trips whose pickup falls inside this window (raw data has junk years like 2002/2009)
START_TS = "2024-01-01 00:00:00"
END_TS = "2025-01-01 00:00:00"

# Final column order of the exported, ML-ready table (same for Spark and Ray)
OUTPUT_COLS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pickup_date",
    "pickup_hour",
    "trip_duration_sec",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "PULocationID",
    "pickup_borough",
    "pickup_zone",
    "DOLocationID",
    "dropoff_borough",
    "dropoff_zone",
    "speed_mph",
    "avg_speed_hour",
]

MAX_SPEED_MPH = 100.0


def avg_speed_mph(distance_miles, duration_sec):
    """The custom Python UDF (identical in both frameworks).

    Returns the trip's average speed in miles per hour, or None for
    impossible trips (zero/negative duration, > 100 mph).
    """
    if distance_miles is None or duration_sec is None:
        return None
    if duration_sec <= 0:
        return None
    speed = float(distance_miles) / (float(duration_sec) / 3600.0)
    if speed > MAX_SPEED_MPH:
        return None
    return round(speed, 4)


def data_files(data_dir):
    files = sorted(
        os.path.join(data_dir, f)
        for f in os.listdir(data_dir)
        if f.startswith("yellow_tripdata_") and f.endswith(".parquet")
    )
    if not files:
        raise SystemExit(f"No yellow_tripdata_*.parquet files found in {data_dir}")
    return files


class ResourceMonitor:
    """Samples system-wide CPU % and used RAM every `interval` seconds in a
    background thread. System-wide is used on purpose: Spark executors (JVM)
    and Ray workers run as separate processes, so per-process numbers would
    miss them. A baseline is recorded before the run starts."""

    def __init__(self, interval=0.5):
        self.interval = interval
        self.samples = []  # (t, cpu_percent, mem_used_gb)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        psutil.cpu_percent(interval=None)  # prime the counter
        time.sleep(0.5)
        self.baseline_mem_gb = psutil.virtual_memory().used / 1e9

    def _run(self):
        t0 = time.time()
        while not self._stop.is_set():
            cpu = psutil.cpu_percent(interval=None)
            mem = psutil.virtual_memory().used / 1e9
            self.samples.append((round(time.time() - t0, 2), cpu, round(mem, 3)))
            time.sleep(self.interval)

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        self._thread.join()
        cpus = [s[1] for s in self.samples] or [0]
        mems = [s[2] for s in self.samples] or [self.baseline_mem_gb]
        return {
            "peak_cpu_percent": max(cpus),
            "avg_cpu_percent": round(sum(cpus) / len(cpus), 1),
            "peak_mem_used_gb": max(mems),
            "baseline_mem_gb": round(self.baseline_mem_gb, 3),
            "peak_mem_increase_gb": round(max(mems) - self.baseline_mem_gb, 3),
            "cpu_count": psutil.cpu_count(),
            "total_mem_gb": round(psutil.virtual_memory().total / 1e9, 2),
        }

    def save_timeline(self, path):
        with open(path, "w") as f:
            f.write("t_sec,cpu_percent,mem_used_gb\n")
            for s in self.samples:
                f.write(f"{s[0]},{s[1]},{s[2]}\n")


def save_result(framework, result, monitor, results_dir="results"):
    os.makedirs(results_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.join(results_dir, f"{framework}_{stamp}")
    monitor.save_timeline(base + "_timeline.csv")
    result["framework"] = framework
    result["timestamp"] = stamp
    result["timeline_csv"] = base + "_timeline.csv"
    with open(base + ".json", "w") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))
    print(f"\nSaved -> {base}.json")
