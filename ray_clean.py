"""NYC Taxi cleaning pipeline in Ray Data.

Usage:
    python ray_clean.py --address auto      # connect to the running 2-worker Ray cluster
    python ray_clean.py --address local     # quick local test (starts a throwaway Ray)

Same steps and same logic as spark_clean.py:
ingest -> cleanse -> heavy join + Python UDF -> export Parquet.
Then a separate UDF micro-benchmark (row-by-row Python vs vectorised).
"""
import argparse
import os
import shutil
import statistics
import time

import numpy as np
import pandas as pd
import ray

from common import (END_TS, MAX_SPEED_MPH, OUTPUT_COLS, RAW_COLS, START_TS,
                    ResourceMonitor, avg_speed_mph, data_files, save_result)

N_DEDUP_BUCKETS = 32
HERE = os.path.dirname(os.path.abspath(__file__))


# ---------------- batch functions (run inside Ray workers) ----------------
def cast_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    for c in ("VendorID", "PULocationID", "DOLocationID"):
        df[c] = df[c].astype("Int64")
    for c in ("passenger_count", "trip_distance", "fare_amount", "total_amount"):
        # "+ 0.0" turns -0.0 into 0.0, which Spark also does inside dropDuplicates
        df[c] = df[c].astype("float64") + 0.0
    df = df.dropna(subset=RAW_COLS)
    for c in ("VendorID", "PULocationID", "DOLocationID"):
        df[c] = df[c].astype("int64")
    pu, do = df["tpep_pickup_datetime"], df["tpep_dropoff_datetime"]
    keep = (
        (df["trip_distance"] > 0)
        & (df["fare_amount"] > 0)
        & (do > pu)
        & (pu >= pd.Timestamp(START_TS))
        & (pu < pd.Timestamp(END_TS))
    )
    df = df[keep].copy()
    # bucket id for distributed de-duplication: identical rows -> identical hash -> same bucket
    df["_bucket"] = (pd.util.hash_pandas_object(df[RAW_COLS], index=False) % N_DEDUP_BUCKETS).astype("int64")
    return df


def drop_dupes_in_bucket(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset=RAW_COLS).drop(columns=["_bucket"])


def format_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    pu, do = df["tpep_pickup_datetime"], df["tpep_dropoff_datetime"]
    df["pickup_date"] = pu.dt.strftime("%Y-%m-%d")
    df["pickup_hour"] = pu.dt.hour.astype("int64")
    # whole seconds, exactly like Spark's unix_timestamp(drop) - unix_timestamp(pickup)
    df["trip_duration_sec"] = (do.dt.floor("s") - pu.dt.floor("s")).dt.total_seconds().astype("float64")
    return df


def make_zone_join(zones: pd.DataFrame):
    pu = zones.rename(columns={"LocationID": "PULocationID", "Borough": "pickup_borough", "Zone": "pickup_zone"})
    do = zones.rename(columns={"LocationID": "DOLocationID", "Borough": "dropoff_borough", "Zone": "dropoff_zone"})

    def join_zones(df: pd.DataFrame) -> pd.DataFrame:
        df = df.merge(pu, on="PULocationID", how="inner")
        return df.merge(do, on="DOLocationID", how="inner")

    return join_zones


def apply_speed_udf(df: pd.DataFrame) -> pd.DataFrame:
    # the SAME row-level Python function Spark uses, applied natively in Python
    df["speed_mph"] = [avg_speed_mph(d, s) for d, s in zip(df["trip_distance"], df["trip_duration_sec"])]
    df["speed_mph"] = df["speed_mph"].astype("float64")
    return df[df["speed_mph"].notna()]


def hourly_partial(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("pickup_hour")["speed_mph"].agg(["sum", "count"]).reset_index()
    return g.rename(columns={"sum": "s", "count": "n"})


def make_hourly_join(hourly: pd.DataFrame):
    def join_hourly(df: pd.DataFrame) -> pd.DataFrame:
        df = df.merge(hourly, on="pickup_hour", how="inner")
        return df[OUTPUT_COLS]
    return join_hourly


# ---------------- pipeline ----------------
def run_pipeline(data_dir, out_dir):
    files = data_files(data_dir)

    # 1. INGESTION
    ds = ray.data.read_parquet(files, columns=RAW_COLS)
    # keep_default_na=False: the real file has Zone "NA"/"N/A" which pandas would turn into NaN
    zones = pd.read_csv(os.path.join(data_dir, "taxi_zone_lookup.csv"), keep_default_na=False,
                        dtype={"Borough": str, "Zone": str})[["LocationID", "Borough", "Zone"]]
    zones[["Borough", "Zone"]] = zones[["Borough", "Zone"]].replace("", "Unknown")
    zones["LocationID"] = zones["LocationID"].astype("int64")

    # 2. CLEANSING (nulls + filters, then distributed de-dup, then timestamp formatting)
    ds = ds.map_batches(cast_and_clean, batch_format="pandas")
    ds = ds.groupby("_bucket").map_groups(drop_dupes_in_bucket, batch_format="pandas")
    ds = ds.map_batches(format_timestamps, batch_format="pandas")

    # 3. TRANSFORMATION: heavy join (broadcast lookup) + Python UDF + hourly average
    ds = ds.map_batches(make_zone_join(zones), batch_format="pandas")
    ds = ds.map_batches(apply_speed_udf, batch_format="pandas")
    ds = ds.materialize()  # needed twice below (aggregate + join back)
    # map-side partial aggregates (sum, count per hour) -> tiny table -> final average.
    # (equivalent to groupby().mean(), but avoids a full sort-shuffle of every row)
    partial = ds.map_batches(hourly_partial, batch_format="pandas").to_pandas()
    hourly = partial.groupby("pickup_hour", as_index=False)[["s", "n"]].sum()
    hourly["avg_speed_hour"] = hourly["s"] / hourly["n"]
    hourly = hourly[["pickup_hour", "avg_speed_hour"]].astype({"pickup_hour": "int64"})
    ds = ds.map_batches(make_hourly_join(hourly), batch_format="pandas")

    # 4. EXPORT
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    ds.write_parquet(out_dir)
    return len(files)


def udf_benchmark(out_dir, repeats=1):
    base = ray.data.read_parquet(out_dir, columns=["trip_distance", "trip_duration_sec"]).materialize()
    n = base.count()

    def python_rowwise(df):
        return pd.DataFrame({"s": [avg_speed_mph(d, s) for d, s in zip(df["trip_distance"], df["trip_duration_sec"])]},
                            dtype="float64")

    def vectorised(df):
        dur = df["trip_duration_sec"].to_numpy()
        with np.errstate(divide="ignore", invalid="ignore"):
            s = df["trip_distance"].to_numpy() / (dur / 3600.0)
        s = np.where((dur <= 0) | (s > MAX_SPEED_MPH), np.nan, np.round(s, 4))
        return pd.DataFrame({"s": s})

    out = {"udf_bench_rows": n}
    for name, fn in {"python_udf_sec": python_rowwise, "native_expr_sec": vectorised}.items():
        times = []
        for _ in range(repeats):
            t = time.perf_counter()
            base.map_batches(fn, batch_format="pandas").sum("s")
            times.append(time.perf_counter() - t)
        out[name] = round(statistics.median(times), 3)
    out["arrow_pandas_udf_sec"] = None  # Spark-only concept
    out["udf_overhead_sec"] = round(out["python_udf_sec"] - out["native_expr_sec"], 3)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--address", default="auto", help="'auto' = existing cluster, 'local' = throwaway local Ray")
    ap.add_argument("--data", default="data")
    ap.add_argument("--out", default="output/ray_clean.parquet")
    ap.add_argument("--skip-udf-bench", action="store_true")
    args = ap.parse_args()
    args.data, args.out = os.path.abspath(args.data), os.path.abspath(args.out)

    # ship this folder's code (common.py) to every worker, but not the data
    runtime_env = {"working_dir": HERE, "excludes": ["data/", "output/", "results/", "screenshots/", "*.parquet", ".git/"]}
    ray.init(address=None if args.address == "local" else args.address,
             runtime_env=runtime_env, log_to_driver=False)
    ctx = ray.data.DataContext.get_current()
    ctx.enable_progress_bars = True
    print("Ray cluster resources:", ray.cluster_resources())
    print("Nodes alive:", sum(1 for n in ray.nodes() if n["Alive"]))

    mon = ResourceMonitor().start()
    t0 = time.perf_counter()
    n_files = run_pipeline(args.data, args.out)
    total = time.perf_counter() - t0
    res = mon.stop()

    rows_out = ray.data.read_parquet(args.out).count()
    result = {"total_time_sec": round(total, 3), "rows_out": rows_out,
              "input_files": n_files, "master": args.address, **res}
    if not args.skip_udf_bench:
        result.update(udf_benchmark(args.out))
    save_result("ray", result, mon)
    if os.environ.get("HOLD_UI"):
        input("Ray dashboard at port 8265 - take screenshots, then press Enter to exit...")
    ray.shutdown()


if __name__ == "__main__":
    main()
