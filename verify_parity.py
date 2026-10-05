"""Checks that the Spark and Ray outputs are identical.

Streams both Parquet outputs batch by batch (low memory) and compares:
  1. row counts
  2. an order-independent fingerprint of every row (sum of per-row hashes)
  3. column sums
  4. the 24 "average speed per hour" values

Usage: python verify_parity.py [spark_out] [ray_out]
"""
import json
import os
import sys

import numpy as np
import pandas as pd
import pyarrow.dataset as pads

from common import OUTPUT_COLS

ROW_COLS = [c for c in OUTPUT_COLS if c != "avg_speed_hour"]  # float avg compared separately
SUM_COLS = ["passenger_count", "trip_distance", "fare_amount", "total_amount", "trip_duration_sec", "speed_mph"]
MOD = 2 ** 64


def normalise(df):
    for c in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        df[c] = pd.to_datetime(df[c]).dt.tz_localize(None).astype("datetime64[us]").astype("int64")
    for c in ("VendorID", "pickup_hour", "PULocationID", "DOLocationID"):
        df[c] = df[c].astype("int64")
    for c in SUM_COLS + ["avg_speed_hour"]:
        df[c] = df[c].astype("float64") + 0.0  # -0.0 == 0.0
    for c in ("pickup_date", "pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone"):
        df[c] = df[c].astype(str)
    return df


def profile(path):
    ds = pads.dataset(path, format="parquet")
    rows, fp = 0, 0
    sums = {c: 0.0 for c in SUM_COLS}
    hourly = {}
    for batch in ds.to_batches(columns=OUTPUT_COLS, batch_size=500_000):
        df = normalise(batch.to_pandas())
        rows += len(df)
        h = pd.util.hash_pandas_object(df[ROW_COLS], index=False).to_numpy(dtype=np.uint64)
        fp = (fp + int(h.sum(dtype=np.uint64))) % MOD
        for c in SUM_COLS:
            sums[c] += float(df[c].sum())
        for hr, v in df.groupby("pickup_hour")["avg_speed_hour"].first().items():
            hourly[int(hr)] = v
    return {"rows": rows, "fingerprint": fp, "sums": sums, "hourly": dict(sorted(hourly.items()))}


def main():
    spark_out = sys.argv[1] if len(sys.argv) > 1 else "output/spark_clean.parquet"
    ray_out = sys.argv[2] if len(sys.argv) > 2 else "output/ray_clean.parquet"
    s, r = profile(spark_out), profile(ray_out)

    checks = {}
    checks["row_count_equal"] = s["rows"] == r["rows"]
    checks["row_fingerprint_equal"] = s["fingerprint"] == r["fingerprint"]
    checks["column_sums_equal"] = all(np.isclose(s["sums"][c], r["sums"][c], rtol=1e-9) for c in SUM_COLS)
    max_diff = max(abs(s["hourly"][h] - r["hourly"].get(h, np.inf)) for h in s["hourly"])
    checks["hourly_avg_equal"] = set(s["hourly"]) == set(r["hourly"]) and max_diff < 1e-9

    print(f"{'check':28s} spark vs ray")
    print(f"{'rows':28s} {s['rows']:,} vs {r['rows']:,}")
    print(f"{'row fingerprint':28s} {s['fingerprint']} vs {r['fingerprint']}")
    for c in SUM_COLS:
        print(f"{'sum(' + c + ')':28s} {s['sums'][c]:.4f} vs {r['sums'][c]:.4f}")
    print(f"{'max |hourly avg diff|':28s} {max_diff:.2e}")
    print()
    for k, v in checks.items():
        print(f"  [{'PASS' if v else 'FAIL'}] {k}")
    ok = all(checks.values())
    print("\nPARITY:", "PASS - outputs are identical" if ok else "FAIL")

    os.makedirs("results", exist_ok=True)
    with open("results/parity.json", "w") as f:
        json.dump({"checks": checks, "spark": s, "ray": r, "max_hourly_diff": max_diff,
                   "passed": ok}, f, indent=2, default=str)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
