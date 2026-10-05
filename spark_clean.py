"""NYC Taxi cleaning pipeline in Apache Spark (PySpark).

Usage:
    python spark_clean.py --master spark://localhost:7077     # 2-worker standalone cluster
    python spark_clean.py --master "local[4]"                 # quick local test

Steps: ingest -> cleanse -> heavy join + Python UDF -> export Parquet.
Then a separate UDF micro-benchmark (Python UDF vs Arrow pandas UDF vs native).
"""
import argparse
import os
import statistics
import time

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

from common import (END_TS, MAX_SPEED_MPH, OUTPUT_COLS, RAW_COLS, START_TS,
                    ResourceMonitor, avg_speed_mph, data_files, save_result)


JAVA_OPTS = ("-Dio.netty.tryReflectionSetAccessible=true "
             "--add-opens=java.base/java.nio=ALL-UNNAMED "
             "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")


def build_spark(master, executor_mem, executor_cores, max_cores):
    return (
        SparkSession.builder.appName("A3-Spark-NYC-Taxi-Clean")
        .master(master)
        .config("spark.executor.memory", executor_mem)
        .config("spark.executor.cores", executor_cores)
        .config("spark.cores.max", max_cores)
        .config("spark.driver.memory", "3g")
        # Arrow (pandas UDF) needs these JVM flags on Java 17+
        .config("spark.driver.extraJavaOptions", JAVA_OPTS)
        .config("spark.executor.extraJavaOptions", JAVA_OPTS)
        .config("spark.sql.shuffle.partitions", "16")
        # same clock as pandas/Ray: treat timestamps as UTC, no local-tz shifting
        .config("spark.sql.session.timeZone", "UTC")
        # write timestamps as microseconds (same as Ray/pyarrow) instead of legacy INT96
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )


def run_pipeline(spark, data_dir, out_dir):
    files = data_files(data_dir)

    # ---------- 1. INGESTION ----------
    trips = spark.read.parquet(*files).select(*RAW_COLS)
    trips = (
        trips.withColumn("VendorID", F.col("VendorID").cast("long"))
        .withColumn("PULocationID", F.col("PULocationID").cast("long"))
        .withColumn("DOLocationID", F.col("DOLocationID").cast("long"))
        .withColumn("passenger_count", F.col("passenger_count").cast("double"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
    )
    zones = (
        spark.read.csv(os.path.join(data_dir, "taxi_zone_lookup.csv"), header=True)
        .select(F.col("LocationID").cast("long").alias("LocationID"),
                # empty/missing names -> "Unknown" (same rule as Ray)
                F.coalesce(F.col("Borough"), F.lit("Unknown")).alias("Borough"),
                F.coalesce(F.col("Zone"), F.lit("Unknown")).alias("Zone"))
    )

    # ---------- 2. CLEANSING ----------
    trips = trips.dropna(subset=RAW_COLS)
    trips = trips.filter(
        (F.col("trip_distance") > 0)
        & (F.col("fare_amount") > 0)
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        & (F.col("tpep_pickup_datetime") >= F.lit(START_TS).cast("timestamp"))
        & (F.col("tpep_pickup_datetime") < F.lit(END_TS).cast("timestamp"))
    )
    trips = trips.dropDuplicates(RAW_COLS)
    # timestamp formatting
    trips = (
        trips.withColumn("pickup_date", F.date_format("tpep_pickup_datetime", "yyyy-MM-dd"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime").cast("long"))
        .withColumn(
            "trip_duration_sec",
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")).cast("double"),
        )
    )

    # ---------- 3. TRANSFORMATION ----------
    # Heavy join: trips x zone lookup (twice: pickup + dropoff)
    pu = zones.select(F.col("LocationID").alias("PULocationID"),
                      F.col("Borough").alias("pickup_borough"), F.col("Zone").alias("pickup_zone"))
    do = zones.select(F.col("LocationID").alias("DOLocationID"),
                      F.col("Borough").alias("dropoff_borough"), F.col("Zone").alias("dropoff_zone"))
    trips = trips.join(pu, on="PULocationID", how="inner").join(do, on="DOLocationID", how="inner")

    # Custom Python UDF (runs in Python worker processes, rows shipped from the JVM)
    speed_udf = F.udf(avg_speed_mph, DoubleType())
    trips = trips.withColumn("speed_mph", speed_udf("trip_distance", "trip_duration_sec"))
    trips = trips.filter(F.col("speed_mph").isNotNull())

    # Average Speed per Hour (aggregate, then joined back onto every trip)
    hourly = trips.groupBy("pickup_hour").agg(F.avg("speed_mph").alias("avg_speed_hour"))
    trips = trips.join(hourly, on="pickup_hour", how="inner")

    # ---------- 4. EXPORT ----------
    trips.select(*OUTPUT_COLS).write.mode("overwrite").parquet(out_dir)
    return len(files)


def udf_benchmark(spark, out_dir, repeats=1):
    """Times ONLY the speed calculation on the already-cleaned data, three ways."""
    base = spark.read.parquet(out_dir).select("trip_distance", "trip_duration_sec").cache()
    n = base.count()

    py_udf = F.udf(avg_speed_mph, DoubleType())

    @pandas_udf(DoubleType())
    def arrow_udf(dist: pd.Series, dur: pd.Series) -> pd.Series:
        return pd.Series([avg_speed_mph(d, s) for d, s in zip(dist, dur)])

    dur = F.col("trip_duration_sec")
    native = F.when(dur <= 0, None).otherwise(F.col("trip_distance") / (dur / 3600.0))
    native = F.when(native > MAX_SPEED_MPH, None).otherwise(F.round(native, 4))

    variants = {
        "python_udf_sec": py_udf("trip_distance", "trip_duration_sec"),
        "arrow_pandas_udf_sec": arrow_udf("trip_distance", "trip_duration_sec"),
        "native_expr_sec": native,
    }
    out = {"udf_bench_rows": n}
    for name, expr in variants.items():
        times = []
        try:
            for _ in range(repeats):
                t = time.perf_counter()
                base.select(expr.alias("s")).agg(F.sum("s")).collect()
                times.append(time.perf_counter() - t)
            out[name] = round(statistics.median(times), 3)
        except Exception as e:  # Arrow can fail on some JDK versions; don't lose the run
            print(f"[udf bench] {name} failed: {str(e)[:150]}")
            out[name] = None
    out["udf_overhead_sec"] = round(out["python_udf_sec"] - out["native_expr_sec"], 3)
    base.unpersist()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default=os.environ.get("SPARK_MASTER", "spark://localhost:7077"))
    ap.add_argument("--data", default="data")
    ap.add_argument("--out", default="output/spark_clean.parquet")
    ap.add_argument("--executor-memory", default="2g")
    ap.add_argument("--executor-cores", default="2")
    ap.add_argument("--max-cores", default="4")
    ap.add_argument("--skip-udf-bench", action="store_true")
    args = ap.parse_args()

    spark = build_spark(args.master, args.executor_memory, args.executor_cores, args.max_cores)
    spark.sparkContext.setLogLevel("WARN")
    # ship common.py (contains the UDF) to the executors on both workers
    spark.sparkContext.addPyFile(os.path.join(os.path.dirname(os.path.abspath(__file__)), "common.py"))
    print(f"Spark {spark.version} | master={args.master} | UI: {spark.sparkContext.uiWebUrl}")

    mon = ResourceMonitor().start()
    t0 = time.perf_counter()
    n_files = run_pipeline(spark, args.data, args.out)
    total = time.perf_counter() - t0
    res = mon.stop()

    rows_out = spark.read.parquet(args.out).count()
    result = {"total_time_sec": round(total, 3), "rows_out": rows_out,
              "input_files": n_files, "master": args.master, **res}
    if not args.skip_udf_bench:
        result.update(udf_benchmark(spark, args.out))
    save_result("spark", result, mon)
    if os.environ.get("HOLD_UI"):
        input("Spark UI still up at port 4040 - take screenshots, then press Enter to exit...")
    spark.stop()


if __name__ == "__main__":
    main()
