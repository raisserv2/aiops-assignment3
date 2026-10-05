# AIOps Assignment 3 - Spark vs. Ray: The Data Engineering Duel

Mohammed Khaja Ghouse Mohiuddin (DA24B015)

The same NYC Taxi cleaning pipeline built in Apache Spark and in Ray Data, run on a 2-worker cluster for each, and benchmarked.

## Where to find each deliverable

| Deliverable | Location |
|---|---|
| Spark pipeline | `spark_clean.py` |
| Ray pipeline | `ray_clean.py` |
| Shared logic (columns, filters, the Python UDF, resource monitor) | `common.py` |
| Proof that both outputs are identical | `verify_parity.py`, result in `results/parity.json` |
| Spark Master UI with 2 workers, Spark app UI (4040) | `screenshots/` |
| Ray Dashboard cluster/resource view (8265) | `screenshots/` |
| Raw benchmark numbers for every run | `results/spark_*.json`, `results/ray_*.json` (+ CPU/RAM timelines `*_timeline.csv`) |
| Benchmark report (graphs, UDF deep-dive, winner, AI-first vs BI-first) | `report.pdf` |
| Cluster setup steps | `CLUSTER_SETUP.md` |
| AI usage | `AI_DISCLOSURE.md` and Section 8 of the report |

## Pipeline

1. **Ingestion:** all `data/yellow_tripdata_*.parquet` files + `data/taxi_zone_lookup.csv`
2. **Cleansing:** drop nulls, drop invalid trips (distance/fare <= 0, dropoff before pickup, pickup outside 2024), remove exact duplicates, format timestamps (`pickup_date`, `pickup_hour`, `trip_duration_sec`)
3. **Transformation:** inner join with the zone table on pickup and dropoff location; custom Python UDF `avg_speed_mph`; average speed per pickup hour joined back onto each trip
4. **Export:** Parquet in `output/`

## Setup

```bash
sudo apt install -y openjdk-17-jre-headless
conda create -n a3 python=3.10 -y && conda activate a3
pip install pyspark==3.5.3 "ray[data,default]==2.40.0" click==8.1.7 pandas==2.3.3 numpy==2.2.6 pyarrow==25.0.1 psutil==7.2.2 matplotlib==3.10.9

mkdir -p data && cd data
for m in 01 02 03; do wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-$m.parquet; done
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
cd ..
```

## Run

```bash
# 1. Spark: start the cluster (CLUSTER_SETUP.md), then
python spark_clean.py --master spark://localhost:7077      # writes results/spark_<time>.json
# stop the Spark cluster

# 2. Ray: start the cluster (CLUSTER_SETUP.md), then
python ray_clean.py --address auto                          # writes results/ray_<time>.json
# stop the Ray cluster

# 3. check both outputs are identical
python verify_parity.py

# 4. graphs + report (open report.html, print to PDF)
python make_report.py
```

Each script prints and saves: total wall-clock time (ingestion to export), peak/average CPU, peak RAM, and a UDF micro-benchmark
(Python UDF vs native expression on the cleaned data, one timing per run, averaged over runs; Spark also tries the Arrow pandas UDF).
Each benchmark is repeated 3 times. The report shows mean ± std.
