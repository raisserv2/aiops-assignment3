# Cluster Setup (2 workers each, single laptop)

Both clusters run on one machine. Each framework gets **2 workers x 2 CPUs** (4 CPUs total), so the comparison is fair.
Run **one cluster at a time**, never both together.

Every terminal that starts a cluster or runs a script needs this first:
```bash
conda activate a3
cd ~/aiops-a3
export PYSPARK_PYTHON=$(which python)
export SPARK_HOME=$(python -c "import pyspark,os;print(os.path.dirname(pyspark.__file__))")
mkdir -p logs spark-work screenshots results
```

## Spark standalone

pip PySpark has no `start-master.sh`, so the Master and Worker daemons are started with `spark-class`.

```bash
# Master: cluster manager, UI on :8080
nohup $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
  --host localhost --port 7077 --webui-port 8080 > logs/spark-master.log 2>&1 &
sleep 5

# Worker 1: offers 2 cores + 3 GB
nohup $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --cores 2 --memory 3g --webui-port 8081 --work-dir $PWD/spark-work/w1 \
  spark://localhost:7077 > logs/spark-worker1.log 2>&1 &

# Worker 2: same, different UI port and scratch dir
nohup $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --cores 2 --memory 3g --webui-port 8082 --work-dir $PWD/spark-work/w2 \
  spark://localhost:7077 > logs/spark-worker2.log 2>&1 &
sleep 8
curl -s localhost:8080/json/ | python -c "import json,sys;d=json.load(sys.stdin);print('alive workers:',d['aliveworkers'])"
```
`spark_clean.py` asks for `spark.executor.cores=2`, `spark.cores.max=4` and `spark.executor.memory=2g`, which gives exactly one executor on each worker.
The application UI is on :4040 while the app runs.

Stop:
```bash
pkill -f deploy.worker.Worker; pkill -f deploy.master.Master
```

## Ray

```bash
# Head: GCS + dashboard on :8265. 0 CPUs, so it only coordinates.
ray start --head --port=6379 --num-cpus=0 --object-store-memory=1000000000 \
  --dashboard-host=0.0.0.0 --dashboard-port=8265 --disable-usage-stats

# Worker nodes 1 and 2: 2 CPUs, 3 GB task memory, 1.5 GB shared object store each
ray start --address=127.0.0.1:6379 --num-cpus=2 --memory=3000000000 --object-store-memory=1500000000 --disable-usage-stats
ray start --address=127.0.0.1:6379 --num-cpus=2 --memory=3000000000 --object-store-memory=1500000000 --disable-usage-stats

ray status     # 3 active nodes, 0.0/4.0 CPU
```
`ray_clean.py` connects with `ray.init(address="auto")` and ships `common.py` to the workers through `runtime_env`.
If data does not fit in the object store, Ray spills it to disk (`/tmp/ray`). Memory never blows up.

Stop:
```bash
ray stop --force
```

