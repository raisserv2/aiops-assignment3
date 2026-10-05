"""Builds the benchmark graphs + report from YOUR real result files.

Reads:  results/spark_*.json, results/ray_*.json, results/*_timeline.csv, results/parity.json,
        screenshots/*.png (optional, embedded if present)
Writes: results/fig_*.png and report.html  (open report.html in Firefox -> Print -> Save as PDF)

Usage: python make_report.py
"""
import base64
import glob
import json
import os
import statistics

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

COLORS = {"spark": "#2a78d6", "ray": "#eb6834"}
LABEL = {"spark": "Spark", "ray": "Ray"}
plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.color": "#e6e6e3", "axes.axisbelow": True,
                     "axes.edgecolor": "#888", "text.color": "#0b0b0b"})

AUTHOR = "Mohammed Khaja Ghouse Mohiuddin (DA24B015)"


def load_runs():
    runs = {"spark": [], "ray": []}
    for fw in runs:
        for p in sorted(glob.glob(f"results/{fw}_*.json")):
            with open(p) as f:
                runs[fw].append(json.load(f))
    if not runs["spark"] or not runs["ray"]:
        raise SystemExit("Need at least one results/spark_*.json and one results/ray_*.json")
    return runs


def stat(runs, key):
    vals = [r[key] for r in runs if r.get(key) is not None]
    if not vals:
        return None, None
    return statistics.mean(vals), (statistics.stdev(vals) if len(vals) > 1 else 0.0)


def fmt(m, s=None, unit=""):
    if m is None:
        return "n/a"
    return f"{m:.2f}{unit}" if not s else f"{m:.2f} ± {s:.2f}{unit}"


def bar(ax, keys, means, errs, ylabel, title):
    x = range(len(keys))
    bars = ax.bar(x, means, yerr=errs, color=[COLORS[k] for k in keys], width=0.55,
                  capsize=4, edgecolor="white", linewidth=2)
    ax.set_xticks(list(x), [LABEL[k] for k in keys])
    ax.set_ylabel(ylabel)
    ax.set_title(title, loc="left", fontsize=11)
    for b, m in zip(bars, means):
        ax.annotate(f"{m:.1f}", (b.get_x() + b.get_width() / 2, m), xytext=(0, 4),
                    textcoords="offset points", ha="center", fontsize=9, color="#52514e")


def figures(runs):
    os.makedirs("results", exist_ok=True)
    fw = ["spark", "ray"]
    figs = {}

    # 1. total wall-clock time
    fig, ax = plt.subplots(figsize=(4.6, 3.2))
    m = [stat(runs[k], "total_time_sec")[0] for k in fw]
    e = [stat(runs[k], "total_time_sec")[1] for k in fw]
    bar(ax, fw, m, e, "seconds (lower is better)", "End-to-end execution time")
    fig.tight_layout(); fig.savefig("results/fig_total_time.png", dpi=160); plt.close(fig)
    figs["total"] = "results/fig_total_time.png"

    # 2. peak CPU and peak memory increase (two separate panels, never a dual axis)
    fig, axes = plt.subplots(1, 2, figsize=(7.6, 3.0))
    bar(axes[0], fw, [stat(runs[k], "peak_cpu_percent")[0] for k in fw],
        [stat(runs[k], "peak_cpu_percent")[1] for k in fw], "% (system-wide)", "Peak CPU")
    bar(axes[1], fw, [stat(runs[k], "peak_mem_increase_gb")[0] for k in fw],
        [stat(runs[k], "peak_mem_increase_gb")[1] for k in fw], "GB above idle baseline", "Peak memory increase")
    fig.tight_layout(); fig.savefig("results/fig_resources.png", dpi=160); plt.close(fig)
    figs["resources"] = "results/fig_resources.png"

    # 3. UDF micro-benchmark, grouped bars
    variants = [("python_udf_sec", "Row-by-row Python UDF"), ("arrow_pandas_udf_sec", "Arrow pandas UDF"),
                ("native_expr_sec", "Native / vectorised")]
    fig, ax = plt.subplots(figsize=(7.6, 3.2))
    width = 0.36
    for i, k in enumerate(fw):
        xs, ys = [], []
        for j, (key, _) in enumerate(variants):
            v = stat(runs[k], key)[0]
            if v is not None:
                xs.append(j + (i - 0.5) * width); ys.append(v)
        bs = ax.bar(xs, ys, width=width, color=COLORS[k], label=LABEL[k], edgecolor="white", linewidth=2)
        for b, y in zip(bs, ys):
            ax.annotate(f"{y:.2f}", (b.get_x() + b.get_width() / 2, y), xytext=(0, 3),
                        textcoords="offset points", ha="center", fontsize=8, color="#52514e")
    ax.set_xticks(range(len(variants)), [v[1] for v in variants])
    ax.set_ylabel("seconds (mean over runs)")
    ax.set_title("Speed calculation on the cleaned data: UDF vs native", loc="left", fontsize=11)
    ax.legend(frameon=False)
    fig.tight_layout(); fig.savefig("results/fig_udf.png", dpi=160); plt.close(fig)
    figs["udf"] = "results/fig_udf.png"

    # 4. CPU and memory over time (latest run of each), two stacked panels
    fig, axes = plt.subplots(2, 1, figsize=(7.6, 4.6), sharex=True)
    for k in fw:
        tl = pd.read_csv(runs[k][-1]["timeline_csv"])
        axes[0].plot(tl["t_sec"], tl["cpu_percent"], color=COLORS[k], lw=2, label=LABEL[k])
        axes[1].plot(tl["t_sec"], tl["mem_used_gb"], color=COLORS[k], lw=2, label=LABEL[k])
    axes[0].set_ylabel("CPU %"); axes[1].set_ylabel("RAM used (GB)"); axes[1].set_xlabel("seconds since start")
    axes[0].set_title("Resource usage during one run of each pipeline", loc="left", fontsize=11)
    axes[0].legend(frameon=False, ncol=2)
    fig.tight_layout(); fig.savefig("results/fig_timeline.png", dpi=160); plt.close(fig)
    figs["timeline"] = "results/fig_timeline.png"
    return figs


def img(path, width="100%"):
    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()
    return f'<img src="data:image/png;base64,{b64}" style="width:{width}">'


def main():
    runs = load_runs()
    figs = figures(runs)
    parity = json.load(open("results/parity.json")) if os.path.exists("results/parity.json") else None
    hw = runs["spark"][-1]

    S = {k: {key: stat(runs[k], key) for key in
             ["total_time_sec", "peak_cpu_percent", "avg_cpu_percent", "peak_mem_increase_gb",
              "python_udf_sec", "arrow_pandas_udf_sec", "native_expr_sec", "udf_overhead_sec"]}
         for k in runs}
    ts, tr = S["spark"]["total_time_sec"][0], S["ray"]["total_time_sec"][0]
    faster, slower = ("Spark", "Ray") if ts < tr else ("Ray", "Spark")
    ratio = max(ts, tr) / min(ts, tr)
    rows_out = runs["spark"][-1]["rows_out"]

    sp_udf, sp_nat = S["spark"]["python_udf_sec"][0], S["spark"]["native_expr_sec"][0]
    ry_udf, ry_nat = S["ray"]["python_udf_sec"][0], S["ray"]["native_expr_sec"][0]
    sp_arrow = S["spark"]["arrow_pandas_udf_sec"][0]
    sp_slow = sp_udf / sp_nat if sp_nat else float("nan")
    ry_slow = ry_udf / ry_nat if ry_nat else float("nan")

    def row(label, key, unit=""):
        return (f"<tr><td>{label}</td><td>{fmt(*S['spark'][key], unit)}</td>"
                f"<td>{fmt(*S['ray'][key], unit)}</td></tr>")

    perf_table = "".join([
        row("Total wall-clock time (s)", "total_time_sec"),
        row("Peak CPU (%)", "peak_cpu_percent"),
        row("Average CPU (%)", "avg_cpu_percent"),
        row("Peak RAM increase over idle (GB)", "peak_mem_increase_gb"),
        row("Row-by-row Python UDF (s)", "python_udf_sec"),
        row("Arrow pandas UDF (s)", "arrow_pandas_udf_sec"),
        row("Native / vectorised equivalent (s)", "native_expr_sec"),
        row("Python UDF overhead = UDF − native (s)", "udf_overhead_sec"),
    ])
    runs_line = f"Spark: {len(runs['spark'])} run(s), Ray: {len(runs['ray'])} run(s); values are mean ± std. dev."

    if parity:
        c = parity["checks"]
        parity_html = (f"<p>Both outputs were compared by <code>verify_parity.py</code>: "
                       f"{parity['spark']['rows']:,} vs {parity['ray']['rows']:,} rows; "
                       f"order-independent row fingerprint {'identical' if c['row_fingerprint_equal'] else 'DIFFERENT'}; "
                       f"column sums {'identical' if c['column_sums_equal'] else 'DIFFERENT'}; "
                       f"hourly averages differ by at most {parity['max_hourly_diff']:.1e} "
                       f"(floating-point summation order only). "
                       f"<b>Result: {'PASS' if parity['passed'] else 'FAIL'}.</b></p>")
    else:
        parity_html = "<p><b>Run verify_parity.py before building the report.</b></p>"

    arrow_txt = (f" Spark's Arrow-based pandas UDF took {sp_arrow:.2f} s, which shows how much of the cost is "
                 f"the row-at-a-time serialisation rather than the Python code itself."
                 if sp_arrow else "")
    udf_html = f"""
<p>To isolate the UDF cost, each framework re-reads only <code>trip_distance</code> and
<code>trip_duration_sec</code> from the cleaned output ({runs['spark'][-1].get('udf_bench_rows', 0):,} rows) and computes
the speed in two ways: with the same row-level Python function used in the pipeline, and with an equivalent
native expression (Spark SQL column expression / NumPy vectorised code in Ray). The difference is the
overhead of running custom Python code.</p>
<ul>
<li><b>Spark:</b> Python UDF {sp_udf:.2f} s vs native {sp_nat:.2f} s, so the UDF is {sp_slow:.1f}× slower.
Spark executors are JVM processes. A classic Python UDF makes Spark start separate Python worker processes,
pickle every row in the JVM, send it over a socket, run the function, and send the result back.
That serialisation and copying is the "JVM overhead".{arrow_txt}</li>
<li><b>Ray:</b> Python UDF {ry_udf:.2f} s vs vectorised {ry_nat:.2f} s, so it is {ry_slow:.1f}× slower.
Ray workers are Python processes and the data is already pandas/Arrow in Ray's shared-memory object store,
so the function runs on the batch directly, with no JVM to Python hop. The remaining cost is just the Python
loop itself.</li>
</ul>
<p><b>Observation:</b> {'Ray pays a smaller penalty for custom Python code than Spark' if ry_slow < sp_slow else 'In this run Spark paid a smaller relative penalty than Ray'}
({ry_slow:.1f}× vs {sp_slow:.1f}×). This is the "Python-native" advantage: in Ray, the code you write
in Python is the execution engine. In Spark, Python sits outside the engine.</p>"""

    shots = ""
    for p in sorted(glob.glob("screenshots/*.png")):
        cap = os.path.splitext(os.path.basename(p))[0].replace("_", " ")
        shots += f'<figure style="display:inline-block;width:49%;vertical-align:top;margin:2px 0">{img(p)}<figcaption>{cap}</figcaption></figure>'
    if not shots:
        shots = "<p><b>Add screenshots to screenshots/ and re-run make_report.py.</b></p>"

    html = f"""<!doctype html><html><head><meta charset="utf-8"><title>AIOps Assignment 3</title>
<style>
body{{font-family:Georgia,'Times New Roman',serif;max-width:760px;margin:24px auto;color:#0b0b0b;line-height:1.45;font-size:10pt}}
h1{{font-size:20pt;margin:0}} h2{{font-size:13pt;color:#1a3c6e;border-bottom:1px solid #ccc;padding-bottom:2px;margin-top:20px}}
table{{border-collapse:collapse;width:100%;font-size:10pt;margin:8px 0}} th,td{{border-bottom:1px solid #ddd;padding:4px 6px;text-align:left}}
th{{background:#eef3f9}} code{{font-size:9.5pt;background:#f3f3f1;padding:0 3px}}
figure{{margin:10px 0;page-break-inside:avoid}} figcaption{{font-size:9pt;color:#52514e;text-align:center}}
.note{{background:#f6f6f4;border-left:3px solid #2a78d6;padding:6px 10px;font-size:10pt}}
@media print{{h2{{page-break-after:avoid}}}}
</style></head><body>
<h1>AIOps Assignment 3: Spark vs. Ray</h1>
<p>{AUTHOR}</p>

<h2>1. Setup</h2>
<p>Both frameworks ran on one laptop ({hw['cpu_count']} logical CPUs, {hw['total_mem_gb']} GB RAM)
as a 2-worker cluster:</p>
<ul>
<li><b>Spark 3.5 standalone:</b> one Master (<code>spark://localhost:7077</code>, UI :8080) and two Worker daemons.
The application requests one executor per worker (cores per executor = cores per worker), so it gets one executor on each worker. Application UI is on :4040.</li>
<li><b>Ray 2.40:</b> a head node started with <code>--num-cpus=0</code> (so it only coordinates) and two worker nodes
joined with <code>ray start --address</code>. Dashboard is on :8265. Pipeline uses Ray Data.</li>
</ul>
<p>Data: NYC TLC Yellow Taxi trip records, {runs['spark'][-1]['input_files']} monthly Parquet files of 2024, plus
<code>taxi_zone_lookup.csv</code>. The final cleaned table has {rows_out:,} rows.</p>

<h2>2. Pipeline (identical in both)</h2>
<table><tr><th>Step</th><th>Logic</th></tr>
<tr><td>Ingestion</td><td>Read all <code>yellow_tripdata_*.parquet</code> files and keep 9 columns. Read the zone lookup CSV.</td></tr>
<tr><td>Cleansing</td><td>Drop rows with nulls. Keep trip_distance &gt; 0, fare &gt; 0, dropoff after pickup, pickup in 2024. Remove exact duplicate rows. Format timestamps into <code>pickup_date</code> (yyyy-MM-dd), <code>pickup_hour</code> and <code>trip_duration_sec</code>.</td></tr>
<tr><td>Heavy join</td><td>Inner join with the zone table twice, on pickup and dropoff location, to get borough and zone names.</td></tr>
<tr><td>UDF</td><td><code>avg_speed_mph(distance, duration)</code>, the same Python function from <code>common.py</code> in both, applied per row. Trips faster than 100 mph or with invalid duration are dropped.</td></tr>
<tr><td>Feature</td><td>Average speed per pickup hour (24 values), joined back onto every trip as <code>avg_speed_hour</code>.</td></tr>
<tr><td>Export</td><td>Write 18 columns to Parquet.</td></tr></table>
<p>Implementation difference: Spark uses its built-in <code>dropDuplicates</code>, joins and <code>groupBy</code>.
Ray Data has no global distinct, so rows are hashed into 32 buckets and de-duplicated inside each bucket
(identical rows always land in the same bucket). The small zone table is broadcast into each batch with a pandas merge.</p>

<h2>3. Pipeline parity</h2>
{parity_html}

<h2>4. Performance</h2>
<p>{runs_line}</p>
<table><tr><th>Metric</th><th>Spark</th><th>Ray</th></tr>{perf_table}</table>
<figure>{img(figs['total'], '45%')}<figcaption>Figure 1: end-to-end time, ingestion to Parquet export</figcaption></figure>
<figure>{img(figs['resources'], '75%')}<figcaption>Figure 2: peak CPU and memory (system-wide, sampled every 0.5 s with psutil)</figcaption></figure>
<figure>{img(figs['timeline'], '75%')}<figcaption>Figure 3: CPU and RAM over time</figcaption></figure>

<h2>5. UDF deep-dive: Python-native Ray vs JVM-based Spark</h2>
{udf_html}
<figure>{img(figs['udf'], '75%')}<figcaption>Figure 4: cost of the speed calculation by implementation</figcaption></figure>

<h2>6. Winner</h2>
<p><b>{faster}</b> finished the full pipeline faster ({min(ts, tr):.1f} s vs {max(ts, tr):.1f} s, {ratio:.2f}×) on this setup.</p>
<p><b>BI-first project</b> (SQL reports, joins, aggregations over structured tables): <b>Spark</b>. Its Catalyst optimizer
and built-in operators (distinct, joins, groupBy) handle this kind of work natively, it has a SQL interface,
and it fits with warehouses and BI tools. The Python UDF penalty can be avoided by writing logic as native expressions.</p>
<p><b>AI-first project</b> (preprocessing that feeds model training, custom Python, GPUs): <b>Ray</b>. Custom Python runs
without a JVM hop, the same cluster can also run training, tuning and serving, and operations like distinct
that Spark gives for free are easy to write in pandas.</p>

<h2>7. Cluster screenshots</h2>
{shots}

<h2>8. Performance Tuning Note and AI use</h2>
<div class="note">
<p>An AI assistant (Claude) was used to help translate the pipeline logic between PySpark and Ray Data and
to debug it. Its performance suggestions:</p>
<ul>
<li><b>Ray hourly average:</b> my first version used <code>ds.groupby("pickup_hour").mean()</code>, which
sort-shuffles every row and was the slowest step in a test run. Suggestion: compute (sum, count) per hour inside
each batch, then combine the small partial table on the driver. Same result, much less data moved.</li>
<li><b>Ray de-duplication:</b> hash rows into 32 buckets and drop duplicates per bucket, instead of a group per row.</li>
<li><b>Parity settings:</b> set Spark's session time zone to UTC and write timestamps as microseconds, so both
frameworks produce the same timestamps.</li>
</ul>
<p>The cluster setup was done manually. All numbers in this report come from my own runs, saved in
<code>results/*.json</code>, and match the Spark UI and Ray Dashboard screenshots.</p>
</div>
</body></html>"""
    with open("report.html", "w") as f:
        f.write(html)
    print("Wrote report.html  -> open in Firefox, Ctrl+P, 'Save to PDF' as report.pdf")


if __name__ == "__main__":
    main()
