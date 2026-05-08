# Performance Report — AA4

## Measurement Protocol

Three controlled experiments, each run twice (AQE on / AQE off or hint on / hint off).
Raw timings in `reports/perf_metrics.jsonl`. Screenshots of Spark UI in `reports/spark_ui/`.

---

## Experiment 1 — AQE On vs Off

**Stage:** `silver.charts_enriched` (the large charts × tracks sort-merge join)

| Run | AQE | Wall time (s) | Shuffle write (MB) | Stages |
|---|---|---|---|---|
| 1 | OFF | (pending) | (pending) | (pending) |
| 2 | ON  | (pending) | (pending) | (pending) |

**Expected:** AQE coalesces post-shuffle partitions dynamically, reducing the number of
tasks in the join stage. Expected 20–40% wall-clock improvement.

**How to reproduce:**
```python
from bigdata_music.spark_session import get_spark, get_spark_no_aqe
from bigdata_music.utils.metrics import measure
from bigdata_music.silver.join_enriched import build_charts_enriched

spark_aqe = get_spark()
with measure(spark_aqe, "enriched-join-AQE-ON"):
    build_charts_enriched(spark_aqe)

spark_no_aqe = get_spark_no_aqe()
with measure(spark_no_aqe, "enriched-join-AQE-OFF"):
    build_charts_enriched(spark_no_aqe)
```

---

## Experiment 2 — Broadcast Hint vs Default

**Stage:** Charts × countries join (63-row right side)

| Run | Hint | Shuffle bytes (countries side) | Stage count |
|---|---|---|---|
| Without `broadcast()` | Default | (pending) | (pending) |
| With `broadcast()` | Forced | (pending) | (pending) |

**Expected:** With `broadcast()`, the shuffle bytes for the countries side drop to ~0
(the 5 KB table is replicated to each executor instead of shuffled).

---

## Experiment 3 — Pre-Repartition Before Sort-Merge Join

**Stage:** charts × tracks join on `track_id`

| Run | Pre-repartition | Shuffle stages | Shuffle write (GB) | Wall time (s) |
|---|---|---|---|---|
| Without | Default | (pending) | (pending) | (pending) |
| With `repartition(200, "track_id")` | Explicit | (pending) | (pending) | (pending) |

**Expected:** Explicit pre-repartitioning on the join key means both sides are already
co-located — the planner can skip one shuffle stage.

---

## Analysis notebook

See `notebooks/99_perf_analysis.ipynb` for charts generated from `perf_metrics.jsonl`.
