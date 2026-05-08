# Data Engineering Report — Rapport v2

**Project:** UE28 Big Data — Spotify Charts Analysis
**Milestone:** M2 — Data Engineering

*This document supplements poc_report.md. Focus: package architecture, test coverage,
performance results, and what changed between M1 and M2.*

---

## Refactoring: Notebook → Package

The M1 PoC notebook contained all pipeline logic inline. M2 extracted it into
`src/bigdata_music/` with the following benefits:

1. **Testability** — cleaning functions and the streak algorithm now have unit tests
   (`tests/`) that run in under 60s without needing the full datasets.
2. **Reproducibility** — `python -m bigdata_music.pipeline` runs the full Bronze→Silver→Gold
   pipeline identically every time, with no "run cells in order" requirement.
3. **Feature flags** — `RUN_BRONZE/SILVER/GOLD` env vars let you re-run only the Gold layer
   during iteration, skipping the 40-minute Bronze + Silver stages.

## Test Coverage

```
(to be filled after pytest run)
```

## Performance Results

*See `perf_report.md` for detailed tables and Spark UI screenshots.*

Summary:
- AQE reduced the charts × tracks join wall time by __ % (Experiment 1)
- Broadcast hint eliminated __ MB of shuffle for the countries join (Experiment 2)
- Pre-repartition saved __ shuffle stages (Experiment 3)

## What I Would Do Differently

*(Inherited from poc_report.md + additions after M2 implementation)*

5. **Streaming-first Bronze** — the current Bronze uses `mode("overwrite")`. Migrating to
   `mode("append")` + Delta `MERGE INTO` at Silver would halve the pipeline runtime after
   the first run and make Part 4 (Streaming) a smaller diff.

6. **More granular Gold for the dashboard** — some dashboard queries re-aggregate Gold
   data at render time. Pre-computing the exact aggregates the UI needs (at the cost of
   more Gold tables) would make the dashboard faster on cold load.
