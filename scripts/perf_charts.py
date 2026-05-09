"""Generate AA4 performance charts from reports/perf_metrics.jsonl.

Run after the pipeline has completed at least once:
    python scripts/perf_charts.py

Outputs saved to reports/spark_ui/:
    exp1_aqe_comparison.png
    exp3_total_pipeline.png
    all_stages_comparison.png
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
METRICS_FILE = ROOT / "reports" / "perf_metrics.jsonl"
OUT_DIR = ROOT / "reports" / "spark_ui"

try:
    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    import numpy as np
except ImportError as e:
    sys.exit(f"Missing dependency: {e}\nRun: pip install pandas matplotlib numpy")

if not METRICS_FILE.exists():
    sys.exit(
        f"{METRICS_FILE} not found.\n"
        "Run the pipeline first:\n"
        "  Docker:  make pipeline\n"
        "  Windows: .\\run_local.ps1 -Stage pipeline"
    )

OUT_DIR.mkdir(parents=True, exist_ok=True)

records = [json.loads(l) for l in METRICS_FILE.read_text().splitlines() if l.strip()]
df = pd.DataFrame(records)
print(f"Loaded {len(df)} records from {METRICS_FILE.relative_to(ROOT)}")

# ── Experiment 1: AQE ON vs OFF (silver.charts_enriched) ─────────────────────
stage = "silver.charts_enriched"
exp1 = df[df["label"] == stage].groupby("aqe")["wall_sec"].mean()

fig, ax = plt.subplots(figsize=(6, 4))
labels = ["AQE OFF", "AQE ON"]
values = [exp1.get(False, 0), exp1.get(True, 0)]
colors = ["#e74c3c", "#2ecc71"]
bars = ax.bar(labels, values, color=colors, width=0.5, edgecolor="white")
ax.bar_label(bars, fmt="%.1fs", padding=4, fontsize=11, fontweight="bold")
ax.set_ylabel("Wall time (s)")
ax.set_title(f"Experiment 1 — AQE impact on '{stage}'")
ax.set_ylim(0, max(v for v in values if v) * 1.3 if any(values) else 1)
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0fs"))
ax.spines[["top", "right"]].set_visible(False)
if all(v > 0 for v in values):
    delta_pct = (values[0] - values[1]) / values[0] * 100
    ax.annotate(
        f"↓ {delta_pct:.0f}% improvement",
        xy=(1, values[1]), xytext=(1.2, (values[0] + values[1]) / 2),
        arrowprops=dict(arrowstyle="->", color="gray"),
        fontsize=10, color="gray",
    )
plt.tight_layout()
out = OUT_DIR / "exp1_aqe_comparison.png"
plt.savefig(out, dpi=150, bbox_inches="tight")
plt.close()
print(f"Saved {out.relative_to(ROOT)}")

# ── Experiment 3: Total pipeline AQE ON vs OFF ────────────────────────────────
total = df.groupby("aqe")["wall_sec"].sum().reset_index()
total["label"] = total["aqe"].map({True: "AQE ON\n(+ repartition)", False: "AQE OFF\n(no repartition)"})

fig, ax = plt.subplots(figsize=(7, 4))
colors = ["#e74c3c" if not v else "#2ecc71" for v in total["aqe"]]
bars = ax.bar(total["label"], total["wall_sec"], color=colors, width=0.5, edgecolor="white")
ax.bar_label(bars, fmt="%.0fs", padding=4, fontsize=11, fontweight="bold")
ax.set_ylabel("Total wall time (s)")
ax.set_title("Experiment 3 — Total pipeline time (all stages)")
ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0fs"))
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout()
out = OUT_DIR / "exp3_total_pipeline.png"
plt.savefig(out, dpi=150, bbox_inches="tight")
plt.close()
print(f"Saved {out.relative_to(ROOT)}")

# ── All-stages grouped bar chart ──────────────────────────────────────────────
stages = df["label"].unique()
x = range(len(stages))
width = 0.35

aqe_on  = [df[(df["label"] == s) & (df["aqe"] == True)]["wall_sec"].mean()  for s in stages]
aqe_off = [df[(df["label"] == s) & (df["aqe"] == False)]["wall_sec"].mean() for s in stages]
aqe_on  = [0 if np.isnan(v) else v for v in aqe_on]
aqe_off = [0 if np.isnan(v) else v for v in aqe_off]

fig, ax = plt.subplots(figsize=(10, 5))
ax.bar([i - width/2 for i in x], aqe_off, width, label="AQE OFF", color="#e74c3c", alpha=0.85)
ax.bar([i + width/2 for i in x], aqe_on,  width, label="AQE ON",  color="#2ecc71", alpha=0.85)
ax.set_xticks(list(x))
ax.set_xticklabels(stages, rotation=30, ha="right", fontsize=9)
ax.set_ylabel("Wall time (s)")
ax.set_title("Per-stage wall time: AQE ON vs OFF")
ax.legend()
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout()
out = OUT_DIR / "all_stages_comparison.png"
plt.savefig(out, dpi=150, bbox_inches="tight")
plt.close()
print(f"Saved {out.relative_to(ROOT)}")

# ── Summary table ─────────────────────────────────────────────────────────────
print("\n── Summary ──────────────────────────────────────────────────────")
summary = (
    df.groupby(["label", "aqe"])["wall_sec"]
    .agg(["mean", "min", "max", "count"])
    .round(1)
)
print(summary.to_string())
