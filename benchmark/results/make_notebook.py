"""Writes analysis.ipynb (no nbformat needed). The cells only read the CSVs in results/, so the notebook
works on any machine with pandas + matplotlib. Run `python3 make_notebook.py --check` to execute all
code cells as one script (a smoke test without Jupyter)."""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

CELLS = [
("md", """# GeoBenchr results analysis

Reads the CSV tables produced by `build_results.py` (in `results/`) and reproduces the main tables and
figures of the write-up (`README.md`). One row of `executions.csv` is one query execution; everything
else is derived from it.

| file | one row per |
|---|---|
| `executions.csv` | query execution (scenario, system, query, repetition, status, duration, parameters, SQL) |
| `summary.csv` | (scenario, query, system): runs, failures, mean/median/min/max/std, equivalence, rank |
| `winners.csv` | (scenario, query): fastest ranked system, runner-up, margin |
| `system_scores.csv` | system: wins, completed queries, geometric-mean slowdown vs the fastest |
| `pairwise_total_time.csv` | pair of systems: total runtime over the queries both completed |
| `equivalence.csv` | (scenario, query, system): result compared with PostGIS |
| `h3_vs_scan.csv` | ClickHouse query repetition: H3 version vs scan-only version |"""),
("code", """import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

RES = os.environ.get("RESULTS_DIR", "results")
SYSTEMS = ["PostGIS", "MobilityDB", "ClickHouse", "DuckDB", "SpaceTime", "Sedona"]
COLORS = {"PostGIS": "#336791", "MobilityDB": "#5b9bd5", "ClickHouse": "#f2b705", "DuckDB": "#7f7f7f",
          "SpaceTime": "#c0392b", "Sedona": "#27ae60"}
ex_all = pd.read_csv(f"{RES}/executions.csv")
# measured executions: without the warm-up repetitions and the parameter sets left out for every system
dropped = pd.read_csv(f"{RES}/dropped_parameter_sets.csv") if os.path.exists(f"{RES}/dropped_parameter_sets.csv") else pd.DataFrame(columns=["scenario", "query", "repetition"])
drop_keys = set(zip(dropped.scenario, dropped["query"], dropped.repetition))
ex = ex_all[~ex_all.warmup.fillna(False).astype(bool)]
ex = ex[[k not in drop_keys for k in zip(ex.scenario, ex["query"], ex.repetition)]]
summary = pd.read_csv(f"{RES}/summary.csv")
winners = pd.read_csv(f"{RES}/winners.csv")
scores = pd.read_csv(f"{RES}/system_scores.csv")
print(len(ex_all), "executions,", len(ex), "measured;", ex.status.value_counts().to_dict())"""),
("md", "## Left out for every system\n\nParameter sets with broken parameters, or on which every system hit the timeout. The first repetitions of every query are warm-up and are left out as well."),
("code", """dropped"""),
("md", "## Overall: wins and relative speed per system"),
("code", """scores.set_index("system")"""),
("code", """fig, axes = plt.subplots(1, 2, figsize=(12, 4))
axes[0].bar(scores.system, scores.wins, color=[COLORS[s] for s in scores.system])
axes[0].set_title("queries won (fastest ranked system)")
axes[1].bar(scores.system, scores.geomean_slowdown_vs_best, color=[COLORS[s] for s in scores.system])
axes[1].set_title("geometric-mean slowdown vs the fastest system (lower is better)")
axes[1].set_yscale("log")
for ax in axes:
    ax.tick_params(axis="x", rotation=30)
plt.tight_layout()"""),
("md", "## Total runtime, pair by pair\n\nSum of the measured repetitions' runtimes over only the queries both systems completed (a per-system total over each system's own queries would compare different query sets)."),
("code", """pw = pd.read_csv(f"{RES}/pairwise_total_time.csv") if os.path.exists(f"{RES}/pairwise_total_time.csv") else pd.DataFrame()
pw.assign(total_a_min=(pw.total_a_s / 60).round(1), total_b_min=(pw.total_b_s / 60).round(1))[["system_a", "system_b", "common_queries", "total_a_min", "total_b_min", "ratio_a_over_b"]] if len(pw) else None"""),
("md", "## Wins per scenario and per query type"),
("code", """pd.crosstab(winners.scenario, winners.winner, margins=True)"""),
("code", """pd.crosstab(winners.query_type, winners.winner, margins=True)"""),
("md", "## Per query: ranking, mean runtime, margin"),
("code", """pd.set_option("display.max_colwidth", 200)
winners[["scenario", "query", "query_type", "winner", "winner_mean_ms", "runner_up", "margin_x", "ranking", "not_ranked"]]"""),
("md", "## Mean runtime matrix (ms), one row per query"),
("code", """summary.pivot_table(index=["scenario", "query"], columns="system", values="mean_ms").reindex(columns=[s for s in SYSTEMS if s in summary.system.unique()]).round(0)"""),
("md", "## Slowdown vs the fastest system"),
("code", """sd = summary.pivot_table(index=["scenario", "query"], columns="system", values="slowdown_vs_best")
sd = sd.reindex(columns=[s for s in SYSTEMS if s in sd.columns])
fig, ax = plt.subplots(figsize=(8, 0.3 * len(sd) + 1.5))
im = ax.imshow(np.log10(sd.values.astype(float)), aspect="auto", cmap="RdYlGn_r", vmin=0, vmax=3)
ax.set_xticks(range(len(sd.columns))); ax.set_xticklabels(sd.columns, rotation=30, ha="right")
ax.set_yticks(range(len(sd))); ax.set_yticklabels([f"{a}/{b}" for a, b in sd.index], fontsize=7)
fig.colorbar(im, ax=ax, label="log10 slowdown (blank = not ranked)")
plt.tight_layout()"""),
("md", "## Variability across the repetitions (different parameters each)\n\n`cv` = standard deviation / mean over the repetitions of one query on one system."),
("code", """summary[summary.succeeded > 1].groupby("system").cv.describe()[["count", "mean", "50%", "max"]].round(2)"""),
("md", "## Plot one query: every repetition"),
("code", """def plot_query(scenario, query):
    d = ex[(ex.scenario == scenario) & (ex["query"] == query) & (ex.status == "success")]
    systems = [s for s in SYSTEMS if s in d.system.unique()]
    fig, ax = plt.subplots(figsize=(7, 4))
    for i, s in enumerate(systems):
        v = d[d.system == s].duration_ms.clip(lower=1) / 1000
        ax.bar(i, v.mean(), color=COLORS[s], alpha=0.85)
        ax.scatter(np.full(len(v), i) + np.linspace(-0.15, 0.15, len(v)), v, color="black", s=15, zorder=3)
    ax.set_xticks(range(len(systems))); ax.set_xticklabels(systems, rotation=30)
    ax.set_yscale("log"); ax.set_ylabel("seconds (log)"); ax.set_title(f"{scenario}/{query}")
    plt.tight_layout()

first = winners.iloc[0]
plot_query(first.scenario, first["query"])"""),
("md", "## Warm-up vs measured repetitions\n\nMedian runtime of the warm-up repetitions divided by the median of the measured ones, per system (above 1: the first queries were slower)."),
("code", """ok = ex_all[ex_all.status == "success"]
w = ok.groupby(["system", "warmup"]).duration_ms.median().unstack()
(w[True] / w[False]).round(2).rename("warm-up / measured median") if True in w and False in w else "no warm-up repetitions\""""),
("md", "## Hardware per system\n\nMean CPU busy (user+system, %) and memory used (GB) per node while each system ran, from `hardware_by_phase.csv` (5 s samples)."),
("code", """hw = pd.read_csv(f"{RES}/hardware_by_phase.csv") if os.path.exists(f"{RES}/hardware_by_phase.csv") else pd.DataFrame()
if len(hw):
    nodes = ["nst-1", "nst-2", "nst-3", "nst-4", "nst-5"]
    cols = {f"{n} cpu %": f"{n}_cpu_busy_mean_pct" for n in nodes} | {f"{n} mem GB": f"{n}_mem_used_mean_gb" for n in nodes}
    t = hw[["scenario", "system", "minutes"] + list(cols.values())].rename(columns={v: k for k, v in cols.items()})
    display(t.round(1)) if "display" in globals() else print(t.round(1))"""),
("md", "## Failures (errors and timeouts)"),
("code", """fails = ex[ex.status != "success"]
fails.groupby(["system", "status"]).size().unstack(fill_value=0) if len(fails) else "no failures\""""),
("code", """fails[["scenario", "query", "system", "repetition", "status", "error"]].head(50) if len(fails) else None"""),
("md", "## Equivalence with PostGIS"),
("code", """eq = pd.read_csv(f"{RES}/equivalence.csv")
eq["category"] = eq.verdict.fillna("").str.extract(r"^([A-Za-z/ ]*[A-Za-z/])", expand=False)
pd.crosstab(eq.system, eq.category)"""),
("code", """eq[~eq.acceptable][["scenario", "query", "system", "verdict"]]"""),
("md", "## ClickHouse: H3 index vs scan-only queries\n\nSame parameters, every repetition: row count and an order-independent hash of the full result. The times come from the equivalence test, not the benchmark (wrapped in `count()`/`sum(cityHash64(*))`, cluster shared with other tests)."),
("code", """h3 = pd.read_csv(f"{RES}/h3_vs_scan.csv") if os.path.exists(f"{RES}/h3_vs_scan.csv") else pd.DataFrame()
if len(h3):
    print("identical results:", int(h3.identical.sum()), "of", len(h3))
    g = h3.groupby(["scenario", "query"]).agg(reps=("repetition", "count"), identical=("identical", "sum"),
                                              h3_s=("h3_secs", "mean"), scan_s=("scan_secs", "mean"))
    g["speedup_x"] = (g.scan_s / g.h3_s).round(1)
    display(g.round(2)) if "display" in globals() else print(g.round(2))"""),
]


def build():
    cells = []
    for kind, src in CELLS:
        lines = src.split("\n")
        source = [l + "\n" for l in lines[:-1]] + [lines[-1]]
        if kind == "md":
            cells.append({"cell_type": "markdown", "metadata": {}, "source": source})
        else:
            cells.append({"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": source})
    nb = {"cells": cells, "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                                       "language_info": {"name": "python"}}, "nbformat": 4, "nbformat_minor": 5}
    json.dump(nb, open(os.path.join(HERE, "analysis.ipynb"), "w"), indent=1)
    print("wrote analysis.ipynb with", len(cells), "cells")


def check():
    import matplotlib
    matplotlib.use("Agg")
    ns = {}
    for kind, src in CELLS:
        if kind == "code":
            lines = src.strip().split("\n")
            if lines[-1].startswith((" ", "\t")):
                exec(compile(src, "<cell>", "exec"), ns)
                continue
            exec(compile("\n".join(lines[:-1]), "<cell>", "exec"), ns)
            try:   # last line is the cell's displayed value
                val = eval(compile(lines[-1], "<cell>", "eval"), ns)
                if val is not None:
                    print(str(val)[:400], "\n---")
            except SyntaxError:
                exec(compile(lines[-1], "<cell>", "exec"), ns)


if __name__ == "__main__":
    build()
    if "--check" in sys.argv:
        check()
