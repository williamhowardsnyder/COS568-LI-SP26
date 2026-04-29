"""
Milestone 3 analysis: compare DynamicPGM, LIPP, HybridPGMLIPP (M2 naive),
and HybridPGMLIPPAsync (M3 async) on mixed workloads across all three datasets.

Produces one figure per dataset (3 total), each with a 2x2 layout:
  row 0: throughput for 90%Lkp/10%Ins | throughput for 10%Lkp/90%Ins
  row 1: index size  for 90%Lkp/10%Ins | index size  for 10%Lkp/90%Ins
"""

import os
import pandas as pd
import matplotlib.pyplot as plt

RESULT_DIR = "results"
DATASETS   = ["fb", "books", "osmc"]
INDEXES    = ["DynamicPGM", "LIPP", "HybridPGMLIPP", "HybridPGMLIPPAsync"]
COLORS     = ["#4C72B0", "#DD8452", "#55A868", "#C44E52"]

WORKLOAD_10PCT = "{d}_ops_2M_0.000000rq_0.500000nl_0.100000i_0m_mix"
WORKLOAD_90PCT = "{d}_ops_2M_0.000000rq_0.500000nl_0.900000i_0m_mix"


def best_row(df, index_name):
    rows = df[df["index_name"] == index_name].copy()
    if rows.empty:
        return None
    cols = ["mixed_throughput_mops1", "mixed_throughput_mops2", "mixed_throughput_mops3"]
    available = [c for c in cols if c in rows.columns]
    rows["avg"] = rows[available].mean(axis=1)
    return rows.loc[rows["avg"].idxmax()]


def load_workload(name):
    path = os.path.join(RESULT_DIR, f"{name}_results_table.csv")
    if not os.path.exists(path):
        print(f"WARNING: result file not found: {path}")
        return None
    return pd.read_csv(path)


def extract_metrics(df, indexes):
    throughputs, sizes = {}, {}
    if df is None:
        return {k: 0 for k in indexes}, {k: 0 for k in indexes}
    for idx in indexes:
        row = best_row(df, idx)
        if row is None:
            throughputs[idx] = 0
            sizes[idx]       = 0
        else:
            throughputs[idx] = row["avg"]
            sizes[idx]       = row["index_size_bytes"] / (1024 ** 2)
    return throughputs, sizes


def plot_bar(ax, values, indexes, title, ylabel, colors):
    bars = ax.bar(
        range(len(indexes)),
        [values[i] for i in indexes],
        color=colors[:len(indexes)],
        edgecolor="black",
        linewidth=0.7,
    )
    ax.set_title(title, fontsize=10)
    ax.set_ylabel(ylabel, fontsize=9)
    ax.set_xticks(range(len(indexes)))
    ax.set_xticklabels(indexes, rotation=15, ha="right", fontsize=8)
    for bar in bars:
        h = bar.get_height()
        if h > 0:
            ax.text(bar.get_x() + bar.get_width() / 2.0, h * 1.01,
                    f"{h:.2f}", ha="center", va="bottom", fontsize=7)


def main():
    os.makedirs("analysis_results", exist_ok=True)

    for ds in DATASETS:
        full = f"{ds}_100M_public_uint64"
        wl10 = WORKLOAD_10PCT.format(d=full)
        wl90 = WORKLOAD_90PCT.format(d=full)

        df10 = load_workload(wl10)
        df90 = load_workload(wl90)

        thr10, sz10 = extract_metrics(df10, INDEXES)
        thr90, sz90 = extract_metrics(df90, INDEXES)

        fig, axs = plt.subplots(2, 2, figsize=(12, 8))
        fig.suptitle(
            f"Milestone 3: Hybrid Async vs Baselines  ({ds.upper()} dataset)",
            fontsize=13, fontweight="bold",
        )

        plot_bar(axs[0, 0], thr10, INDEXES,
                 "Throughput – 90% Lookup / 10% Insert", "Throughput (Mops/s)", COLORS)
        plot_bar(axs[0, 1], thr90, INDEXES,
                 "Throughput – 10% Lookup / 90% Insert", "Throughput (Mops/s)", COLORS)
        plot_bar(axs[1, 0], sz10,  INDEXES,
                 "Index Size – 90% Lookup / 10% Insert", "Index Size (MB)", COLORS)
        plot_bar(axs[1, 1], sz90,  INDEXES,
                 "Index Size – 10% Lookup / 90% Insert", "Index Size (MB)", COLORS)

        plt.tight_layout(rect=[0, 0, 1, 0.95])
        out = f"analysis_results/milestone3_{ds}_results.png"
        plt.savefig(out, dpi=300, bbox_inches="tight")
        print(f"Saved: {out}")
        plt.close()

    # Summary table
    print("\n=== Throughput (Mops/s) ===")
    header = f"{'Index':<22}"
    for ds in DATASETS:
        header += f"  {ds+' 90%Lkp':>12}  {ds+' 10%Lkp':>12}"
    print(header)
    print("-" * (22 + len(DATASETS) * 28))

    all_data = {}
    for ds in DATASETS:
        full = f"{ds}_100M_public_uint64"
        df10 = load_workload(WORKLOAD_10PCT.format(d=full))
        df90 = load_workload(WORKLOAD_90PCT.format(d=full))
        thr10, _ = extract_metrics(df10, INDEXES)
        thr90, _ = extract_metrics(df90, INDEXES)
        all_data[ds] = (thr10, thr90)

    for idx in INDEXES:
        row = f"{idx:<22}"
        for ds in DATASETS:
            thr10, thr90 = all_data[ds]
            row += f"  {thr10[idx]:>12.3f}  {thr90[idx]:>12.3f}"
        print(row)


if __name__ == "__main__":
    main()
