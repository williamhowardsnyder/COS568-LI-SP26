"""
NOTE: this file was generated using Claude Code

Milestone 2 analysis: compare DynamicPGM, LIPP, and HybridPGMLIPP on the
Facebook dataset for two mixed workloads.

Produces four bar plots:
  1. Throughput  – 90% Lookup / 10% Insert
  2. Index size  – 90% Lookup / 10% Insert
  3. Throughput  – 10% Lookup / 90% Insert
  4. Index size  – 10% Lookup / 90% Insert
"""

import os
import pandas as pd
import matplotlib.pyplot as plt


RESULT_DIR = "results"
DATASET    = "fb_100M_public_uint64"
INDEXES    = ["DynamicPGM", "LIPP", "HybridPGMLIPP"]

# Workload filenames (CSV results live in results/<name>_results_table.csv)
WORKLOAD_10PCT = f"{DATASET}_ops_2M_0.000000rq_0.500000nl_0.100000i_0m_mix"
WORKLOAD_90PCT = f"{DATASET}_ops_2M_0.000000rq_0.500000nl_0.900000i_0m_mix"


def best_row(df, index_name):
    """Return the row with the highest average mixed throughput for index_name."""
    rows = df[df["index_name"] == index_name].copy()
    if rows.empty:
        return None
    cols = ["mixed_throughput_mops1", "mixed_throughput_mops2", "mixed_throughput_mops3"]
    available = [c for c in cols if c in rows.columns]
    rows["avg_throughput"] = rows[available].mean(axis=1)
    return rows.loc[rows["avg_throughput"].idxmax()]


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
            throughputs[idx] = row["avg_throughput"]
            sizes[idx]       = row["index_size_bytes"] / (1024 ** 2)  # MB
        print(idx)
        print(row)
    return throughputs, sizes


def plot_bar(ax, values, indexes, title, ylabel):
    colors = ["#4C72B0", "#DD8452", "#55A868"]
    bars = ax.bar(indexes, [values[i] for i in indexes],
                  color=colors[:len(indexes)], edgecolor="black", linewidth=0.7)
    ax.set_title(title, fontsize=12)
    ax.set_ylabel(ylabel, fontsize=11)
    ax.set_xlabel("Index", fontsize=11)
    for bar, idx in zip(bars, indexes):
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2.0, h * 1.01,
                f"{h:.2f}", ha="center", va="bottom", fontsize=9)
    return bars


def main():
    df_10 = load_workload(WORKLOAD_10PCT)
    df_90 = load_workload(WORKLOAD_90PCT)

    thr_10, sz_10 = extract_metrics(df_10, INDEXES)
    thr_90, sz_90 = extract_metrics(df_90, INDEXES)

    fig, axs = plt.subplots(2, 2, figsize=(12, 9))
    fig.suptitle(
        "Milestone 2: Hybrid PGM+LIPP vs Baselines (Facebook Dataset)",
        fontsize=14, fontweight="bold"
    )

    plot_bar(axs[0, 0], thr_10, INDEXES,
             "Throughput - 90% Lookup / 10% Insert", "Throughput (Mops/s)")
    plot_bar(axs[0, 1], sz_10, INDEXES,
             "Index Size - 90% Lookup / 10% Insert", "Index Size (MB)")
    plot_bar(axs[1, 0], thr_90, INDEXES,
             "Throughput - 10% Lookup / 90% Insert", "Throughput (Mops/s)")
    plot_bar(axs[1, 1], sz_90, INDEXES,
             "Index Size - 10% Lookup / 90% Insert", "Index Size (MB)")

    plt.tight_layout(rect=[0, 0, 1, 0.95])

    os.makedirs("analysis_results", exist_ok=True)
    out = "analysis_results/milestone2_hybrid_results.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    print(f"Saved: {out}")
    plt.show()

    # Also print a summary table
    print("\n=== Throughput (Mops/s) ===")
    print(f"{'Index':<20} {'90%Lkp/10%Ins':>15} {'10%Lkp/90%Ins':>15}")
    print("-" * 52)
    for idx in INDEXES:
        print(f"{idx:<20} {thr_10[idx]:>15.3f} {thr_90[idx]:>15.3f}")

    print("\n=== Index Size (MB) ===")
    print(f"{'Index':<20} {'90%Lkp/10%Ins':>15} {'10%Lkp/90%Ins':>15}")
    print("-" * 52)
    for idx in INDEXES:
        print(f"{idx:<20} {sz_10[idx]:>15.2f} {sz_90[idx]:>15.2f}")


if __name__ == "__main__":
    main()
