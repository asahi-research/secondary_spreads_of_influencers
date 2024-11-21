#!/usr/bin/env python3

import argparse
import polars as pl
import matplotlib as mpl
mpl.use("WebAgg")
mpl.rc("webagg", port=8899, port_retries=10, open_in_browser=False)
import matplotlib.pyplot as plt
import seaborn as sns
import scienceplots


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--merged_second_sperads_result", type=str, required=True)
arg_parser.add_argument("--algorithm", type=str, nargs="+",
                        choices=["degree", "h-index", "hg_index"],
                        required=True)


# [File Summary]
# This script plots the second spread results from the merged second spreads.
# [Inputs]
# merged_second_sperads_result:
#   The path to the merged second spreads result file.
#   The file outputed by the script merge_retweet_rate_results.py.
# algorithm:
#   The algorithm used to calculate the influence score.


def main(args):
    df = pl.read_excel(args.merged_second_sperads_result)

    df = df.filter(pl.col("algorithm").is_in(args.algorithm))
    print(df)

    plt.style.use(["science", "nature"])
    fig, ax = plt.subplots(1, 2, figsize=(10, 5))

    df_6_hours = df.filter(pl.col("passed_time") == 6)
    sns.barplot(
        data=df_6_hours.to_pandas(),
        x="min_repost",
        y="second_spread_ratio",
        hue="influence_score_group",
        alpha=1.0,
        ax=ax[0],
        palette=reversed(sns.diverging_palette(220, 20)),
    )
    print(df_6_hours)
    ax[0].set_ylim(0.0, 0.01)
    ax[0].set_xlabel("Number of Repost", fontsize=14)
    ax[0].set_ylabel("Cascading Repost Probability (CRP)", fontsize=14)
    ax[0].set_label("Influence")
    ax[0].set_title("Passed Time: 6 hours", fontsize=14)
    ax[0].legend(title=f"User Influence", fontsize=12, loc="upper left", title_fontsize=10)
    ax[0].set_xticklabels(["$< 1000$"] + ["$\\geq$ " + str(n) for n in [1000, 5000, 10000]], fontsize=14)

    df_min_5000 = df.filter(pl.col("min_repost") == 5000)
    sns.pointplot(
        data=df_min_5000.to_pandas(),
        x="passed_time",
        y="second_spread_ratio",
        hue="influence_score_group",
        alpha=1.0,
        ax=ax[1],
        palette=reversed(sns.diverging_palette(220, 20)),
    )
    ax[1].set_ylim(0.0, 0.01)
    ax[1].set_xlabel("Passed Time (hours)", fontsize=12)
    ax[1].set_ylabel("Cascading Repost Probability (CRP)", fontsize=14)
    ax[1].set_title("Number of Repost: $\geq 5000$", fontsize=14)
    ax[1].legend(title=f"User Influence", fontsize=12, loc="upper right", title_fontsize=10)
    ax[1].set_xticklabels(["3", "6", "12", "24"], fontsize=14)

    plt.show()


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
