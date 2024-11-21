#!/usr/bin/env python3

import os
import argparse
import polars as pl


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--retweet_rate_results_dir", type=str, required=True)
arg_parser.add_argument("--save_path", type=str, required=True)

INFLUENCE = ["low", "low-mid", "mid", "high-mid", "high", "very-high"]
INF2NUM = {influence: i for i, influence in enumerate(INFLUENCE)}


# [File Summary]
# This script merges the aggregated second spread results from the retweet rate results directory.
# [Inputs]
# retweet_rate_results_dir:
#   The path to the aggregated second spread results directory.
#   This file is the output of the script aggregate_second_spreads.py.
# [Outputs]
# save_path:
#   The path to save the merged second spread results file.


def main(args):
    retweet_rate_results_files = os.listdir(args.retweet_rate_results_dir)
    # retweet_rate_results_paths = [os.path.join(args.retweet_rate_results_dir, f)
    #                               for f in retweet_rate_results_files]

    df_list = []
    algorithm = args.retweet_rate_results_dir.split("/")[-2]
    for i, file_name in enumerate(retweet_rate_results_files):
        path = os.path.join(args.retweet_rate_results_dir, file_name)
        hours = int(file_name.split("_")[0].rstrip("hours"))
        min_rt = int(file_name.split("_")[1].split(".")[0].lstrip("minrt"))
        df_rate = pl.read_excel(path)
        df_rate = df_rate.with_columns(
            pl.Series([hours] * len(df_rate), dtype=pl.Int32).alias("passed_time"),
            pl.Series([min_rt] * len(df_rate), dtype=pl.Int32).alias("min_repost"),
            pl.Series([algorithm] * len(df_rate), dtype=pl.Utf8).alias("algorithm"),
            pl.col("influence_score_group").replace_strict(INF2NUM).alias("influence_id"),
        )
        df_list.append(df_rate)

    df_rate = pl.concat(df_list)
    del df_list

    df_rate = df_rate.sort(
        ["passed_time", "min_repost", "influence_id"],
        descending=[False, False, True])
    df_rate = df_rate.drop("influence_id")

    df_rate.write_excel(args.save_path)


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
