#!/usr/bin/env python3

import argparse
import os
import polars as pl


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--second_sperads_dir", type=str, required=True)
arg_parser.add_argument("--userinfo_path", type=str, required=True)
arg_parser.add_argument("--save_path", type=str, required=True)
arg_parser.add_argument("--present_tweet_ids_path", type=str, required=True)
arg_parser.add_argument("--filter_hours", type=int, default=-1)


MINUTE = 60
HOUR = 60 * MINUTE


# [File Summary]
# This script aggregates the second spread results from the second spreads directory.
# Namely, This computes the cascading repost probability (CRP) for each user in each tweet.
# [Inputs]
# second_sperads_dir:
#   The path to the second spreads directory.
#   The directory contains the second spreaders for each tweet.
#   The file is the output of the script compute_second_spread_ratio.py.
# userinfo_path:
#   The path to the user information file.
#   This file is the output of the script extract_user_bio.py.
# present_tweet_ids_path:
#   The path to the present tweet ids.
#   This file is the output of the script extract_present_tweet_id_in_cascade.py.

def main(args):
    print("Loading User Information...")

    df_userinfo = pl.read_parquet(args.userinfo_path)
    no_feature_official_users = ["biccameraE", "paypay_matsuri"]
    df_userinfo = df_userinfo.with_columns(
        (pl.col("description").str.contains("公式") |
         pl.col("description").str.contains("オフィシャル") |
         pl.col("description").str.to_lowercase().str.contains("official") |
         pl.col("description").str.contains("商品情報") |
         pl.col("name").str.contains("公式") |
         pl.col("name").str.contains("オフィシャル") |
         pl.col("name").str.to_lowercase().str.contains("official") |
         pl.col("screen_name").str.to_lowercase().str.contains("official") |
         pl.col("screen_name").str.to_lowercase().str.starts_with("pr_") |
         pl.col("screen_name").str.to_lowercase().str.ends_with("_pr") |
         pl.col("screen_name").is_in(no_feature_official_users)
         ).alias("is_official"))

    official_user_ids = df_userinfo.filter(
        df_userinfo["is_official"])["user_id"].to_list()

    dfs = []
    for second_spread_path in os.listdir(args.second_sperads_dir):
        second_spread_path = os.path.join(args.second_sperads_dir, second_spread_path)
        df_ss = pl.read_parquet(second_spread_path)

        if args.filter_hours > 0:
            df_ss = df_ss.with_columns(
                (pl.col("followee_timestamp") - pl.col("source_timestamp")).alias("passed_seconds"),
               ).filter(pl.col("passed_seconds") <= args.filter_hours * HOUR)

        present_tweet_ids = pl.read_parquet(args.present_tweet_ids_path)["tweet_id"].to_list()
        df_ss = df_ss.filter(pl.col("parent_tweet_id").is_in(present_tweet_ids))

        print("Num Followees: ", df_ss["followee"].n_unique())

        df_ss = df_ss.group_by(["source_tweet_id", "followee"]).agg(
            pl.len().alias("num_views"),
            pl.sum("is_retweeted").alias("num_retweets"),
            pl.col("is_in_cascade").first().alias("is_in_cascade"),
        )
        dfs.append(df_ss)

    df_ss = pl.concat(dfs)

    df_ss = df_ss.group_by(["source_tweet_id", "followee"]).agg(
        pl.sum("num_views").alias("num_views"),
        pl.sum("num_retweets").alias("num_retweets"),
        pl.col("is_in_cascade").first().alias("is_in_cascade"),
    )

    df_ss = df_ss.with_columns(
        pl.col("followee").is_in(official_user_ids).alias("is_official")
    )

    df_ss.write_parquet(args.save_path)


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
