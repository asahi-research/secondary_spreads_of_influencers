#!/usr/bin/env python3

import argparse
import os
from tqdm import tqdm
import polars as pl


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--rt_save_path", type=str, required=True)
arg_parser.add_argument("--retweet_ym", type=str, required=True)
arg_parser.add_argument("--retweets_dir", type=str, required=True)
arg_parser.add_argument("--qt", action="store_true")
arg_parser.add_argument("--rt", action="store_true")


# [File Summary]
# This script extracts timestamps from the retweets.
# [Configs]
# rt:
#   If True, the retweets are extracted.
# qt:
#   If True, the quoted tweets are extracted.
# [Inputs]
# retweets_dir:
#   X (Twitter) API's retweets saved directory
# [Outputs]
# rt_save_path:
#   Path to save the retweets with timestamps.
#   The retweets are saved in the parquet format.
# retweet_ym:
#   Year and month of the retweets.
#   The format is "YYYY-MM".
#   The retweets are saved in the directory named by the year and month.


def main(args):
    if not args.rt and not args.qt:
        raise ValueError("Either --rt or --qt must be set")

    retweet_files = []
    for retweet_file in os.listdir(f"{args.retweets_dir}/{args.retweet_ym}"):
        if retweet_file.endswith('.gz'):
            if (args.rt and "rt_" in retweet_file) or (args.qt and "qt_" in retweet_file):
                retweet_files.append(f"{args.retweet_ym}/{retweet_file}")

    df_rt = []

    for retweet_file in tqdm(retweet_files):
        df = pl.read_csv(f"{args.retweets_dir}/{retweet_file}", has_header=False, separator='\t')
        df_rt.append(df)

    df_rt = pl.concat(df_rt)

    df_rt.columns = [
        "tweet_id",
        "user_id",
        "user_screen_name",
        "timestamp",
        "source_tweet_id",
        "source_user_id",
        "source_user_screen_name",
        "source_timestamp",
    ]

    df_rt = df_rt[["tweet_id", "timestamp", "source_tweet_id", "source_timestamp"]]

    df_rt_ = df_rt.unique(subset=["tweet_id"])
    df_rt_ = df_rt.sort("timestamp")
    df_rt_.write_parquet(args.save_path)


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
