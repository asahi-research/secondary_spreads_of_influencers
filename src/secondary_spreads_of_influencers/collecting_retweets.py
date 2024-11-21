#!/usr/bin/env python3

import argparse
import os
import json
from tqdm import tqdm
import polars as pl


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--save_path", type=str, required=True)
arg_parser.add_argument("--retweet_ym", type=str, required=True)
arg_parser.add_argument("--retweets_dir", type=str, required=True)
arg_parser.add_argument("--qt", action="store_true")
arg_parser.add_argument("--rt", action="store_true")


# [File Summary]
# This script aggregates the retweets
#   by the source tweet for building the retweet cascades.
# [Configs]
# rt:
#   If True, the retweets are extracted.
# qt:
#   If True, the quoted tweets are extracted.
# retweet_ym:
#   Year and month of the retweets.
# [Inputs]
# retweets_dir:
#   X (Twitter) API's retweets saved directory.
# [Outputs]
# save_path:
#   Path to save the retweets with timestamps.


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

    df_rt = df_rt.unique(subset=["tweet_id", "source_tweet_id"])

    with open(f"{args.save_path}/{args.retweet_ym}_retweets.jsonl", "w") as f:
        for source_tweet_id, df_rt_k in df_rt.group_by("source_tweet_id"):
            retweet_data = {
                "source_tweet_id": source_tweet_id,
                "source_user_id": df_rt_k["source_user_id"].to_list()[0],
                "source_timestamp": df_rt_k["source_timestamp"].to_list()[0],
                "retweets": df_rt_k["tweet_id"].to_list(),
                "retweeted_user_ids": df_rt_k["user_id"].to_list(),
                "retweet_timestamps": df_rt_k["timestamp"].to_list(),
                "cascade_size": len(df_rt_k),
            }

            f.write(json.dumps(retweet_data, ensure_ascii=False))
            f.write("\n")


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
