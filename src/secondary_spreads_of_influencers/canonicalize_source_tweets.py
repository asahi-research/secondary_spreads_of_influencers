#!/usr/bin/env python3

import argparse
import polars as pl
import os
from utils import Tokenizer


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--source_tweets_dir", type=str, required=True)
arg_parser.add_argument("--data_save_path", type=str, required=True)

# [File Summary]
# This script canonicalizes the source tweets.
# [Input]
# source_tweets_dir:
#   Directory containing the source tweets.
#   Output of the `format_source_tweets.py`.
# [Output]
# data_save_path:
#  Path to save the canonicalized source tweets.
#  The canonicalized source tweets are saved in the parquet format.


def main(args):
    df_list = []
    source_tweets_files = os.listdir(args.source_tweets_dir)
    source_tweets_paths = [os.path.join(args.source_tweets_dir, f)
                           for f in source_tweets_files]

    for i, path in enumerate(source_tweets_paths):
        df = pl.read_parquet(path)
        df_list.append(df)

    df_source = pl.concat(df_list)
    df_source = df_source.group_by(["source_tweet_id"]).agg(
        pl.first("source_user_id").alias("user_id"),
        pl.first("source_name").alias("name"),
        pl.first("source_screen_name").alias("screen_name"),
        pl.first("source_full_text").alias("tweet_text"),
        pl.first("source_description").alias("description"),
        pl.first("source_followers_count").alias("followers_count"),
        pl.first("source_friends_count").alias("friends_count"),
        pl.first("source_entities").alias("entities"),
        pl.first("source_extended_entities").alias("extended_entities"),
    )

    df_source = df_source.rename({"source_tweet_id": "tweet_id"})

    df_source = df_source.with_columns(
        pl.col("tweet_text").map_elements(
            Tokenizer(),
            return_dtype=pl.List(pl.String)
        ).alias("tokenized_text")
    )

    df_source.write_parquet(args.data_save_path)


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
