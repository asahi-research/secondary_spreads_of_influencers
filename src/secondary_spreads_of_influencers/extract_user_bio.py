#!/usr/bin/env python3

import argparse
import polars as pl
import os
from utils import clean_text, Tokenizer


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--canonicalized_source_tweets_path", type=str, required=True)
arg_parser.add_argument("--retweeted_tweets_dir", type=str, required=True)
arg_parser.add_argument("--data_save_path", type=str, required=True)


# [File Summary]
# This script extracts the user bio information from the source and retweeted tweets.
# [Inputs]
# canonicalized_source_tweets_path:
#   Path to the canonicalized source tweets.
#   The source tweets are canonicalized by the canonicalize_source_tweets.py.
# retweeted_tweets_dir:
#   Directory to the retweeted tweets.
#   This files outputed by format_retweeted_data.py.
# [Outputs]
# data_save_path:
#  Path to save the user bio information.


def main(args):
    df_list = []
    retweeted_tweets_files = os.listdir(args.retweeted_tweets_dir)
    retweeted_tweets_paths = [os.path.join(args.retweeted_tweets_dir, f)
                              for f in retweeted_tweets_files]

    for i, path in enumerate(retweeted_tweets_paths):
        df = pl.read_parquet(path)
        df_list.append(df)

    df_retweet = pl.concat(df_list)
    df_retweet = df_retweet.group_by(["user_id"]).agg(
        pl.first("name").alias("name"),
        pl.first("screen_name").alias("screen_name"),
        pl.first("description").alias("description"),
        pl.first("followers_count").alias("followers_count"),
        pl.first("friends_count").alias("friends_count"),
    )

    df_source = pl.read_parquet(args.canonicalized_source_tweets_path)
    df_source = df_source[[
        "user_id",
        "name",
        "screen_name",
        "description",
        "followers_count",
        "friends_count"]]

    df = pl.concat([df_retweet, df_source])
    df = df.unique(subset=["user_id"])
    df = df.filter(df["description"].is_not_null())
    df = df.with_columns(
        pl.col("description").map_elements(lambda x: clean_text(
            x), return_dtype=pl.String).alias("cleaned_description"))
    df = df.with_columns(
        pl.col("description").map_elements(
            lambda x: clean_text(x, symbolize=set(), remove_nlsp=False),
            return_dtype=pl.String).alias("text_for_topic_model"))

    tokenize = Tokenizer()
    df = df.with_columns(
        pl.col("description").map_elements(
            lambda x: tokenize(x),
            return_dtype=pl.List(pl.String)).alias("tokenized_description"))

    df.write_parquet(args.data_save_path)


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
