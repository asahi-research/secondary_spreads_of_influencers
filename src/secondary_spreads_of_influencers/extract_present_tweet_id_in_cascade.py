#!/usr/bin/env python3

import os
import argparse
import polars as pl


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--retweeted_tweets_dir", type=str, required=True)
arg_parser.add_argument("--rt_cascades_path", type=str, required=True)
arg_parser.add_argument("--save_presence_tweet_ids_path", type=str, required=True)
arg_parser.add_argument("--save_cover_rate_path", type=str, required=True)


# [File Summary]
# This script extracts the tweet ids that are present in the retweet cascades.
# [Inputs]
# rt_cascades_path:
#    Path to the retweet cascades.
#    The retweet cascades are saved in the ndjson format.
#    This file is outputed by build_rt_cascades.py.
# retweeted_tweets_dir:
#   Directory path where the retweeted tweets are stored.
#   This files outputed by format_retweeted_data.py.
# [Outputs]
# save_presence_tweet_ids_path:
#   Path to save the tweet ids that are present in the retweet cascades.
# save_cover_rate_path:
#   Path to save the cover rate of the retweet cascades.


def main(args):
    print("Loading retweet cascades...")
    df_rt_cascade = pl.read_ndjson(args.rt_cascades_path)
    df_rt_cascade = df_rt_cascade.with_columns(
        pl.col("graph").struct[3].list.len().alias("num_nodes"),
        pl.col("graph").struct[3].alias("data"),
    ).drop("graph")
    df_nodes = df_rt_cascade.explode("data")
    df_nodes = df_nodes.with_columns(
        pl.col("data").struct[1].alias("tweet_id")).drop("data")
    df_rt_cascade = df_rt_cascade.drop("data")

    print("Loading retweeted tweets...")
    retweeted_tweets_files = os.listdir(args.retweeted_tweets_dir)
    retweeted_tweets_paths = [os.path.join(args.retweeted_tweets_dir, f)
                              for f in retweeted_tweets_files]

    df_list = []
    for i, path in enumerate(retweeted_tweets_paths):
        df_rt = pl.read_parquet(path)
        df_list.append(df_rt)
    df_rt = pl.concat(df_list)
    del df_list
    df_rt = df_rt.filter(df_rt["user_id"].is_not_null())
    df_rt = df_rt[["source_tweet_id", "user_id", "tweet_id"]]
    df_rt = df_rt.filter(df_rt["source_tweet_id"].is_not_null())

    df_rt = df_rt.join(
        df_nodes, on=["source_tweet_id", "tweet_id"], how="left")

    # nullでないtweet_idを持つ行を保存 = present ids
    df_rt.filter(
        ~pl.col("num_nodes").is_null()
    )[["tweet_id"]].write_parquet(args.save_path)

    df_rt.groupby("source_tweet_id").agg(
        pl.len().alias("num_retweets"),
        (~pl.col("num_nodes").is_null()).sum().alias("num_nodes"),
    ).with_columns(
        (pl.col("num_nodes") / pl.col("num_retweets")).alias("cover_rate")
    ).sort(["cover_rate", "num_retweets"], descending=True).write_parquet(args.save_cover_rate_path)


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
