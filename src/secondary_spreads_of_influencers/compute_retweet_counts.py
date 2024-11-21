#!/usr/bin/env python3

import os
import argparse
import polars as pl


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--retweeted_tweets_dir", type=str, required=True)
arg_parser.add_argument("--output_retweet_count_file", type=str, required=True)


# [File Summary]
# This script aggregates the number of retweets for each tweet.
# [Inputs]
# retweeted_tweets_dir:
#   Directory to the retweeted tweets.
#   This files outputed by format_retweeted_data.py.
# [Outputs]
# output_retweet_count_file:
#   Path to save the aggregated retweet count.


def main(args):
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
    df_rt = df_rt.group_by("source_tweet_id").agg(
        pl.col("user_id").n_unique().alias("num_source_tweets")
    )

    df_rt.write_parquet(args.output_retweet_count_file)


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
