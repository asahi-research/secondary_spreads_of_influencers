#!/usr/bin/env python3

import os
import argparse
import polars as pl
from tqdm import tqdm


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--retweets_dir", type=str, required=True)
arg_parser.add_argument("--save_path", type=str, required=True)
arg_parser.add_argument("--only_qt_ids", action="store_true")


# [File Summary]
# This script extracts qt ids from the raw api response.
# [Configs]
# only_qt_ids:
#   If True, only the quoted tweet ids are extracted.
#   If False, the full text of the quoted tweets are extracted.
# [Inputs]
# retweets_dir:
#   X (Twitter) API's retweets saved directory
#   Retweets are saved in the raw format of the API except the created_at.
#   the created_at is transformed to the UNIX epoch time.
# [Outputs]
# save_path:
#  Path to save the extracted retweets.


def main(args):
    json_pathes = []
    qt_pathes = []
    retweet_files = os.listdir(args.retweets_dir)

    for retweet_file in retweet_files:
        if retweet_file.startswith('json_'):
            json_pathes.append(f"{args.retweets_dir}/{retweet_file}")
        if retweet_file.startswith('qt_'):
            qt_pathes.append(f"{args.retweets_dir}/{retweet_file}")

    df_qt = pl.concat([pl.read_csv(qt_path, separator="\t", has_header=False)
                       for qt_path in qt_pathes])

    if args.only_qt_ids:
        df_qt = df_qt.rename({"column_1": "tweet_id"})
        df_qt = df_qt[["tweet_id"]].unique(["tweet_id"])
        df_qt.write_parquet(args.save_path, compression="lz4")
        return
    qt_ids = df_qt["column_1"].to_list()

    retweet_dtype = pl.Struct([
        pl.Field("full_text", pl.String),
    ])

    df_list = []
    for json_path in tqdm(json_pathes):
        df_hour = pl.read_csv(json_path, separator="\t", has_header=False)
        df_hour.columns = ["tweet_id", "json_data"]
        df_hour = df_hour.filter(df_hour["tweet_id"].is_in(qt_ids))
        df_hour = df_hour.with_columns(
            pl.col("json_data").str.json_decode(retweet_dtype).alias("extracted_json_data"),
        ).with_columns(
            pl.col("extracted_json_data").struct[0].alias("full_text"),
        ).drop("json_data", "extracted_json_data")
        df_list.append(df_hour)

    df_rt_json = pl.concat(df_list)

    df_rt_json.with_columns(
        pl.col("json_data").str.json_decode(retweet_dtype).alias("extracted_json_data"),
    ).with_columns(
        pl.col("extracted_json_data").struct[0].alias("full_text"),
    ).drop("json_data", "extracted_json_data")
    df_rt_json.write_parquet(args.save_path, compression="lz4")


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
