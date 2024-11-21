#!/usr/bin/env python3

import argparse
import os
from collections import defaultdict
from tqdm import tqdm
import polars as pl


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--retweets_dir", type=str, required=True)
arg_parser.add_argument("--save_sources_dir", type=str, required=True)
arg_parser.add_argument("--save_retweets_dir", type=str, required=True)
arg_parser.add_argument("--force_overwrite", action="store_true")


# [File Summary]
# This script extracts and formats the retweets from the raw api response.
# [Inputs]
# retweets_dir:
#   X (Twitter) API's retweets saved directory
# [Outputs]
# save_sources_dir:
#   Directory to save the source tweets of the retweets.
#   The source tweets are saved in the parquet format.
# save_retweets_dir:
#   Directory to save the retweets.
#   The retweets are saved in the parquet format.


def main(args):
    json_pathes_dict = defaultdict(list)

    retweet_files = os.listdir(args.retweets_dir)
    for retweet_file in retweet_files:
        if retweet_file.startswith('json_'):
            date = retweet_file.split('_')[1].strip('.txt.gz')[:-3]
            json_pathes_dict[date].append(f"{args.retweets_dir}/{retweet_file}")

    for date, json_pathes in tqdm(json_pathes_dict.items()):
        if not args.force_overwrite and (
                os.path.exists(f"{args.save_retweets_dir}/{date}.parquet")
                and os.path.exists(f"{args.save_sources_dir}/{date}.parquet")):
            print(f"Skip {date}")
            continue

        df_list_by_date = []
        for json_path in tqdm(json_pathes):
            print(f"Reading {json_path}")
            try:
                df_hour = pl.read_csv(
                    json_path,
                    separator="\t",
                    has_header=False,
                    truncate_ragged_lines=True)
                df_hour.columns = ["tweet_id", "json_data"]
                df_hour = df_hour.filter(
                    df_hour["json_data"].str.starts_with("{")
                    & df_hour["json_data"].str.ends_with("}")
                )
            except Exception as err:
                print(f"Error: {err}")
                import ipdb; ipdb.set_trace()
            df_list_by_date.append(df_hour)

        df_rt_json = pl.concat(df_list_by_date)

        retweet_dtype = pl.Struct([
            pl.Field("user",
                     pl.Struct([
                         pl.Field("id", pl.Int64),
                         pl.Field("name", pl.String),
                         pl.Field("screen_name", pl.String),
                         pl.Field("description", pl.String),
                         pl.Field("followers_count", pl.Int64),
                         pl.Field("friends_count", pl.Int64),
                         pl.Field("created_at", pl.String)
                     ])),
            pl.Field("retweeted_status", pl.Struct([pl.Field("id", pl.Int64)])),
            pl.Field("full_text", pl.String),
        ])

        df_rt_json.with_columns(
            pl.col("json_data").str.json_decode(retweet_dtype).alias("extracted_json_data"),
        ).with_columns(
            pl.col("extracted_json_data").struct[0].struct[0].alias("user_id"),
            pl.col("extracted_json_data").struct[0].struct[1].alias("name"),
            pl.col("extracted_json_data").struct[0].struct[2].alias("screen_name"),
            pl.col("extracted_json_data").struct[0].struct[3].alias("description"),
            pl.col("extracted_json_data").struct[0].struct[4].alias("followers_count"),
            pl.col("extracted_json_data").struct[0].struct[5].alias("friends_count"),
            pl.col("extracted_json_data").struct[0].struct[6].alias("user_created_at"),
            pl.col("extracted_json_data").struct[1].struct[0].alias("source_tweet_id"),
            pl.col("extracted_json_data").struct[2].alias("full_text"),
        ).drop("json_data", "extracted_json_data").with_columns(
            pl.col("user_created_at").str.strptime(
                pl.Datetime, "%a %b %d %H:%M:%S %z %Y"
            ).dt.epoch(time_unit="s").alias("user_created_at"),
        ).write_parquet(
            f"{args.save_retweets_dir}/{date}.parquet", compression="lz4")

        source_dtype = pl.Struct([
            pl.Field("retweeted_status",
                     pl.Struct([
                         pl.Field("id", pl.Int64),
                         pl.Field("user",
                                 pl.Struct([
                                     pl.Field("id", pl.Int64),
                                     pl.Field("name", pl.String),
                                     pl.Field("screen_name", pl.String),
                                     pl.Field("description", pl.String),
                                     pl.Field("followers_count", pl.Int64),
                                     pl.Field("friends_count", pl.Int64),
                                     pl.Field("created_at", pl.String),
                                    ])),
                         pl.Field("full_text", pl.String),
                         pl.Field("entities", pl.Struct([
                             pl.Field("hashtags",
                                      pl.List(pl.Struct([
                                          pl.Field("text", pl.String)]))),
                             pl.Field("urls",
                                      pl.List(pl.Struct([
                                          pl.Field("expanded_url", pl.String)
                                      ]))),
                            ])),
                         pl.Field("extended_entities", pl.Struct([
                             pl.Field("media",
                                      pl.List(pl.Struct([
                                          pl.Field("type", pl.String)
                                      ])))
                            ]))
                     ]))
        ])

        df_rt_json.with_columns(
            pl.col("json_data").str.json_decode(source_dtype).alias("extracted_json_data"),
        ).with_columns(
            pl.col("extracted_json_data").struct[0].struct[0].alias("source_tweet_id"),
            pl.col("extracted_json_data").struct[0].struct[1].struct[0].alias("source_user_id"),
            pl.col("extracted_json_data").struct[0].struct[1].struct[1].alias("source_name"),
            pl.col("extracted_json_data").struct[0].struct[1].struct[2].alias("source_screen_name"),
            pl.col("extracted_json_data").struct[0].struct[1].struct[3].alias("source_description"),
            pl.col("extracted_json_data").struct[0].struct[1].struct[4].alias("source_followers_count"),
            pl.col("extracted_json_data").struct[0].struct[1].struct[5].alias("source_friends_count"),
            pl.col("extracted_json_data").struct[0].struct[1].struct[6].alias("source_user_created_at"),
            pl.col("extracted_json_data").struct[0].struct[2].alias("source_full_text"),
            pl.col("extracted_json_data").struct[0].struct[3].alias("source_entities"),
            pl.col("extracted_json_data").struct[0].struct[4].alias("source_extended_entities"),
        ).drop("json_data", "extracted_json_data").filter(
            pl.col("source_user_created_at") != "Thu Jan 01 00:00:00 +0000 1970"
        ).with_columns(
            pl.col("source_user_created_at").str.strptime(
                pl.Datetime, "%a %b %d %H:%M:%S %z %Y"
            ).dt.timestamp(time_unit="ms").cast(pl.String).str.strip_chars_end(
                "000").cast(pl.Int64).alias("source_user_created_at"),
        ).write_parquet(f"{args.save_sources_dir}/{date}.parquet", compression="lz4")


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
