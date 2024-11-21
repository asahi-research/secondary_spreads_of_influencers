#!/usr/bin/env python3

import argparse
import os
from tqdm import tqdm
import polars as pl


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--save_path", type=str, required=True)
arg_parser.add_argument("--follower_dir", type=str, required=True)
arg_parser.add_argument("--following_dir", type=str, required=True)


# [File Summary]
# This script builds the follower relations from the follower and following relations.
# [Inputs]
# follower_dir:
#   Directory path where the follower relations are stored.
#     Data Format (follower_dir/following_dir)
#     follower_dir/2021-10-02.txt (ex. sampled follower at 2021-10-02)
#   File:
#     following_user_id;follower_user_id
#     11568;11489
# following_dir:
#   Directory path where the following relations are stored.
#     following_dir/2021-10-02.txt (ex. sampled following at 2021-10-02)
#   File:
#     follower_user_id;following_user_id
#     12872;11993
# [Outputs]
# save_path:
#   Path to save the follower relations.


def main(args):
    follower_files = os.listdir(args.follower_dir)
    following_files = os.listdir(args.following_dir)

    follower_target_files = []
    for follower_file in follower_files:
        if "unknown.err" in follower_file:
            continue
        follower_target_files.append(f"{args.follower_dir}/{follower_file}")

    records = set()  # [(follower_id, user_id), ...)]

    for follower_file in tqdm(follower_target_files):
        df = pl.read_csv(
            follower_file,
            separator=";",
            has_header=False,
            new_columns=["user_id", "follower_id"],
            schema={"user_id": pl.Int64, "follower_id": pl.Int64},
        )
        records.update(df[["follower_id", "user_id"]].rows())

    following_target_files = []
    for following_file in following_files:
        if "unknown.err" in following_file:
            continue
        following_target_files.append(f"{args.following_dir}/{following_file}")

    for following_file in tqdm(following_target_files):
        df = pl.read_csv(
            following_file,
            separator=";",
            has_header=False,
            new_columns=["user_id", "following_id"],
            schema={"user_id": pl.Int64, "following_id": pl.Int64},
        )
        records.update(df[["user_id", "following_id"]].rows())

    df = pl.DataFrame(list(records),
                      schema=["follower", "followee"],
                      orient="row")
    df.write_parquet(args.save_path)


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
