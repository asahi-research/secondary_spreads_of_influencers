#!/usr/bin/env python3

import argparse
import os
import polars as pl
import matplotlib as mpl
mpl.use("WebAgg")
mpl.rc("webagg", port=8899, port_retries=10, open_in_browser=False)
import matplotlib.pyplot as plt
import seaborn as sns
import scienceplots


arg_parser = argparse.ArgumentParser()
# arg_parser.add_argument("--userinfo_path", type=str, required=True)
arg_parser.add_argument("--retweeted_tweets_dir", type=str, required=True)
arg_parser.add_argument("--exclude_qt", action="store_true")
arg_parser.add_argument("--qt_ids_path", type=str, required=False)
arg_parser.add_argument("--user_influence_score_path", type=str, required=True)
arg_parser.add_argument("--influence_column_name", type=str, default="h-index")
arg_parser.add_argument("--userinfo_path", type=str, required=True)
arg_parser.add_argument("--exclude_official", action="store_true")
arg_parser.add_argument("--rt_timestamps_path", type=str, required=False)
arg_parser.add_argument("--filter_hours", type=int, default=-1)
arg_parser.add_argument("--output_dir", type=str, required=True)


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

    if args.exclude_qt:
        if not args.qt_ids_path:
            raise ValueError("qt_ids_path is required when exclude_qt is True")
        df_qt_ids = pl.read_parquet(args.qt_ids_path)
        df_rt = df_rt.filter(~df_rt["source_tweet_id"].is_in(df_qt_ids["tweet_id"]))

    if args.filter_hours > 0:
        df_rt_timestamp = pl.read_parquet(args.rt_timestamps_path)
        df_rt = df_rt.join(
            df_rt_timestamp,
            left_on="tweet_id",
            right_on="tweet_id",
            how="inner"
        )
        df_rt = df_rt.with_columns(
            (pl.col("timestamp") - pl.col("source_timestamp")).alias("passed_seconds"),
        ).filter(pl.col("passed_seconds") <= args.filter_hours * 3600)

    print("Num Retweeted Tweets: ", df_rt["source_tweet_id"].n_unique())
    print("Num Retweeters: ", df_rt["user_id"].n_unique())

    pl.DataFrame({
        "num_retweets": [df_rt["source_tweet_id"].n_unique()],
        "num_retweeters": [df_rt["user_id"].n_unique()],
    }).write_excel(args.output_dir + "num_retweets.xlsx")

    print("Computing CCDF...")
    df_rt = df_rt.group_by("user_id").agg(
        pl.len().alias("num_retweets"),
        pl.col("source_tweet_id").n_unique().alias("num_unique_retweets"),
    )

    df_user_influence = pl.read_parquet(args.user_influence_score_path)
    df_user_influence = df_user_influence.with_columns(
        pl.col(args.influence_column_name).qcut(
            [0.5, 0.7, 0.9, 0.95, 0.99],
            labels=["low", "low-mid", "mid", "high-mid", "high", "very-high"],
            allow_duplicates=True,
        ).alias("user_influence"))

    df_rt_ui = df_rt.join(
        df_user_influence,
        on="user_id",
        how="inner",
        coalesce=True
    )

    if args.exclude_official:
        df_rt_ui = df_rt_ui.filter(~df_rt_ui["user_id"].is_in(official_user_ids))

    df_result = df_rt_ui.group_by("user_influence").agg(
        pl.col("num_retweets").mean().alias("mean_num_retweets"),
        pl.col("num_retweets").median().alias("median_num_retweets"),
        pl.col("num_retweets").std().alias("std_num_retweets"),
    ).sort("user_influence", descending=True)

    df_rt = df_rt.sort("num_retweets").with_columns(
        pl.col("num_retweets").rank("ordinal").alias("rank"),
        pl.col("num_retweets").cum_sum().alias("cumulative_num_retweets"),
    )

    df_rt = df_rt.with_columns(
        ((pl.col("cumulative_num_retweets").max() - pl.col("cumulative_num_retweets")) / pl.col("cumulative_num_retweets").max()).alias("ccdf"),
    )
    df_rt = df_rt.sort("num_retweets", descending=True)

    # 上位5%のユーザのRetweet数の%を見られる
    df_rt = df_rt.with_columns(
        pl.col("ccdf").rank("ordinal").alias("ccdf_rank")
    ).with_columns(
        pl.col("ccdf_rank") / pl.col("ccdf_rank").max()
    )

    # TODO: 投稿から6時間以内verを作成する
    shares = []
    for p in [1.0, 0.5, 0.3, 0.1, 0.05, 0.01]:
        df_rt_top = df_rt.filter(df_rt["ccdf_rank"] <= p)
        df_rt_top = df_rt_top.join(
            df_user_influence,
            on="user_id",
            how="inner",
            coalesce=True,
        )
        tweet_share = df_rt_top["ccdf"].max()

        df_rt_top = df_rt_top.group_by("user_influence").agg(
            pl.len().alias("num_users"),
            pl.Series([tweet_share] * len(df_rt_top), dtype=pl.Float64).alias("repost_share")
        ).with_columns(
            pl.col("num_users") / pl.col("num_users").sum(),
        ).sort("user_influence", descending=True)

        df_rt_top = df_rt_top.with_columns(
            pl.Series([p] * len(df_rt_top),
                      dtype=pl.Float32).alias("top_user_share"),
            (pl.col("num_users") * pl.col("repost_share")).alias("num_users_for_plot"),
        )

        shares.append(df_rt_top)
    df_rt_top = pl.concat(shares)
    df_result = df_rt_top.pivot(
        on="user_influence",
        index="top_user_share",
        values="num_users_for_plot").join(
            df_rt_top[["top_user_share", "repost_share"]].unique(),
        on="top_user_share").sort("top_user_share", descending=True)
    print(df_result)

    fig, ax = plt.subplots(1, 2, figsize=(12, 5))
    sns.lineplot(data=df_rt.to_pandas(), x="rank", y="ccdf", ax=ax[1])
    ax[1].set_xlabel("Number of Users")
    ax[1].set_ylabel("CCDF (Reposts)")
    ax[1].yaxis.set_major_formatter(mpl.ticker.PercentFormatter(1.0))

    ax[0].stackplot(
        df_result["top_user_share"].to_list(),
        *[
            df_result[col].to_list()
            for col in df_result.columns[1:-1]
        ],
        colors=reversed(sns.diverging_palette(220, 20)),
    )
    ax[0].set_xlabel("Top Reposter Percentage")
    ax[0].set_xscale("log")
    ax[0].xaxis.set_major_formatter(mpl.ticker.PercentFormatter(1.0))
    ax[0].set_xlim(0.01, 1.0)
    ax[0].set_ylabel("Repost Share")
    ax[0].yaxis.set_major_formatter(mpl.ticker.PercentFormatter(1.0))
    ax[0].legend(title="User Influence", loc="upper left", labels=df_result.columns[1:-1])

    fig, ax_ = plt.subplots(1, 1, figsize=(10, 5))
    cmap = sns.diverging_palette(220, 20)
    df_user_share = df_user_influence["user_influence"].value_counts()
    df_user_share = df_user_share.with_columns(
        pl.Series(reversed([cmap[i] for i in range(df_user_share.shape[0])])).alias("color")
    )
    df_user_share = df_user_share.sort("count", descending=True)
    df_plot = df_user_share.to_pandas().loc[:, ["user_influence", "count"]].set_index("user_influence")
    df_plot = ((df_plot / df_plot.sum()) * 100).T
    print(df_plot)
    df_plot.plot(
        kind="barh",
        stacked=True,
        ax=ax_,
        colormap=sns.diverging_palette(220, 20, as_cmap=True),
        legend=False,
        xlim=(0, 100),
    )
    ax_.tick_params(labelleft=False)
    ax_.axis("off")

    plt.show()

    df_result.write_excel(args.output_dir + "user_ccdf.xlsx")
    df_user_share.write_excel(args.output_dir + "user_share.xlsx")


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
