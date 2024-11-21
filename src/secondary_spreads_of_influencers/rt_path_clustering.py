#!/usr/bin/env python3

import pickle
import os
import argparse
import polars as pl
import networkx as nx


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--canonicalized_source_tweets_path", type=str, required=False)
arg_parser.add_argument("--retweeted_tweets_dir", type=str, required=False)
arg_parser.add_argument("--saved_graph_path", type=str, required=False)
arg_parser.add_argument("--graph_save_path", type=str, required=False)
arg_parser.add_argument("--clusters_save_path", type=str, required=True)
arg_parser.add_argument("--algorithm", type=str, default="louvain")
arg_parser.add_argument("--undirected", action="store_true")


# [File Summary]
# This script computes the clustering of the users.
# [Configs]
# algorithm:
#   The algorithm to compute the clustering.
#   Currently, only "louvain" is supported.
# undirected:
#   If this is provided, the graph is treated as undirected.
# [Inputs]
# canonicalized_source_tweets_path:
#   Path to the canonicalized source tweets.
#   The source tweets are canonicalized by the canonicalize_source_tweets.py.
# retweeted_tweets_dir:
#   Directory to the retweeted tweets.
#   This files outputed by format_retweeted_data.py.
# saved_graph_path:
#   Path to the saved graph.
#   The graph is saved in the pickle format.
#   If this is provided, the script will load the graph from the file.
# [Outputs]
# graph_save_path:
#   Path to save the graph.
#   The graph is saved in the pickle format.
# clusters_save_path:
#   Path to save the clusters.
#   The clusters are saved in the parquet format.


def main(args):
    if args.saved_graph_path is not None:
        print("Loading Graph...")
        G = pickle.load(open(args.saved_graph_path, "rb"))
        user_ids = list(G.nodes)
    else:
        if args.graph_save_path is None:
            raise ValueError("graph_save_path is required when saved_graph_path is not provided.")

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

        df_source = pl.read_parquet(args.canonicalized_source_tweets_path)
        df_source = df_source.filter(df_source["tweet_id"].is_not_null())
        df_source = df_source[["tweet_id", "user_id"]]

        df = df_source.join(
            df_rt,
            left_on="tweet_id",
            right_on="source_tweet_id",
            how="inner")
        df = df.rename({"user_id": "source_user_id", "user_id_right": "user_id"})

        df = df.group_by("user_id", "source_user_id").agg(
            pl.count("source_user_id").alias("num_rt_users")
        )

        user_ids = list(set(df["user_id"].to_list() + df["source_user_id"].to_list()))
        user_ids = sorted(user_ids)

        G = nx.DiGraph()
        G.add_nodes_from(user_ids)

        print("Loading relations...")
        G.add_weighted_edges_from(df[["user_id", "source_user_id", "num_rt_users"]].rows())

        print("Saving Graph...")
        pickle.dump(G, open(args.graph_save_path, "wb"), protocol=4)

    if args.undirected:
        G = G.to_undirected()

    print("Clustering...")
    if args.algorithm == "louvain":
        clusters = nx.community.louvain_communities(
            G,
            weight="weight",
            seed=42,
        )
        user_id2cluster = {}
        for i, cluster in enumerate(clusters):
            for user_id in cluster:
                user_id2cluster[user_id] = i

        cluster_ids = [user_id2cluster[user_id] for user_id in user_ids]
    else:
        raise ValueError("Invalid algorithm: ", args.algorithm)

    print("Saving clusters...")
    pl.DataFrame({
        "user_id": user_ids,
        "cluster": cluster_ids,
    }).write_parquet(args.clusters_save_path)


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
