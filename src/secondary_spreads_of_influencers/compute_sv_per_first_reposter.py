#!/usr/bin/env python3

import os
import argparse
import networkx as nx
import polars as pl


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--retweeted_tweets_dir", type=str, required=True)
arg_parser.add_argument("--rt_cascades_path", type=str, required=True)
arg_parser.add_argument("--min_rt", type=int, default=0)
arg_parser.add_argument("--save_path", type=str, required=True)


def subtree_moments(G, root):
    if G.out_degree(root) == 0:  # Leaf node
        return 1, 1, 1

    size = 1
    sum_sizes = 1
    sum_sizes_sqr = 1

    for child in G.successors(root):
        child_size, child_sum_sizes, child_sum_sizes_sqr = subtree_moments(G, child)
        size += child_size
        sum_sizes += child_sum_sizes
        sum_sizes_sqr += child_sum_sizes_sqr

    sum_sizes += size
    sum_sizes_sqr += size ** 2

    return size, sum_sizes, sum_sizes_sqr


def average_distance(G, root):
    size, sum_sizes, sum_sizes_sqr = subtree_moments(G, root)
    if size <= 1:
        return 0
    dist_avg = (2 * size / (size - 1)) * (sum_sizes / size - sum_sizes_sqr / size**2)
    return dist_avg


def compute_structural_virality(G_rt_cascade: nx.Graph):
    # v(T) = 1 / (|V| * (|V| - 1)) * sum_{u in V} ( sum_{v in V} (d(u, v)) )
    # v(T) = structural virality of T
    # d(u, v) = shortest path length between u and v
    # V = set of nodes in T
    # u, v = nodes in T

    # num_nodes = len(G_rt_cascade)
    # if num_nodes <= 1:
    #     return 0.0

    # dists = nx.shortest_path_length(G_rt_cascade)

    # total = 0
    # for source, dist_dict in dists:
    #     for target, dist in dist_dict.items():
    #         total += dist

    # return (1 / (num_nodes * (num_nodes - 1))) * total

    # indexやnextで取れないため
    for node in G_rt_cascade.nodes:
        root = node
        break
    return average_distance(G_rt_cascade, root)


def compute_max_depth(G):
    return nx.dag_longest_path_length(G)


def decompose_after_root(G):
    nodes = list(G.nodes)[1:]
    H = nx.subgraph(G, nodes)
    Qs = [H.subgraph(comps) for comps in nx.weakly_connected_components(H)]
    if len(Qs) == 0:
        return []
    return Qs


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
        pl.col("user_id").n_unique().alias("num_retweets")
    )

    target_source_tweet_ids = df_rt.filter(
        pl.col("num_retweets") >= args.min_rt)["source_tweet_id"].to_list()

    print("Loading retweet cascades...")
    df = pl.read_ndjson(args.rt_cascades_path)
    df = df.filter(df["source_tweet_id"].is_in(target_source_tweet_ids))

    df = df.with_columns(
        pl.col("graph").struct[3].list.len().alias("num_nodes"))

    print("Load NetworkX graphs...")
    df = df.with_columns(pl.col("graph").map_elements(
        lambda x: nx.node_link_graph(x),
        return_dtype=pl.Object).alias("graph"))

    print("Decompose subgraph of first reposters...")
    df = df.with_columns(pl.col("graph").map_elements(
        # lambda G: nx.ego_graph(G, [n for n in G.nodes][0], radius=1),
        lambda G: decompose_after_root(G),
        # return_dtype=pl.List(pl.Object),
    ).alias("graph"))

    df = df.explode("graph")
    def extract_root_user_id(G):
        return list(G.nodes(data=True))[0][1]["data"]["user_id"]
    df = df.with_columns(pl.col("graph").map_elements(
        lambda G: extract_root_user_id(G),
    ).alias("root_user_id"))

    print("Computing structural virality...")
    df = df.with_columns(pl.col("graph").map_elements(
        compute_structural_virality,
        return_dtype=pl.Float64,
    ).alias("structural_virality"))

    print("Computing max depth...")
    df = df.with_columns(pl.col("graph").map_elements(
        compute_max_depth,
        return_dtype=pl.Int64,
    ).alias("max_depth"))

    print("Saving...")
    df[["source_tweet_id", "root_user_id", "structural_virality", "max_depth", "num_nodes"]].write_parquet(args.save_path)


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
