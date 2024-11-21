#!/usr/bin/env python--rt_cascades_path3

import argparse
import networkx as nx
import polars as pl


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--rt_cascades_path", type=str, required=True)
arg_parser.add_argument("--retweeted_tweets_dir", type=str, required=True)
arg_parser.add_argument("--min_rt", type=int, default=0)
arg_parser.add_argument("--save_path", type=str, required=True)


# [File Summary]
# This script computes the structural virality of the retweet cascades.
# [Configs]
# min_rt:
#   Minimum number of retweets in the cascade.
# [Inputs]
# rt_cascades_path:
#   Path to the retweet cascades.
# retweeted_tweets_dir:
#   Directory to the retweeted tweets.
#   This files outputed by format_retweeted_data.py.
# [Outputs]
# save_path:
#   Path to save the computed structural virality.


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


def main(args):
    print("Loading retweet cascades...")
    df = pl.read_ndjson(args.rt_cascades_path)

    df = df.with_columns(
        pl.col("graph").struct[3].list.len().alias("num_nodes"))

    print("Load NetworkX graphs...")
    df = df.with_columns(pl.col("graph").map_elements(
        lambda x: nx.node_link_graph(x),
        return_dtype=pl.Object).alias("graph"))

    print("Computing structural virality...")
    df = df.with_columns(pl.col("graph").map_elements(
        compute_structural_virality,
        return_dtype=pl.Float64,
    ).alias("structural_virality"))

    print("Saving...")
    df[["source_tweet_id", "structural_virality", "num_nodes"]].write_parquet(args.save_path)


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
