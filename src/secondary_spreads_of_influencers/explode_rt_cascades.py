#!/usr/bin/env python3

import argparse
import polars as pl
import igraph as ig
import networkx as nx


arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--rt_cascades_path", type=str, required=True)
arg_parser.add_argument("--parent_child_nodes_save_path", type=str, required=True)


# [File Summary]
# This script extracts the parent and child nodes
#   from the retweet cascades for computing second spreaders.
# [Inputs]
# rt_cascades_path:
#    Path to the retweet cascades.
#    The retweet cascades are saved in the parquet format.
# [Outputs]
# parent_child_nodes_save_path:
#   Path to save transformed rt cascades as data frame style.


def extract_parent_and_child_nodes(G):
    source_tweet_id = G.vs[0]["data"]["tweet_id"]

    node_tuples = []  # (parent, child)
    for edge in G.es:
        parent_tweet_id = G.vs[edge.source]["data"]["tweet_id"]
        if parent_tweet_id == source_tweet_id:
            continue

        parent = G.vs[edge.source]["data"]["user_id"]
        child = G.vs[edge.target]["data"]["user_id"]
        child_tweet_id = G.vs[edge.target]["data"]["tweet_id"]
        node_tuples.append({"parent": parent,
                            "child": child,
                            "parent_tweet_id": parent_tweet_id,
                            "child_tweet_id": child_tweet_id})
    return node_tuples


def main(args):
    df_rt_cascade = pl.read_ndjson(args.rt_cascades_path)
    df_rt_cascade = df_rt_cascade.with_columns(
        pl.col("graph").struct[3].list.len().alias("num_nodes"))

    df_rt_cascade = df_rt_cascade.filter(df_rt_cascade["num_nodes"] > 1)
    df_rt_cascade = df_rt_cascade.with_columns(pl.col("graph").map_elements(
        lambda x: ig.Graph.from_networkx(nx.node_link_graph(x)),
        return_dtype=pl.Object).alias("graph"))

    dtype = pl.List(pl.Struct(
        {"parent": pl.Int64, "child": pl.Int64, "parent_tweet_id": pl.Int64, "child_tweet_id": pl.Int64,}))
    df_rt_cascade = df_rt_cascade.with_columns(pl.col("graph").map_elements(
        lambda G: extract_parent_and_child_nodes(G),
        return_dtype=dtype).alias("parent_child_nodes"))

    df_parent_child_nodes = df_rt_cascade[
        ["source_tweet_id", "parent_child_nodes"]
    ].explode("parent_child_nodes")
    df_parent_child_nodes = df_parent_child_nodes.unnest("parent_child_nodes")
    df_parent_child_nodes = df_parent_child_nodes.drop_nulls(subset=["parent",
                                                                     "child",
                                                                     "parent_tweet_id",
                                                                     "child_tweet_id"])

    df_parent_child_nodes.write_parquet(args.parent_child_nodes_save_path)


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
