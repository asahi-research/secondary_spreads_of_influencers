#!/usr/bin/env python3

from typing import Set, List
from itertools import combinations
from pydantic import BaseModel
from neo4j import GraphDatabase
from tqdm import tqdm


class User(BaseModel):
    user_id: int
    follower_ids: Set[int] = set()
    following_ids: Set[int] = set()

    def add_follower_id(self, follower_id: int) -> None:
        self.follower_ids.add(int(follower_id))

    def has_follower_id(self, follower_id: int) -> bool:
        return follower_id in self.follower_ids

    def add_following_id(self, following_id: int) -> None:
        self.following_ids.add(int(following_id))

    def search_following_id(self, following_id: int) -> bool:
        return following_id in self.following_ids


class Tweet(BaseModel):
    tweet_id: int
    user_id: int
    timestamp: int
    is_quote: bool = False

    def __lt__(self, other):
        return self.timestamp < other.timestamp


class Neo4jClient:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()


class Neo4jNetwork:
    # このクラスは同じツイートをリツイートしたかでユーザ間にエッジが張られるネットワークを構築する
    # User AとUser Bが同じツイートをリツイートした場合、User AとUser Bの間にエッジを張る
    # 同じツイートをリツイートすればするほど、エッジの重みが増す
    # 無向グラフとして扱う

    def __init__(self, client: Neo4jClient):
        self.client = client

    def create_users(self, user_ids: Set[int], num_chunks: int=10000):
        with self.client.driver.session() as session:
            session.write_transaction(self._create_users, user_ids, num_chunks)

    @staticmethod
    def _create_users(tx, user_ids: Set[int], num_chunks: int):
        user_id_chunks = [user_ids[i:i + num_chunks] for i in range(0, len(user_ids), num_chunks)]

        for user_id_chunk in tqdm(user_id_chunks):
            tx.run("""
                UNWIND $user_id_chunk AS user_id
                MERGE (u:User {user_id: user_id})
            """, user_id_chunk=user_id_chunk)

    def create_user(self, user_id: int):
        with self.client.driver.session() as session:
            session.write_transaction(self._create_user, user_id)

    @staticmethod
    def _create_user(tx, user_id: int):
        tx.run("CREATE (u:User {user_id: $user_id})",
               user_id=user_id)

    def create_relations(self, retweet_user_ids: List[int], num_chunks: int=10000):
        with self.client.driver.session() as session:
            session.write_transaction(self._create_relations, retweet_user_ids, num_chunks)

    @staticmethod
    def _create_relations(tx, retweet_user_ids: List[int], num_chunks):
        user_id_combinations = list(combinations(retweet_user_ids, 2))
        user_id_combinations_chunks = [user_id_combinations[i:i + num_chunks] for i in range(0, len(user_id_combinations), num_chunks)]

        for user_id_combinations_chunk in user_id_combinations_chunks:
            tx.run("""
                UNWIND $user_id_combinations AS user_id_comb
                MATCH (u1:User {user_id: user_id_comb[0]})
                MATCH (u2:User {user_id: user_id_comb[1]})
                MERGE (u1)-[r:RELATED]-(u2)
                ON CREATE SET r.weight = 1
                ON MATCH SET r.weight = r.weight + 1
            """, user_id_combinations=user_id_combinations_chunk)
        # for user_id_comb in user_id_combinations:
        #     tx.run("""
        #         UNWIND $user_id_comb AS user_id_comb
        #         MATCH (u1:User {user_id: user_id_comb[0]})
        #         MATCH (u2:User {user_id: user_id_comb[1]})
        #         MERGE (u1)-[r:RELATED]-(u2)
        #         ON CREATE SET r.weight = 1
        #         ON MATCH SET r.weight = r.weight + 1
        #     """, user_id_comb=user_id_comb)
