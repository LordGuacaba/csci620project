"""
Main program file for running db loading code.
Data files should be placed in ../data from root directory
"""

import time
import db.postgres as rdb
from db.config import connect_to_mongo
import pandas as pd
import psycopg2


def get_pda_dataframe(unindexed_results, indexed_results):

    data = {
        "Query": [],
        "Unindexed Time (s)": [],
        "Indexed Time (s)": [],
        "Speedup": [],
    }

    for key in unindexed_results.keys():
        unindexed_time = unindexed_results[key]
        indexed_time = indexed_results[key]
        speedup = unindexed_time / indexed_time if indexed_time > 0 else float("inf")

        data["Query"].append(key)
        data["Unindexed Time (s)"].append(unindexed_time)
        data["Indexed Time (s)"].append(indexed_time)
        data["Speedup"].append(speedup)

    df = pd.DataFrame(data)
    return df


def rdb_queries():
    time_results = {}
    # 3.a query
    start_time = time.perf_counter()
    a = rdb.exec_commit("")
    end_time = time.perf_counter()
    time_results["3.a"] = end_time - start_time

    # 3.b query
    start_time = time.perf_counter()
    b = rdb.exec_commit("")
    end_time = time.perf_counter()
    time_results["3.b"] = end_time - start_time

    # 3.c query
    start_time = time.perf_counter()
    c = rdb.exec_commit("")
    end_time = time.perf_counter()
    time_results["3.c"] = end_time - start_time

    # 3.d query
    start_time = time.perf_counter()
    d = rdb.exec_commit("")
    end_time = time.perf_counter()
    time_results["3.d"] = end_time - start_time

    # 3.e query
    start_time = time.perf_counter()
    e = rdb.exec_commit("")
    end_time = time.perf_counter()
    time_results["3.e"] = end_time - start_time

    return time_results


def create_rdb_indexes():
    # 4.a create index
    rdb.exec_commit("")

    # 4.b create index
    rdb.exec_commit("")

    # 4.c create index
    rdb.exec_commit("")

    # 4.d create index
    rdb.exec_commit("")

    # 4.e create index
    rdb.exec_commit("")


def main():
    # --- mongo part ---#
    # 1. mongodb setup (im aware will will want to change this to mimic rdb baseball class later)
    client, mdb = connect_to_mongo()

    # 2. load data into mongodb

    # --- rdb part ---#
    # 3. 5 interesting queries on rdb (time these queries and print the results)
    unindexed_results = rdb_queries()

    # 4. create indexes on rdb to speed up queries (time these again and print the results)
    create_rdb_indexes()
    indexed_results = rdb_queries()

    df = get_pda_dataframe(unindexed_results, indexed_results)
    print("RDB Query Performance:")
    print(df)

    # 5. discover functional dependencies in rdb
    # this is achieved in phase2/discover_fds.py
    # results are output to phase2_fd_results.csv and phase2_fd_results.txt


if __name__ == "__main__":
    main()
