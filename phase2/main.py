"""
Main program file for running db loading code.
Data files should be placed in ../data from root directory
"""

from time import time
from phase2.db.config import connect_to_mongo
from phase1 import db as rdb
from phase2 import db as mdb


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
    # create pandas df to compare unindexed vs indexed results
    print("RDB Query Performance:")
    # 5. discover functional dependencies in rdb
    fds = rdb.discover_fds()
    print("Discovered FDs:", fds)
    # 6. ????normalize rdb based on discovered fds????


if __name__ == "__main__":
    main()
