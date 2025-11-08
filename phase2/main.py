"""
Main program file for running db loading code.
Data files should be placed in ../data from root directory
"""

from time import time
from phase2.db.config import connect_to_mongo
from phase1 import db as rdb
from phase2 import db as mdb


def main():
    # --- mongo part ---#
    # 1. mongodb setup (im aware will will want to change this to mimic rdb baseball class later)
    client, mdb = connect_to_mongo()

    # 2. load data into mongodb

    # --- rdb part ---#
    # 3. 5 interesting queries on rdb (time these queries and print the results)
    # 3.a query
    start_time = time.perf_counter()
    a = rdb.exec_commit("")
    end_time = time.perf_counter()
    print(f"Query 3.a took {end_time - start_time} seconds")

    # 3.b query
    start_time = time.perf_counter()
    b = rdb.exec_commit("")
    end_time = time.perf_counter()
    print(f"Query 3.b took {end_time - start_time} seconds")

    # 3.c query
    start_time = time.perf_counter()
    c = rdb.exec_commit("")
    end_time = time.perf_counter()
    print(f"")

    # 3.d query
    start_time = time.perf_counter()
    d = rdb.exec_commit("")
    end_time = time.perf_counter()
    print(f"Query 3.d took {end_time - start_time} seconds")

    # 3.e query
    start_time = time.perf_counter()
    e = rdb.exec_commit("")
    end_time = time.perf_counter()
    print(f"Query 3.e took {end_time - start_time} seconds")

    # 4. create indexes on rdb to speed up queries (time these again and print the results)

    # 5. discover functional dependencies in rdb

    # 6. ????normalize rdb based on discovered fds????


if __name__ == "__main__":
    main()
