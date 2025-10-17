"""
Provides a class that connects to the postgres database and provides methods for updates and queries.
"""

import psycopg2
from model.relations import Relation

# DB connection settings
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "baseball_db"
DB_USER = "postgres"
DB_PASSWORD = "$nax459:)"
DB_SCHEMA = "baseball_db"

def connect():
    DB = dict(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
    return psycopg2.connect(**DB)   

def exec_file(filepath: str):
    """
    Execute the code in the provided sql file in the database
    """
    conn = connect()
    cur = conn.cursor()
    with open(filepath, 'r') as file:
        cur.execute(file.read())
    conn.commit()
    conn.close()

def exec_get_one(sql, args={}):
    conn = connect()
    cur = conn.cursor()
    cur.execute(sql, args)
    one = cur.fetchone()
    conn.close()
    return one

def exec_get_all(sql, args={}):
    conn = connect()
    cur = conn.cursor()
    cur.execute(sql, args)
    rows = cur.fetchall()
    conn.close()
    return rows

def exec_commit(sql, args={}):
    conn = connect()
    cur = conn.cursor()
    result = cur.execute(sql, args)
    conn.commit()
    conn.close()
    return result

def insert_relation_rows(rows: list[Relation]):
    cols = ", ".join([col for col in rows[0].cols])
    sql = f"INSERT INTO {rows[0].name} ({cols}) VALUES "
    values = []
    param_strings = []
    for row in rows:
        param_strings.append("(" + ", ".join(["%s" for _ in range(len(row.cols))]) + ")")
        for val in row.getValues():
            values.append(val)
    sql += ", ".join(param_strings)
    exec_commit(sql, values)