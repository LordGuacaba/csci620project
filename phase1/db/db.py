"""
Provides postgres db helper methods.
"""

from db.config import connect
from model.relations import Relation   

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