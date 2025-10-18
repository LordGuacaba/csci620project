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
    with open(filepath, "r") as file:
        cur.execute(file.read())
    conn.commit()
    conn.close()


def exec_commit(sql, args={}):
    """
    Execute an update to the database.
    Params:
    - sql: the prepared update statement
    - args: the parameters to insert into the prepared statement.
    """
    conn = connect()
    cur = conn.cursor()
    result = cur.execute(sql, args)
    conn.commit()
    conn.close()
    return result


def insert_relation_rows(rows: list[Relation]):
    """
    Insert each row in the list of Relation rows into the database.
    """
    cols = ", ".join([col for col in rows[0].cols])
    sql = f"INSERT INTO {rows[0].name} ({cols}) VALUES "
    values = []
    param_strings = []
    for row in rows:
        param_strings.append(
            "(" + ", ".join(["%s" for _ in range(len(row.cols))]) + ")"
        )
        for val in row.getValues():
            values.append(val)
    sql += ", ".join(param_strings)
    exec_commit(sql, values)
