"""
Provides postgres db helper methods.
"""

from db.config import connect_to_postgres as connect


def discorver_fds():
    """
    Discover functional dependencies in the relational database.
    Returns a list of functional dependencies found.
    """
    # Placeholder implementation
    fds = []
    return fds


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

def exec_query(sql, args={}):
    conn = connect()
    cur = conn.cursor()
    cur.execute(sql, args)
    tuples = cur.fetchall()
    conn.close()
    return tuples
