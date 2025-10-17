"""
Provides a class that connects to the postgres database and provides methods for updates and queries.
"""

import psycopg2

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