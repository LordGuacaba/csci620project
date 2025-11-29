"""
Postgres database config file - provides a connect method that opens a db connection with specific credentials.
"""

import psycopg2

# DB connection settings - these should be changed as needed for your local machine.
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "baseball_db"
DB_USER = "postgres"
DB_PASSWORD = "$nax459:)" 

def connect():
    """
    Open a connection to the postgres database using the provided credentials.
    """
    DB = dict(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
    return psycopg2.connect(**DB)