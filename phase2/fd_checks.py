import pandas as pd
import psycopg2

DB_PARAMS = dict(
    host="localhost",
    dbname="baseball_db",
    user="postgres",
    password="$nax459:)",
)


def run_sql_file(conn, filename):
    """
    Execute SQL statements from a file and print the results.
    """
    with open(filename, "r") as f:
        sql_text = f.read()

    # split statements on semicolon, but ignore trailing empty statements so it runs all
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]

    cur = conn.cursor()

    for stmt in statements:
        print("SQL Query:")
        print(stmt)

        try:
            cur.execute(stmt)

            if cur.description:
                rows = cur.fetchall()
                df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
                print(df)
            else:
                print("no result set")

        except Exception as e:
            print(e)

    cur.close()


def main():
    conn = psycopg2.connect(**DB_PARAMS)

    run_sql_file(conn, "./fd_queries.sql")

    conn.close()


if __name__ == "__main__":
    main()
