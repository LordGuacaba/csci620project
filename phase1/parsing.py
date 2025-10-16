def parse_csv_file_to_pandas_df(file_path):
    """
    assume first line is the header, including column names seperated by comma
    1. parse first line to get column names
    2. parse the rest of the file to get data rows
    return list of rows
    """
    import pandas as pd

    df = pd.read_csv(file_path)
    return df

def parse_df_to_SQL_inserts(df, table_name):
    """
    convert pandas dataframe to SQL insert statements
    may need adjustment based on the actual schema / columns we want for specific tables
    """
    import pandas as pd
    insert_statements = []
    for index, row in df.iterrows():
        columns = ', '.join(df.columns)
        values = ', '.join([f"'{str(value).replace("'", "''")}'" if pd.notnull(value) else 'NULL' for value in row])
        insert_statement = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
        insert_statements.append(insert_statement)
    return insert_statements


# going to use this table map to define expected columns for different tables
# and expected files to parse from data directory
# this is a simplified example, real schema will be more complex
TABLE_MAP = {
    "STADIUMS": ["id", "name", "city", "state", "country"],
    "TEAMS": ["id", "name", "city", "state", "country"],
    "PLAYERS": ["id", "name", "team_id", "position", "birthdate"]}


def connect_to_db():
    """
    Connect to Postgres using settings in DB dict. example usage: conn = connect_to_db()
    add params for host, port, dbname, user, password as needed. find defaults to fill in.
    """
    import psycopg2

    DB = dict(
        host="localhost", port=5432, dbname="mydb", user="postgres", password="postgres"
    )
    SCHEMA = "baseball_db"
    return psycopg2.connect(**DB)


def main():
    """
    example function use and instruction sequence to run this script:
    1. python (or python3) -m venv venv.    # to create a virtual environment
    2. source venv/bin/activate             # to activate the virtual environment
    3. pip install -r requirements.txt      # to install required packages
    4. python parsing.py                    # to run this script
    """
    conn = connect_to_db()
    file_path = "../data/ballparks.csv"
    df = parse_csv_file_to_pandas_df(file_path)
    print(df)
    sql_inserts = parse_df_to_SQL_inserts(df, "ballparks") 
    for insert in sql_inserts:
        print(insert)



if __name__ == "__main__":
    main()
