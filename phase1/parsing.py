### GLOBAL CONSTANTS AND IMPORTS

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "mydb"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_SCHEMA = "baseball_db"


# going to use this table map to define expected columns for different tables
# and expected files to parse from data directory
# this is a simplified example, real schema will be more complex
TABLE_MAP = {
    "Stadiums": {"columns": ["id", "name", "city", "state"], "file": "../data/ballparks.csv"},
    "Teams": {"columns": ["id", "name", "city", "first", "last"], "file": "../data/teams.csv"},
    "Players": {"columns": ["id", "firstName", "lastName", "battingHandedness", "position", "DOB", "throwingHandedness"], "file": "../data/biofile/biofile.csv"},
    # add more tables as needed. files can be added later. It is likely
    # that we will get mutliple tables from the same file, 
    # in which case we can map that to a different map and update this to a more
    # standalone entity map (i.e no FKs)

    #"AtBats":{"columns":["number", "game", "batter", "inning", "top", "pitches", "play", "playDetails", "baserunnerDetails"], "file": ""},
    #"Games":{"columns":["id", "homeTeam", "visTeam", "date", "location", "useDH", "htbf", "attendance", "wp", "lp", "save"], "file": ""},
    #"PlayerActivity":{"columns":["gameId", "playerId", "team", "battingPosition", "fieldingPosition", "inning", "pinchHit", "pinchRun"], "file": ""},

}


### Helper functions for parse CSV files to pandas dataframe and convert to SQL insert statements

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





def connect_to_db():
    """
    Connect to Postgres using settings in DB dict. example usage: conn = connect_to_db()
    add params for host, port, dbname, user, password as needed. find defaults to fill in.
    """
    import psycopg2

    DB = dict(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    SCHEMA = "baseball_db"
    return psycopg2.connect(**DB)








#### -------- #### ---------------------- example usage below ----------------- #### -------- ####
def main():
    """
    example function use and instruction sequence to run this script:
    1. python (or python3) -m venv venv.                                # to create a virtual environment
    2. source venv/bin/activate                                         # to activate the virtual environment
    3. pip install -r requirements.txt                                  # to install required packages
    4. python parsing.py                                                # to run this script
    """
    conn = connect_to_db()                                              # connect to the database
    cursor = conn.cursor()                                              # create a cursor object to execute SQL commands

    for table_name, info in TABLE_MAP.items():                          # for loop to parse each file and insert into corresponding table
        file_path = info["file"]
        df = parse_csv_file_to_pandas_df(file_path)
        sql_inserts = parse_df_to_SQL_inserts(df, table_name) 
        for insert in sql_inserts:
            # this is printing for now, but should be executed in real use
            print(insert)
            # cursor.execute(insert)

if __name__ == "__main__":
    main()
