### GLOBAL CONSTANTS AND IMPORTS #########################################################

# DB connection settings
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

    # "AtBats":{"columns":["number", "game", "batter", "inning", "top", "pitches", "play", "playDetails", "baserunnerDetails"], "file": ""},
    # "Games":{"columns":["id", "homeTeam", "visTeam", "date", "location", "useDH", "htbf", "attendance", "wp", "lp", "save"], "file": ""},
    # "PlayerActivity":{"columns":["gameId", "playerId", "team", "battingPosition", "fieldingPosition", "inning", "pinchHit", "pinchRun"], "file": ""},

}

#################################################################################################
### Helper functions for parse CSV files to pandas dataframe and convert to SQL insert statements
#################################################################################################

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


def parse_info_line(line):
    """
    this is extremely variable, so we will just return the line for now
    """
    pass

def parse_id_line(line):
    """
    first 3 letters id home team
    next 4 are the year
    next 2 are month
    next 2 are day
    last number is single game(0), first game(1), or second game(2)
    """
    pass
def parse_start_and_sub_line(line):
    """
    1. The first field is the Retrosheet player id, which is unique for each player.
    
    2. The second field is the player's name.

    3. The next field is either 0 (for visiting team), or 1 (for home team). 
        In some games, typically due to scheduling conflicts, the home team (the team whose stadium the game is played in) 
        bats first (in the top of the innings) and the visiting team bats second (in the bottom of the innings). 
        In these games, contrary to "normal" games, start records for the home team ("1") precede start records for the visiting team ("0"). 
        Similarly, the play codes pertaining to the home team ("1") precede the play codes pertaining to the visiting team ("0").

    4. The next field is the position in the batting order, 1 - 9. 
        When a game is played using the DH rule the pitcher is given the batting order position 0.

    5. The last field is the fielding position. 
        The numbers are in the standard notation, with designated hitters being identified as position 10. 
        On sub records 11 indicates a pinch hitter and 12 is used for a pinch runner. 
        When a player pinch hits or pinch runs for the DH, that player automatically becomes the DH, 
        so no 'sub' record is included to identify the new DH.
    """
    pass

def parse_play_line(line):
    """
    play The play records contain the events of the game. Each play record has 6 fields after "play".

    1. The first field is the inning, an integer starting at 1.

    2. The second field is either 0 (for visiting team) or 1 (for home team).

    3. The third field is the Retrosheet player id of the player at the plate.

    4. The fourth field is the count on the batter when this particular event (play) occurred. 
        Most games prior to 1988 do not have this information, and in such cases, "??" appears in this field.

    5. The fifth field is of variable length and contains all pitches to this batter in this plate appearance and is described below. 
        If pitches are unknown, this field is left empty with nothing in between the commas.

    6. The sixth field describes the play or event that occurred.
    """
    pass
def parse_com_line(line):
    """
    The final record type is used primarily to add explanatory information for a play. 
    Although it may occur anywhere in a file, it is usually not present until after the start records. 
    The second field of the com record is quoted.

        com,"ML debut for Behenna"
    """
    pass
def parse_data_line(line):
    """
    Data records appear after all records from the game. 
    At present, the only data type, that is defined specifies the number of earned runs allowed by a pitcher. 
    Each such record contains the pitcher's Retrosheet player id and the number of earned runs he allowed. 
    There is a data record for each pitcher that appeared in the game.

        data,er,showe001,2
    """
    pass

def parse_badj_line(line):
    """
    batter adjustment
    This record is used to mark a plate appearance in which the batter bats from the side that is not expected. The syntax is:
        badj,batter id,hand
    therefore as an example:

        badj,bonib001,R
    """
    pass
def parse_padj_line(line):
    """
    pitcher adjustment
        padj,pitcher id,hand
    The expectation is defined by the roster file and an example is:

        padj,harrg001,L
    """
    pass
def parse_ladj_line(line):
    """
    "Lineup adjustment". 
    This record is used when a team bats out of order. The syntax is:

        ladj,batting team,batting order position

    therefore as an example:

        ladj,0,4
    """
    pass
def parse_radj_line(line):
    """
    "Runner adjustment". 
    This record is used in games beginning in 2020 in which an extra inning begins with a runner on 2nd. The syntax is:

        radj,runner id,base

    therefore as an example:

        radj,turnj001,2
    """
    pass
def parse_presadj_line(line):
    """
    "Pitcher responsibility adjustment"
        presadj,pitcher id,base occupied by relevant runner

    therefore as an example:

        presadj,cicoe001,2

    """
    pass

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
