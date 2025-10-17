### GLOBAL CONSTANTS AND IMPORTS #########################################################

from model.relations import *

PLAY_ACT_ID = 1

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

# def parse_df_to_SQL_inserts(df, table_name):
#     """
#     convert pandas dataframe to SQL insert statements
#     may need adjustment based on the actual schema / columns we want for specific tables
#     """
#     import pandas as pd
#     insert_statements = []
#     for index, row in df.iterrows():
#         columns = ', '.join(df.columns)
#         values = ', '.join([f"'{str(value).replace("'", "''")}'" if pd.notnull(value) else 'NULL' for value in row])
#         insert_statement = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
#         insert_statements.append(insert_statement)
#     return insert_statements


def parse_event_file(file_path):
    """parsing eva and evn files"""
    ### Storage for all table rows from this event file
    games = []
    plays = []
    activity = []

    # Current game values
    current_game: Game = None
    current_inning = 1
    play_num = 1
    
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith("id"):
                current_game = Game()
                current_inning = 1
                play_num = 1
                current_game.setValue("id", parse_id_line(line))
                games.append(current_game)
            elif line.startswith("info"):
                parse_info_line(line, current_game)
            elif line.startswith("start") or line.startswith("sub"):
                act = parse_start_and_sub_line(line, (current_game.values["visteam"], current_game.values["hometeam"]), current_inning)
                act.setValue("gameId", current_game.values["id"])
                activity.append(act)
            elif line.startswith("play"):
                at_bat = parse_play_line(line, (current_game.values["visteam"], current_game.values["hometeam"]))
                current_inning = at_bat.values["inning"]
                if at_bat.values["play"] == None:
                    continue
                at_bat.setValue("num", play_num)
                play_num += 1
                at_bat.setValue("game", current_game.values["id"])
                plays.append(at_bat)

    return games, plays, activity

def parse_info_line(line: str, game: Game):
    """
    If we care about the info type, add it to the game's values. Otherwise ignore.
    """
    mapping = {
        "visteam": "visteam",
        "hometeam": "hometeam",
        "date": "date",
        "site": "location",
        "attendance": "attendance",
        "htbf": "htbf",
        "usedh": "usedh",
        "wp": "winningPitcher",
        "lp": "losingPitcher",
        "save": "sv"
    }
    parts = line.split(",")
    try:
        game.setValue(mapping[parts[1]], parts[2])
    except:
        pass
        

def parse_id_line(line: str):
    """
    Return the unique game id provided by the id lines in a file
    """
    return line.strip().split(",")[1]

def parse_start_and_sub_line(line: str, home_away: tuple, inning: int):
    """
    With the below details, add the appropriate fields to a new PlayerActivity row object and return it.

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
    parts = line.split(",")
    act = PlayerActivity()
    global PLAY_ACT_ID
    act.setValue("id", PLAY_ACT_ID)
    PLAY_ACT_ID += 1

    act.setValue("playerId", parts[1])
    act.setValue("team", home_away[int(parts[3])])
    act.setValue("inning", inning)
    act.setValue("battingPos", int(parts[4]))
    act.setValue("fieldingPos", int(parts[5]))
    act.setValue("pinchHit", int(parts[5]) == 11)
    act.setValue("pinchRun", int(parts[5]) == 12)
    return act

def parse_play_line(line: str, home_away: tuple) -> AtBat:
    """
    With the below details, create a new AtBat row object and add the appropriate fields.

    The play records contain the events of the game. Each play record has 6 fields after "play".

    1. The first field is the inning, an integer starting at 1.

    2. The second field is either 0 (for visiting team) or 1 (for home team).

    3. The third field is the Retrosheet player id of the player at the plate.

    4. The fourth field is the count on the batter when this particular event (play) occurred. 
        Most games prior to 1988 do not have this information, and in such cases, "??" appears in this field.

    5. The fifth field is of variable length and contains all pitches to this batter in this plate appearance and is described below. 
        If pitches are unknown, this field is left empty with nothing in between the commas.

    6. The sixth field describes the play or event that occurred.
    """
    atBat = AtBat()
    parts = line.split(",")

    atBat.setValue("inning", int(parts[1]))
    if parts[6] == "NP":
        return atBat
    atBat.setValue("team", home_away[int(parts[2])])
    atBat.setValue("top_bottom", ["T", "B"][int(parts[2])])
    atBat.setValue("batter", parts[3])
    atBat.setValue("pitches", parts[5])
    play = parts[6].split("/")
    atBat.setValue("play", play[0])
    baserun_split = play[-1].split(".")
    if len(baserun_split) > 1:
        atBat.setValue("baserunnerDetails", baserun_split[1])
    play[-1] = baserun_split[0]
    atBat.setValue("playDetails", "/".join(play[1:]))
    return atBat

#### -------- #### ---------------------- example usage below ----------------- #### -------- ####
def example():
    """
    example function use and instruction sequence to run this script:
    1. python (or python3) -m venv venv.                                # to create a virtual environment
    2. source venv/bin/activate                                         # to activate the virtual environment
    3. pip install -r requirements.txt                                  # to install required packages
    4. python parsing.py                                                # to run this script
    """

