"""
Main program file for running db loading code.
Data files should be placed in ../data from the phase1 directory
"""

import db.db as baseball_db
from parsing.parsing import (
    parse_event_file,
    parse_csv_file_to_pandas_df as parse_csv,
    parse_ballpark_df_to_SQL_inserts as df_to_ballparks,
    parse_team_df_to_SQL_inserts as df_to_teams,
    parse_player_df_to_SQL_inserts as df_to_players,
)
from recursive_directory_read import recursiverly_read_directory as rec_read


def main():
    # Set up schema
    baseball_db.exec_file("db/schema.sql")

    # Load static Ballpark, Player, and Team data
    ballpark_df = parse_csv("../data/ballparks.csv")
    ballparks = df_to_ballparks(ballpark_df)

    player_df = parse_csv("../data/biofile/biofile.csv")
    players = df_to_players(player_df)

    team_df = parse_csv("../data/teams.csv")
    teams = df_to_teams(team_df)

    baseball_db.insert_relation_rows(ballparks)
    baseball_db.insert_relation_rows(players)
    baseball_db.insert_relation_rows(teams)

    # Find event files, parse each, and insert the resulting rows into the database.
    data_dir = "../data/eventFiles"
    all_files = rec_read(data_dir)
    for filepath in all_files:
        all_relation_rows = parse_event_file(filepath)
        for tups in all_relation_rows:
            baseball_db.insert_relation_rows(tups)


if __name__ == "__main__":
    main()
