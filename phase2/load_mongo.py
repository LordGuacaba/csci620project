"""
Loads the mongodb with the document-based model for our data.
"""

import db.config as baseball_db
from parsing.parsing import (
    parse_event_file,
    parse_csv_file_to_pandas_df as parse_csv,
    parse_ballpark_df_to_documents as df_to_ballparks,
    parse_team_df_to_documents as df_to_teams,
    parse_player_df_to_documents as df_to_players,
)
from recursive_directory_read import recursiverly_read_directory as rec_read


def main():
    # Set up mongodb
    client, db = baseball_db.connect_to_mongo()

    # Create collections
    current_collections = db.list_collection_names()
    if "Ballparks" not in current_collections:
        db.create_collection("Ballparks")
    if "Teams" not in current_collections:
        db.create_collection("Teams")
    if "Players" not in current_collections:
        db.create_collection("Players")
    if "Games" not in current_collections:
        db.create_collection("Games")

    # Load static Ballpark, Player, and Team data
    ballpark_df = parse_csv("../data/ballparks.csv")
    ballparks = df_to_ballparks(ballpark_df)
    db["Ballparks"].insert_many(ballparks)

    player_df = parse_csv("../data/biofile/biofile.csv")
    players = df_to_players(player_df)
    db["Players"].insert_many(players)

    team_df = parse_csv("../data/teams.csv")
    teams = df_to_teams(team_df)
    db["Teams"].insert_many(teams)

    print("Static data loaded successfully.")

    client.close()