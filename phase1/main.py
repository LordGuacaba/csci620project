"""
Main program file for running db loading code.
Data files should be placed in ../data from the phase1 directory
"""

import db.db as baseball_db
from parsing.parsing import parse_event_file
from recursive_directory_read import recursiverly_read_directory as rec_read

def main():
    # Set up schema
    baseball_db.exec_file("db/schema.sql")

    data_dir = "../data/eventFiles"
    all_files = rec_read(data_dir)
    for filepath in all_files:
        all_relation_rows = parse_event_file(filepath)
        for tups in all_relation_rows:
            baseball_db.insert_relation_rows(tups)


if __name__ == "__main__":
    main()