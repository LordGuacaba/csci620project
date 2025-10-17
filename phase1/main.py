"""
Main program file for running db loading code.
Data files should be placed in ../data from the phase1 directory
"""

from db.db import *

def main():
    exec_file("db/schema.sql")

if __name__ == "__main__":
    main()