import os, sys
import sqlite3

# script variables
db_name = 'csep_db'
debug = True


def create_schema(filename=None, db_filename=None):
    """
    creates sqlite3 database from text file containing SQL CREATE statements
    :param filename: path to text file containing SQL statements
    :param db_filename: path to the sqlite3 database
    :return: none
    """

    if filename is None:
        print("required: path to filename containing SQL CREATE TABLE statements, separated by a single newline")
        sys.exit(-1)

    if db_filename is None:
        print("required: path to filename of sqlite3 database")
        sys.exit(-1)

    # creates new db if it does not exist, and calls cursor object
    db = sqlite3.connect(db_name)

    try:
        # use 'with' to cleanly close file
        with open(filename, 'r') as f:
            lines = f.readlines()
    except IOError:
        print("error: could not read file.")
        if debug:
            raise
        sys.exit(-1)

    # on unix, no windows compatibility not implemented
    statements = ''.join(lines).split('\n\n')

    try:
        for statement in statements:
            db.execute(statement)
    # general error for sqlite3, subclasses Exception
    except sqlite3.Error:
        print("error: could not execute sqlite statements.")
        if debug:
            raise
        sys.exit(-1)

    return


if __name__ == "__main__":
    # make database
    create_schema("./table_schema.txt", db_name)









