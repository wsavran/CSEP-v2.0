import sys
import sqlite3


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
    db = sqlite3.connect(db_filename)

    # use 'with' to cleanly close file
    with open(filename, 'r') as f:
        lines = f.readlines()

    # on unix, no windows compatibility not implemented
    statements = ''.join(lines).split('\n\n')

    for statement in statements:
        db.execute(statement)
    db.commit()

    return db

