import sys
import sqlite3
import csv

verbose = False


def _get_fields(cursor, table_name):
    """
    get field names from sqlite3 database
    :param cursor: cursor for sqlite3 database
    :param table_name: name of the table to query for field names
    :return: list containing names of all fields in table
             if the table is not found, returns empty list.
    """
    try:
        cursor.execute("select * from {}".format(table_name))
    except sqlite3.OperationalError:
        print("error: table {} does not exist in database".format(table_name))
        return []
    names = [f[0] for f in cursor.description]
    return names


def insert(cursor, table, fields, values):
    num = len(values)
    if num == 0:
        print("Warning: Skipping inserts, no values found.")
    try:
        if verbose:
            print("INSERT INTO {0} ({1}) VALUES ({2});".format(table, fields, values))
        cursor.execute("INSERT INTO {0} ({1}) VALUES ({2});".format(table, fields, values))
    except sqlite3.IntegrityError:
        print("Warning: Integrity Error, Unique constraint failed for table {}".format(table))
    return True


def _insert_from_csv(filename, cursor, table, fields):
    with open(filename) as f:
        reader = csv.reader(f)
        for row in reader:
            quote_row = ['"' + item + '"' for item in row[1:]]
            if not insert(cursor, table, fields, ', '.join(quote_row)):
                return False
    return True


def load_data(db_path, tables, join_tables=[]):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    for k, v in tables.items():
        print('loading data for {}'.format(k))
        # need to handle join tables differently, bc composite private key must be explicitly imported
        if k not in join_tables:
            if verbose:
                print("{} not in join tables".format(k))
            f = ', '.join(_get_fields(c, k)[1:])
            if _insert_from_csv(v, c, k, f):
                conn.commit()
        else:
            if verbose:
                print("{} in join tables.".format(k))
            f = ', '.join(_get_fields(c, k))
            if _insert_from_csv(v, c, k, f):
                conn.commit()


if __name__ == "__main__":
    db_path = './csep_db'
    # mapping from table name => data file
    tables = {'ScheduledForecasts': './testing_data/scheduled_forecasts.csv',
              'ScheduledEvaluations': './testing_data/scheduled_evaluations.csv',
              'Dispatchers': './testing_data/dispatchers.csv',
              'ForecastGroups': './testing_data/forecast_groups.csv',
              'EvaluationTypes': './testing_data/evaluation_types.csv',
              'Evaluations': './testing_data/evaluations.csv',
              'Forecasts': './testing_data/forecasts.csv',
              'Catalogs': './testing_data/catalogs.csv',
              'Dispatchers_ForecastGroups': './testing_data/dispatchers_forecastgroups.csv'}
    join_tables = ['Dispatchers_ForecastGroups']
    load_data(db_path, tables, join_tables)










