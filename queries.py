import sqlite3


def query(cursor, query):
    """
    performs query on database
    :param cursor: cursor to sqlite3 database
    :param query: sql statements to query
    :return:
    """
    out = []
    c = cursor.execute(query)
    out.append(c.fetchall())
    # will return list of tuples for each query
    return out


def _read_statements(filename):
    """
    reads sql statements from file, where statements are separated using a single
    newline
    :param filename: path to ascii file contaning sql statements
    :return: list of sql statements
    """
    with open(filename, 'r') as f:
        lines = f.readlines()
    # on unix, no windows compatibility not implemented
    statements = ''.join(lines).split('\n\n')
    return statements


if __name__ == "__main__":
    conn = sqlite3.connect('./csep_db')
    cur = conn.cursor()
    stmnts = _read_statements('./artifacts/sql_queries.txt')
    results = []
    for stmnt in stmnts:
        results.append(query(cur, stmnt))
    # grab only the first query
    for result in results[7]:
        for tup in result:
            print(tup)
