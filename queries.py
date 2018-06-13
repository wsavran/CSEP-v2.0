import sqlite3

db = sqlite3.connect('csep_db_one-day-forecasts_new_algorithm.sql3')
cur = db.cursor()

cur.execute('select distinct name from Forecasts as result;')
forecasts = cur.fetchall()
results = {}
for forecast in forecasts:
    statement = "select count(rowid) from Forecasts where status='Missing' and name=?;"
    cur.execute(statement, forecast)
    result = cur.fetchone()[0]
    results[forecast[0]] = result
    print('{}|{}'.format(forecast[0], result))
    
