import os
import sqlite3
from artifacts.create import create_schema
from models import Dispatchers

# create database
db_name = 'csep_db.sql3'
try:
    os.remove(db_name)
except FileNotFoundError:
    pass

sql_statements = './table_schema_light.txt'
db = create_schema(sql_statements, db_name)

# start with ANSS one-day catalogs
dispatcher = Dispatchers("/usr/local/csep/cronjobs/dispatcher_ANSS1985_one_day.tcsh", conn=db)
for group in dispatcher.forecast_groups():
    for forecast in group.forecasts():
        for evaluation in forecast.evaluations():
            evaluation.insert()

# commit insertions to database
db.commit()
