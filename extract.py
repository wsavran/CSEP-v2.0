import os
import sqlite3
from artifacts.create import create_schema
from models import Dispatchers

# create database
db_name = 'csep_db_one-day-forecasts_new_algorithm.sql3'

try:
    os.remove(db_name)
except FileNotFoundError:
    pass

sql_statements = 'db_schema.sql'
db = create_schema(sql_statements, db_name)

# start with ANSS one-day catalogs
dispatchers = ['/usr/local/csep/cronjobs/dispatcher_ANSS1985_one_day.tcsh',
               '/usr/local/csep/cronjobs/dispatcher_ANSS1985_M2_95.tcsh',
               '/usr/local/csep/cronjobs/dispatcher_ANSS1932_notFiltered_Md2_one_day.tcsh',
               '/usr/local/csep/cronjobs/dispatcher_ANSS1985_forecasts.tcsh'
               ]
for dispatcher in dispatchers:
    dispatcher = Dispatchers(dispatcher, conn=db)
    for group in dispatcher.forecast_groups():
        for forecast in group.forecasts():
            for evaluation in forecast.evaluations():
                evaluation.insert()

db.commit()
