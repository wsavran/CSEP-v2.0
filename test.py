import os
import sqlite3
from artifacts.create import create_schema
from models import Dispatchers

# create database
db_name = 'csep_db_one-day-eval_test_uniqueness_v2.sql3'

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

#dispatchers = ['/usr/local/csep/cronjobs/dispatcher_ANSS1985_one_day.tcsh']

groups = []
for dispatcher in dispatchers:
    dispatcher = Dispatchers(dispatcher)
    for group in dispatcher.forecast_groups():
        if 'ETAS' in group.models:
            groups.append((group.group_name, group.result_dir))

for name, result_dir in groups:
    if result_dir:
        print("{} | {}".format(name, result_dir))
