import os

from create import create_schema
from load import load_data

db_path = './csep_db'
schema_path = './artifacts/table_schema.txt'
# database fake data path
tables = {'ScheduledForecasts': './artifacts/testing_data/scheduled_forecasts.csv',
          'ScheduledEvaluations': './artifacts/testing_data/scheduled_evaluations.csv',
          'Dispatchers': './artifacts/testing_data/dispatchers.csv',
          'ForecastGroups': './artifacts/testing_data/forecast_groups.csv',
          'EvaluationTypes': './artifacts/testing_data/evaluation_types.csv',
          'Evaluations': './artifacts/testing_data/evaluations.csv',
          'Forecasts': './artifacts/testing_data/forecasts.csv',
          'Catalogs': './artifacts/testing_data/catalogs.csv',
          'Dispatchers_ForecastGroups': './artifacts/testing_data/dispatchers_forecastgroups.csv'}

join_tables = ['Dispatchers_ForecastGroups']

# start fresh
if os.path.isfile(db_path):
    os.remove(db_path)

# make database
create_schema(schema_path, db_path)

# load data
load_data(db_path, tables, join_tables)


