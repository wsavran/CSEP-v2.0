import os

from artifacts.create import create_schema
from artifacts.load import load_data

db_path = './csep_db'
schema_path = './table_schema.txt'
# database fake data path
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

# start fresh
if os.path.isfile(db_path):
    os.remove(db_path)

# make database
create_schema(schema_path, db_path)

# load data
load_data(db_path, tables, join_tables)


