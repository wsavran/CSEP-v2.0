import csv
import os

def _convert_text_to_datetime(datetime_string):
    """
    convert text in sqlite3 database to python datetime object
    :param datetime_string: string in format MM-DD-YYYY HH:MM:SS
    :return: datetime object
    """
    pass


def _convert_datetime_to_text(datetime_object):
    """
    covert python3 datetime object to text string to store in sqlite3 database
    :param datetime_object: python3 datetime object
    :return: string containing datetime in format MM-DD-YYYY HH:MM:SS
    """

def _add_quotes_to_shit(file):
    tfile = 'temp_file.csv'
    with open(file) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            print(", ".join(['"' + item + '"' for item in row]))


if __name__ == "__main__":
    # mapping from table name => data file
    tables = {'Models': './artifacts/testing_data/models.csv',
              'ScheduledForecasts': './artifacts/testing_data/scheduled_forecasts.csv',
              'ScheduledEvaluations': './artifacts/testing_data/scheduled_evaluations.csv',
              'Dispatchers': './artifacts/testing_data/dispatchers.csv',
              'ForecastGroups': './artifacts/testing_data/forecast_groups.csv',
              'EvaluationTypes': './artifacts/testing_data/evaluation_types.csv',
              'Evaluations': './artifacts/testing_data/evaluations.csv',
              'Forecasts': './artifacts/testing_data/forecasts.csv',
              'Catalogs': './artifacts/testing_data/catalogs.csv',
              'Dispatchers_ForecastGroups': './artifacts/testing_data/dispatchers_forecastgroups.csv'}

    for k, v in tables.items():
        _add_quotes_to_shit(v)
