import os
import re
import sqlite3
from datetime import datetime
from ForecastGroupInitFile import ForecastGroupInitFile
from DispatcherInitFile import DispatcherInitFile

verbose = True


# parent class for database model, to ensure that same cursor is used by all models
class Model:
    def __init__(self, db=None):
        if db:
            # set attributes used for all classes
            self.table = self.__class__.__name__
            Model._cur = sqlite3.connect(db).cursor()
            self.__set_fields()

    @property
    def cur(self):
        return self._cur

    def __set_fields(self):
        """
        get field names from sqlite3 database, and set as class members
        """
        try:
            self.cur.execute("select * from {}".format(self.table))
            for f in self.cur.description:
                setattr(self, f[0], f[0])
        except sqlite3.OperationalError:
            print("Error: table {} does not exist in database".format(self.table))


# Classes below represent the fields associated with the database
class ScheduledForecasts(Model):
    def __init__(self, day, **kwargs):
        super().__init__(**kwargs)
        self.date_time = day
        # store the datetime as text object
        self.date_time = self.datetime_to_text()

    def text_to_datetime(self):
        """
        convert text in sqlite3 database to python datetime object
        :param datetime_string: string in format MM-DD-YYYY HH:MM:SS
        :return: datetime object
        """
        return datetime.strptime(self.date_time, '%Y-%m-%d %H:%M:%S')

    def datetime_to_text(self):
        """
        covert python3 datetime object to text string to store in sqlite3 database
        :param datetime_object: python3 datetime object
        :return: string containing datetime in format MM-DD-YYYY HH:MM:SS
        """
        return str(self.date_time)


class ScheduledEvaluations(Model):
    def __init__(self, day):
        self.date_time = self.datetime_to_text(day)

    def text_to_datetime(self):
        """
        convert text in sqlite3 database to python datetime object
        :param datetime_string: string in format MM-DD-YYYY HH:MM:SS
        :return: datetime object
        """
        return datetime.strptime(self.date_time, '%Y-%m-%d %H:%M:%S')

    def datetime_to_text(self):
        """
        covert python3 datetime object to text string to store in sqlite3 database
        :param datetime_object: python3 datetime object
        :return: string containing datetime in format MM-DD-YYYY HH:MM:SS
        """
        return str(self.date_time)


class Dispatchers(Model):
    def __init__(self, script_name, **kwargs):
        super().__init__(**kwargs)
        self.script_name = script_name
        self.config_file_name = None
        self.logfile = None
        self.forecast_groups = []
        self.parse_config_file()
        if self.config_file_name:
            self.parse_forecastgroup_path()

    def groups(self):
        for group in self.forecast_groups:
            yield group

    def parse_config_file(self):
        p = re.compile(r'--configFile=(\S*)')
        with open(self.script_name, 'r') as f:
            lines = f.readlines()
        para = ''.join(lines).strip()
        try:
            # should be regex capture group 1
            self.config_file_name = p.search(para).group(1)
        except AttributeError:
            print('Warning: Could not parse config file name from dispatcher script')
            self.config_file_name = None

    def parse_forecastgroup_path(self):
        if self.config_file_name:
            d = DispatcherInitFile(self.config_file_name)
            g = d.elements('forecastGroup')
            for elem in g:
                self.forecast_groups.append(elem.text)
        else:
            return None


class ForecastGroups(Model):
    # we will only store the name of the hybrid and bayesian models
    hybridModel = 'hybridModel'
    genericModel = 'models'
    bayesianModel = 'BayesianModel'

    def __init__(self, group_path, **kwargs):
        super().__init__(**kwargs)
        self.entry_date = None
        self.forecast_dir = None
        self.result_dir = None
        self.post_processing = None
        self.group_path = group_path
        self.fg = ForecastGroupInitFile(self.group_path)
        self.group_name = self.parse_group_name()
        self.config_filepath = os.path.join(self.group_path, 'forecast.init.xml')
        self.models = self.parse_models()
        self.group_dir = os.path.basename(self.group_path)
        self.evaluation_tests = self.parse_evaluation_tests()

    def parse_forecast_dir(self):
        return os.path.join(self.group_path, self.fg.elementValue('forecastDir'))

    def parse_result_dir(self):
        return os.path.join(self.group_path, self.fg.elementValue('resultDir'))

    def parse_postprocessing(self):
        return self.fg.elementValue('postProcessing')

    def parse_group_name(self):
        """
        parses and sets the group name, fails loudly
        :param group_path: path of the top level folder to the forecast group
        :return:
        """
        # name is stored as attribute on root
        root = self.fg.root()
        name = root.attrib['name']
        return name

    def parse_models(self):
        """
        returns list of models called for each forecast group, handles models defined in
        models tag of config file, along with the hybrid and bayesian models
        :return: list of models running in the forecast group or none
        """
        models = []
        try:
            generic_models = self.fg.elementValue(self.genericModel)
            if generic_models:
                models.extend(generic_models.split(' '))
            for elem in self.fg.next(self.bayesianModel):
                models.append(elem.attrib['name'])
            for elem in self.fg.next(self.hybridModel):
                models.append(elem.attrib['name'])
        except(RuntimeError, KeyError):
            print("Warning: Could not extract models from ForecastGroup {}."
                  .format(self.group_path))
        return models

    def parse_evaluation_tests(self):
        tests = self.fg.elementValue('evaluationTests')
        if tests:
            return tests.split(' ')
        return []


class EvaluationTypes(Model):
    def __init__(self):
        self.name = None
        self.evaluation_class = None


class Catalogs(Model):
    def __init__(self):
        self.data_filename = None
        self.creation_date = None
        self.post_processing = None


class Forecasts(Model):
    forecast_extension = '-fromXML.dat'
    forecast_meta_extension = '-fromXML.dat.meta'

    def __init__(self, period, start_date, name, **kwargs):
        print('In Constructor of Forecasts')
        super().__init__(**kwargs)
        self.period = period
        self.start_datetime = start_date
        self.name = name
        self.filepath = self.get_filename()
        self.meta_filepath = self.get_metafilename()

    def get_filename(self):
        self.filepath = self.name + '_' + self.start_datetime + '_' + self.forecast_extension
        return self.filepath

    def get_metafilename(self):
        self.meta_filepath = self.name + '_' + self.start_datetime + '_' + self.forecast_meta_extension
        return self.meta_filepath

    def update_name(self, name):
        self.name = name
        self.get_filename()
        self.get_metafilepath()


class Evaluations(Model):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class DispatchersForecastGroups(Model):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)




