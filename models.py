import os
import re
import sqlite3
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from ForecastGroupInitFile import ForecastGroupInitFile
from DispatcherInitFile import DispatcherInitFile
from utils import text_to_datetime

verbose = False


# parent class for database model, to ensure that same cursor is used by all models
class Model:
    def __init__(self, db=None):
        self.db = db
        self._cur = None
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
                setattr(self, "_"+f[0], f[0])
        except sqlite3.OperationalError:
            print("Error: table {} does not exist in database".format(self.table))


class Schedule(Model):

    end_date = datetime(2019, 1, 1, 0, 0, 0)
    type = 'GenericScheduled'

    def __init__(self, start_date=None, **kwargs):
        super().__init__(**kwargs)
        self.date_time_text = None
        self.start_date = start_date
        if self.start_date:
            self.date_time_text = self.datetime_to_text()

    def text_to_datetime(self):
        """
        convert text in sqlite3 database to python datetime object
        :param datetime_string: string in format MM-DD-YYYY HH:MM:SS
        :return: datetime object
        """
        return datetime.strptime(self.start_date_text, '%Y-%m-%d %H:%M:%S')

    def datetime_to_text(self):
        """
        covert python3 datetime object to text string to store in sqlite3 database
        :param datetime_object: python3 datetime object
        :return: string containing datetime in format MM-DD-YYYY HH:MM:SS
        """
        return str(self.start_date)

    def date_range(self, days=1, months=0, years=0):
        date = self.start_date
        if date:
            while date < self.end_date:
                yield date
                date += relativedelta(days=days, months=months, years=years)
        else:
            return iter([])


class ScheduledForecasts(Schedule):
    type = 'Forecast'


class ScheduledEvaluations(Schedule):
    type = 'Evaluation'


class Dispatchers(Model):
    def __init__(self, script_name, **kwargs):
        super().__init__(**kwargs)
        self.script_name = script_name
        self.config_file_name = None
        self.logfile = None
        self.forecast_group_paths = []
        self.parse_config_file()
        if self.config_file_name:
            self.parse_forecastgroup_path()

    def forecast_groups(self):
        for group in self.group_paths():
            yield ForecastGroups(group_path=group)

    def group_paths(self):
        for group in self.forecast_group_paths:
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
                self.forecast_group_paths.append(elem.text)
        else:
            return None


class ForecastGroups(Model):
    # we will only store the name of the hybrid and bayesian models
    hybridModel = 'hybridModel'
    genericModel = 'models'
    bayesianModel = 'BayesianModel'
    end_date = Schedule.end_date

    def __init__(self, group_path=None, **kwargs):
        super().__init__(**kwargs)
        self.entry_date = None
        self.result_dir = None
        self.post_processing = None
        self.group_path = None
        self.fg = None
        self.group_name = None
        self.config_filepath = None
        self.group_dir = None
        self.forecast_dir = None
        self.result_dir = None
        self.post_processing = None
        self.entry_date = None
        self.models = []
        if group_path:
            self.group_path = group_path
            self.fg = ForecastGroupInitFile(self.group_path)
            self.group_name = self.parse_group_name()
            self.config_filepath = os.path.join(self.group_path, 'forecast.init.xml')
            self.models = self.parse_models()
            self.evaluation_schedule = self.parse_schedule('evaluationTests')
            self.forecast_schedule = self.parse_schedule('models')
            self.group_dir = os.path.basename(self.group_path)
            self.evaluation_tests = self.parse_evaluation_tests()
            self.forecast_dir = self.parse_forecast_dir()
            self.result_dir = self.parse_result_dir()
            self.post_processing = self.parse_postprocessing()
            self.entry_date_text = self.parse_entry_date_text()
            if self.entry_date_text:
                self.entry_date = datetime.strptime(self.entry_date_text, '%Y-%m-%d %H:%M:%S')

    def parse_entry_date_text(self):
        return self.fg.elementValue('entryDate')

    def schedule(self):
        s = Schedule(start_date=self.entry_date)
        if self.entry_date:
            for date in s.date_range():
                yield date
        else:
            return iter([])

    def forecasts(self):
        for model in self.models:
            yield Forecasts(name=model, )

    def evaluations(self):
        for elem in self.fg.next('evaluationTests'):
            # get directory
            tests = self.parse_evaluation_tests(elem)
            for date in self.schedule():
                for test in tests:
                    for model in self.models:
                        yield Evaluations(name=test, date=date, archive_dir=self.result_dir, forecast_name=model)

    def parse_forecast_dir(self):
        fcdir_path = self.fg.elementValue('forecastDir')
        if not fcdir_path:
            return None
        if os.path.isabs(fcdir_path):
            return fcdir_path
        return os.path.join(self.group_path, fcdir_path)

    def parse_result_dir(self):
        resdir_path = self.fg.elementValue('resultDir')
        if not resdir_path:
            return None
        if os.path.isabs(resdir_path):
            return resdir_path
        return os.path.join(self.group_path, resdir_path)

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

    def parse_evaluation_tests(self, xml_elem=None):
        """
        either parses entire configuration file, or tests from single element.
        xml_elem would be used to get schedules for different evaluations
        :param xml_elem: xml_elem obj to parse tests from
        :return: returns list of xml elements
        """
        # parse all tests
        if not xml_elem:
            tests = []
            try:
                for elem in self.fg.next('evaluationTests'):
                    if elem.text:
                        tests.extend(elem.text.strip().split(' '))
            except AttributeError:
                # print warning that no evaluation tests were found.
                print("Warning: No evaluation tests found for ForecastGroup {}."
                      .format(self.group_path))
            return tests
        # only parse for particular element
        else:
            if xml_elem.text:
                return xml_elem.text.strip().split(' ')
            else:
                return []

    def parse_schedule(self, xml_tag):
        """
        parses CSEPSchedule from forecast group config file
        :param xml_tag: string containing the xml tag to parse schedule from; ('models','evaluationTests')
        :return: CSEPSchedule object
        """
        schedule = []
        try:
            for elem in self.fg.next(xml_tag):
                schedule.append(self.fg.schedule(elem))
        except AttributeError:
            return []
        return schedule

    @staticmethod
    def as_datetime(date_string):
        """
        return parsed string date as datetime
        :param date_string: string representing the datetime
        :return: datetime obj
        """
        return text_to_datetime(date_string)


class Catalogs(Model):
    def __init__(self):
        self.data_filename = None
        self.creation_date = None
        self.post_processing = None


class Forecasts(Model):
    forecast_extension = '-fromXML.dat'
    forecast_meta_extension = '-fromXML.dat.meta'

    def __init__(self, name=None, period=None, **kwargs):
        print('In Constructor of Forecasts')
        super().__init__(**kwargs)
        self.archive_dir = None
        self.period = period
        self.start_datetime = start_date
        self.name = name
        self.relative_filepath = self.get_filename()
        self.relative_meta_filepath = self.get_metafilename()

    def get_filename(self):
        self.relative_filepath = self.name + '_' + self.start_datetime + '_' + self.forecast_extension
        return self.filepath

    def get_metafilename(self):
        self.relative_meta_filepath = self.name + '_' + self.start_datetime + '_' + self.forecast_meta_extension
        return self.meta_filepath

    def update_name(self, name):
        self.name = name
        self.get_filename()
        self.get_metafilepath()


class Evaluations(Model):
    def __init__(self, archive_dir=None, date=None, name=None, forecast_name=None, **kwargs):
        super().__init__(**kwargs)
        self.forecast_group_archive_dir = archive_dir
        self.daily_archive_dir = None
        self.date = date  # should be datetime object
        self.name = name
        self.forecast_name = forecast_name
        self.full_filepath = None
        self._list_of_result_files = []
        if self.date and self.forecast_group_archive_dir:
            self.daily_archive_dir = os.path.join(
                self.forecast_group_archive_dir,
                date.strftime("%Y-%m-%d")
            )
            if not self._list_of_result_files:
                self._list_of_result_files = os.listdir(self.daily_archive_dir)
            if self.name and self.daily_archive_dir:
                self.full_filepath = self.determine_full_filepath(regex=self.capture_regex())

    def capture_regex(self):
        year = self.date.strftime("%Y")
        month = self.date.strftime("%-m")
        day = self.date.strftime("%-d")
        p = re.compile(r"^scec\.csep\S*_{}-Test_{}_{}_{}_{}-fromXML.xml.\S*.\d$"
                       .format(self.name, self.forecast_name, month, day, year))
        print(p)
        return p

    def determine_full_filepath(self, regex=None):
        # not sure what MapReady means, but they do not
        paths = self._list_of_result_files
        print(paths)
        matches = list(filter(regex.match, paths))
        return matches


class DispatchersForecastGroups(Model):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)




