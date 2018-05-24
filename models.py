import os
import re
import sqlite3
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from ForecastGroupInitFile import ForecastGroupInitFile
from DispatcherInitFile import DispatcherInitFile
from utils import text_to_datetime

"""
TODO: 1) re-evaluate data model and eliminate any wasted or unnecessary fields
      2) while doing this, make sure that we aren't missing anything we might want to know from CSEP1.
      3) implement missing pieces in data model. remember. field names should be text or model class
      4) write script to use the api to populate database with csep-debug data, for all forecasts and 
         evaluations for one-day-models class
"""


class Model:

    _table_type = "standard"

    def __init__(self, conn=None):
        self._insert_id = None
        self._conn = conn
        self.table = None
        self.fields = []
        self._insert_values = {}
        self._inserted = False

        if self._conn:
            self.table = self.__class__.__name__
            self.fields = self._fields()

    def _prepare_insert_values(self, field, value):
        if isinstance(value, Model) and not self._inserted:
            # recursive call, will stop when no models have more dependencies
            value.insert()
            last_insert_id = value.insert_id
            # cast inserts to string type. ok for basic types in database.
            # could cause problems if api is used incorrectly.
            self._insert_values[field] = '"' + str(last_insert_id) + '"'
        else:
            self._insert_values[field] = '"' + str(value) + '"'
        return

    def insert(self):
        """
        insert model into the database.
        requirements: each db field must be defined as an attribute of the class itself or an instance of Model, in which
        case that Model will be inserted and the "insert_id" of that insert will be used as the insert value.
        this will be recursively called for each model that needs to be inserted
        :return: (bool) True if successful; False if not successful
        """
        if not self.conn and not self.fields and not self.table:
            raise RuntimeError("Cannot insert values into db unless connection object is bound to Model instance.")

        # get fields and values from class
        fields_values = list(self._db_values())
        num = len(fields_values)
        if num == 0 or not fields_values:
            print("Warning: Skipping inserts, no values found.")
            return False
        try:
            # check if any of the values are model objects
            for field, value in fields_values:
                self._prepare_insert_values(field, value)

            fields, values = zip(*self._insert_values.items())
            fields = ', '.join(fields)
            values = ', '.join(values)

            cursor = self.conn.cursor()
            cursor.execute("INSERT INTO {0} ({1}) VALUES ({2})".format(self.table, fields, values))

            # update insert id for fk purposes
            self._insert_id = cursor.lastrowid
            self._inserted = True

        except sqlite3.IntegrityError:
            print("Warning: Integrity Error, Unique constraint failed for table {}".format(self.table))
            return False

        return True

    def save(self):
        """
        no error checking here.
        :return:
        """
        if self._inserted:
            self.conn.commit()
        else:
            raise RuntimeError("Error: cannot save the same database model twice.")

    @property
    def insert_id(self):
        """
        read-only instance of insert_id used for foreign key inserts
        :return:
        """
        return self._insert_id

    @property
    def conn(self):
        return self._conn

    @conn.setter
    def conn(self, val):
        """
        setter for db connection object. sets db information when connection object is assigned.
        :param val: connection object.
        :return:
        """
        if isinstance(sqlite3.Connection, val):
            self._conn = val
            self.fields = self._fields()
            self.table = self.__class__.__name__
        else:
            raise TypeError("conn must be of type sqlite3.Connection")

    def _fields(self):
        """
        return list of field names from sqlite3 database
        """
        if not self.conn:
            raise RuntimeError("Db connection must be bound to Model instance to retrieve fields.")

        cursor = self.conn.cursor()
        cursor.execute("select * from {}".format(self.table))
        fields = [f[0] for f in cursor.description]

        if self._table_type == 'join':
            return fields
        # ignore private key, unless join table
        return fields[1:]

    def _db_values(self):
        """
        get values of fields from model classes. classes are required to have fields names defined exactly the same
        also, values are assumed to be of the correct type to put into sql database (or can be defined as text)
        :return:
        """
        if not self.conn:
            raise RuntimeError("connection object must be defined to have access to db fields.")

        if not self.fields:
            raise RuntimeError("fields not defined. connection object must be defined to have access to db fields.")

        # generate values
        try:
            for field in self.fields:
                value = getattr(self, field)
                yield field, value
        except AttributeError:
            print("Error: Unable to retrieve attributes. Ensure that Model has attributes corresponding to DB fields.")


class Schedule:

    end_date = datetime(2019, 1, 1, 0, 0, 0)

    def __init__(self, start_date=None, **kwargs):
        super().__init__(**kwargs)
        self.date_time = None
        self.status = None
        self.start_date = start_date  # should be datetime object
        if self.start_date:
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
        return str(self.start_date)

    def date_range(self, days=1, months=0, years=0):
        date = self.start_date
        if date:
            while date < self.end_date:
                yield date
                date += relativedelta(days=days, months=months, years=years)
        else:
            return iter([])


class ScheduledForecasts(Model, Schedule):
    pass


class ScheduledEvaluations(Model, Schedule):
    pass


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
            yield ForecastGroups(group_path=group, conn=self.conn)

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
    # store the name of the hybrid and bayesian models
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
        """
        reads the entry date tag from the forecast group init file
        :return: contents of the entry-date tag
        """
        return self.fg.elementValue('entryDate')

    def schedule(self):
        """
        generator function to create dates used to expect forecasts and evaluations
        :return:
        """
        s = Schedule(start_date=self.entry_date, conn=self.conn)
        if self.entry_date:
            for date in s.date_range():
                yield date
        else:
            return iter([])

    def forecasts(self):
        """
        generator function to return forecasts associated with a particular forecast group
        :return:
        """
        for model in self.models:
            for date in self.schedule():
                yield Forecasts(name=model, archive_dir=self.forecast_dir, start_date=date, forecast_group=self,
                                conn=self.conn)

    def evaluations(self):
        """
        generator function to produce evaluations associated with a forecast group. note: evaluations made from the
        forecast group level do not have any association to a particular forecast. if wanting to populate a db model
        use the generator in class Forecasts(...)
        :return:
        """
        if not self.evaluation_tests:
            return iter([])
        for date in self.schedule():
            for test in self.evaluation_tests:
                for model in self.models:
                    yield Evaluations(name=test, date=date, archive_dir=self.result_dir, forecast_name=model,
                                      conn=self.conn)

    def parse_forecast_dir(self):
        """
        reads location of forecasts stored in forecast group init file
        :return: full path of directory where forecast archive is located
        """
        fcdir_path = self.fg.elementValue('forecastDir')
        if not fcdir_path:
            return None
        if os.path.isabs(fcdir_path):
            return fcdir_path
        return os.path.join(self.group_path, fcdir_path)

    def parse_result_dir(self):
        """
        reads locations of results from forecast group init file
        :return: full path of directory where evaluations are stored
        """
        resdir_path = self.fg.elementValue('resultDir')
        if not resdir_path:
            return None
        if os.path.isabs(resdir_path):
            return resdir_path
        return os.path.join(self.group_path, resdir_path)

    def parse_postprocessing(self):
        """
        reads postprocessing
        :return:
        """
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
    forecast_extension = '.xml'
    forecast_meta_extension = 'xml.meta'

    def __init__(self, name=None, period=None, archive_dir=None, start_date=None, forecast_group=None, **kwargs):
        super().__init__(**kwargs)
        self.waiting_period = None  # TODO: Not implemented
        self.runtime_testdate = None  # TODO: Not implemented
        self.catalog = None  # TODO: Not implemented
        self.dispatcher = None  # TODO: Not implemented
        self.model = None  # Starting this attribute as NULL. Might consider removing from db model in future.
        self.forecast_group = forecast_group
        self.archive_dir = archive_dir
        self.period = period
        self.start_datetime = start_date
        self.name = name
        self.filepath = self.get_filename()
        self.meta_filepath = self.get_metafilename()

    def get_filename(self):
        """
        filename for a forecast file
        template -- <model_name>_<month>_<day>_<year>.xml
        :return: filename if found, None if not found
        """
        relative_filepath = self.name + '_' + self.start_datetime.strftime("%-m_%-d_%Y") + self.forecast_extension
        archive_subdir = self.start_datetime.strftime("%Y_%-m")
        abs_path = os.path.join(self.archive_dir, 'archive', archive_subdir, relative_filepath)
        if os.path.isfile(abs_path):
            return abs_path
        return None

    def get_metafilename(self):
        """
        filename for a forecast metadata file
        template -- <model_name>_<month>_<day>_<year>.xml
        :return: filename if found, None if not found
        """
        relative_filepath = self.name + '_' + self.start_datetime.strftime("%-m_%-d_%Y") + self.forecast_meta_extension
        abs_path = os.path.join(self.archive_dir, 'archive', self.start_datetime.strftime("%Y_%-m"), relative_filepath)
        if os.path.isfile(abs_path):
            return abs_path
        return None

    def evaluations(self):
        """
        generator function to produce evaluations for a given forecast
        :return: evaluation object or empty iterator if none
        """
        if self.name and self.forecast_group.result_dir and self.forecast_group:
            for test in self.forecast_group.evaluation_tests:
                yield Evaluations(name=test, date=self.start_datetime, archive_dir=self.forecast_group.result_dir,
                                  forecast_name=self.name, conn=self.conn)
        else:
            return iter([])


class Evaluations(Model):
    def __init__(self, archive_dir=None, date=None, name=None, forecast_name=None, **kwargs):
        super().__init__(**kwargs)
        self.daily_archive_dir = None
        self.full_filepath = None
        self.forecast = None  # TODO: not implemented
        self.catalog = None  # TODO: not implemented
        self.compute_datetime = None  # TODO: not implemented
        self.forecast_group_archive_dir = archive_dir
        self.date = date  # should be datetime object
        self.name = name
        self.forecast_name = forecast_name
        self._list_of_result_files = []
        if self.date and self.forecast_group_archive_dir:
            self.daily_archive_dir = os.path.join(
                self.forecast_group_archive_dir,
                date.strftime("%Y-%m-%d")
            )
            if not self._list_of_result_files:
                self._list_of_result_files = os.listdir(self.daily_archive_dir)
            if self.name and self.daily_archive_dir:
                self.full_filepath = self.determine_full_filepath(regex=self._build_regex())

    def _build_regex(self):
        """
        internal function to create regex for evaluation files
        :return: regex object
        """
        year = self.date.strftime("%Y")
        month = self.date.strftime("%-m")
        day = self.date.strftime("%-d")
        p = re.compile(r"^scec\.csep\S*{}-Test_{}_{}_{}_{}-fromXML.xml"
                       .format(self.name, self.forecast_name, month, day, year))
        return p

    def determine_full_filepath(self, regex=None):
        """
        locates evaluation files in the results directory
        :param regex: regex object
        :return: list of found forecast files, empty list if none
        """
        paths = self._list_of_result_files
        filters = [regex.match, lambda x: not x.endswith('.meta')]
        matches = [path for path in paths if all(f(path) for f in filters)]
        # apply full filename
        full = list(map(lambda x: os.path.join(self.daily_archive_dir, x), matches))
        return full


class Dispatchers_ForecastGroups(Model):

    """
    to support ManyToMany relationships the join table must be manually populated
    """
    _table_type = "join"

    def __init__(self, dispatcher_id=None, group_id=None, **kwargs):
        super().__init__(**kwargs)
        self.dispatcher_id = dispatcher_id
        self.group_id = group_id





