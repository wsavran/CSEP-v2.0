import os
import re
import glob
import sqlite3
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from ForecastGroupInitFile import ForecastGroupInitFile
from DispatcherInitFile import DispatcherInitFile
from artifacts.utils import text_to_datetime

"""

TODO: 1) debug and propagate changes to models in api
      2) write script to use the api to populate database with csep-debug data, for all forecasts and 
         evaluations for one-day-models class
"""


class Schedule:
    end_date = datetime(2019, 1, 1, 0, 0, 0)

    def __init__(self, start_date=None):
        self.start_date = start_date

    def date_range(self, days=1, months=0, years=0):
        date = self.start_date
        if date:
            while date < self.end_date:
                yield date
                date += relativedelta(days=days, months=months, years=years)
        else:
            return iter([])


class Model:

    _table_type = "standard"

    def __init__(self, conn=None):
        self._insert_id = None
        self._conn = conn
        self.table = None
        self.fields = []
        self._insert_values = {}
        self._inserted = False

        # must manually assign columns as unique, store fieldname as string
        self._unique_columns = []

        if self._conn:
            self.table = self.__class__.__name__
            self.fields = self._fields()

    def _prepare_insert_values(self, field, value):
        """
        puts Model attributed into dict containing values that can be inserted into sqlite3
        resolves all FK fields and stores values in sqlite3 quoted string format
        :param field: name of field
        :param value: value of the field, likely obtained from getattr()
        :return: none
        """
        # enforcing unique values for foreign keys.
        if isinstance(value, Model):
            for unique_field in value._unique_columns:
                if not value._inserted:
                    # check if row containing unique column exists in database
                    cursor = value._conn.cursor()
                    cursor.execute('select rowid from {} where {}=?;'.format(value.table, unique_field),
                                   (getattr(value, unique_field),))
                    result = cursor.fetchone()
                    if result:
                        value._inserted = True
                        value._insert_id = int(result[0])

            if not value._inserted:
                # recursive call, will stop when no models have more dependencies
                value.insert()

            # update dict to store fk of inserted model
            last_insert_id = value.insert_id
            self._insert_values[field] = '"' + str(last_insert_id) + '"'
        else:
            self._insert_values[field] = '"' + str(value) + '"'

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

        for field, value in fields_values:
            self._prepare_insert_values(field, value)

        fields, values = zip(*self._insert_values.items())
        fields = ', '.join(fields)
        values = ', '.join(values)

        cursor = self.conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO {0} ({1}) VALUES ({2})".format(self.table, fields, values))

        # update insert id for fk purposes
        self._insert_id = cursor.lastrowid
        self._inserted = True

        return True

    def save(self):
        """
        no error checking here. will not insert blank models
        :return:
        """
        if self._inserted:
            self.conn.commit()

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
            print("Error: Unable to retrieve attributes. Ensure that {} has attributes corresponding to DB fields."
                  .format(self.table))


class Schedules(Model):
    end_date = datetime(2019, 1, 1, 0, 0, 0)

    def __init__(self, start_date, **kwargs):
        super().__init__(**kwargs)

        # db fields
        self.date_time = ''
        self._unique_columns.append('date_time')

        self.start_date = start_date  # should be datetime object
        if self.start_date and isinstance(start_date, datetime):
            self.date_time = self.datetime_to_text()
        else:
            raise AttributeError("start date must be datetime object.")

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
        return self.start_date.strftime('%Y-%m-%d')


class Dispatchers(Model):

    def __init__(self, script_name, config_file_name=None, waiting_period=None, **kwargs):
        super().__init__(**kwargs)
        self.forecast_group_paths = []

        # db fields
        self.script_name = script_name
        self.config_file_name = config_file_name
        self.waiting_period = waiting_period

        # populate db fields
        if self.script_name:
            self.config_file_name = self.parse_config_file()
            self.waiting_period = self.parse_waiting_period()
        else:
            raise AttributeError("script name cannot be none.")

        if self.config_file_name:
            self.parse_forecastgroup_path()
        else:
            raise AttributeError("config filename cannot be none.")

    def forecast_groups(self):
        for group_path in self.group_paths():
            forecast_group = ForecastGroups(group_path, self, conn=self.conn)
            yield forecast_group

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
            return p.search(para).group(1)
        except AttributeError:
            print('Warning: Could not parse config file name from dispatcher script')
            return None

    def parse_waiting_period(self):
        p = re.compile(r'waitingPeriod=(\S*)')
        with open(self.script_name, 'r') as f:
            lines = f.readlines()
        para = ''.join(lines).strip()
        try:
            # should be regex capture group 1
            return int(p.search(para).group(1))
        except AttributeError:
            print('Warning: Could not parse waiting period from dispatcher script')
            return None

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

    def __init__(self, group_path, dispatcher_id=None,
                 config_filepath='', group_name='', group_description='', **kwargs):
        super().__init__(**kwargs)
        self.entry_date = None
        self.result_dir = None
        self.post_processing = None
        self.fg = None
        self.group_dir = None
        self.forecast_dir = None
        self.post_processing = None
        self.entry_date = None
        self.observation_dir = None
        self.models = []
        self.expected_forecasts = []

        # database fields
        self.group_path = group_path
        self.group_name = group_name
        self.group_description = group_description
        self.config_filepath = config_filepath
        self.dispatcher_id = dispatcher_id

        if group_path:
            self.fg = ForecastGroupInitFile(self.group_path)
            self.group_description = self.parse_group_description()
            self.group_name = self.parse_group_name()
            self.config_filepath = os.path.join(self.group_path, 'forecast.init.xml')
            self.models = self.parse_models()
            self.forecast_dir = self.parse_forecast_dir()
            self.expected_forecasts = self.parse_expected_forecasts()
            self.evaluation_schedule = self.parse_schedule('evaluationTests')
            self.forecast_schedule = self.parse_schedule('models')
            self.group_dir = os.path.basename(self.group_path)
            self.evaluation_tests = self.parse_evaluation_tests()
            self.result_dir = self.parse_result_dir()
            self.observation_dir = self.parse_observation_dir()
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

        FIXME: modify this routine to generate a schedule based on the schedule in the forecast group config file
        FIXME: addresses issues #12 and #15
        """
        if self.entry_date:
            s = Schedule(self.entry_date)
            for date in s.date_range():
                schedule = Schedules(date, conn=self.conn)
                yield schedule
        else:
            return iter([])

    def forecasts(self):
        """
        generator function to return forecasts associated with a particular forecast group
        :return:
        """
        for schedule in self.schedule():
            for name in self.expected_forecasts:
                forecast = Forecasts(schedule, self, name, self.forecast_dir, conn=self.conn)
                yield forecast

    def evaluations(self):
        """
            generator function to produce evaluations associated with a forecast group. note: evaluations made from the
            forecast group level do not have any association to a particular forecast. if wanting to populate a db model
            use the generator in class Forecasts(...)
            :return:
        """
        if not self.evaluation_tests:
            return iter([])
        for schedule in self.schedule():
            for test in self.evaluation_tests:
                for forecast in self.forecasts():
                    evaluation = Evaluations(schedule, forecast, self.result_dir, test, conn=self.conn)
                    yield evaluation

    def parse_forecast_dir(self):
        """
        reads location of forecasts stored in forecast group init file
        :return: full path of directory where forecast archive is located
        """
        fcdir_path = self.fg.elementValue('forecastDir')
        if not fcdir_path:
            return ''
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
            return ''
        if os.path.isabs(resdir_path):
            return resdir_path
        return os.path.join(self.group_path, resdir_path)

    def parse_observation_dir(self):
        """
        reads locations of results from forecast group init file
        :return: full path of directory where evaluations are stored
        """
        obsdir_path = self.fg.elementValue('catalogDir')
        if not obsdir_path:
            return ''
        if os.path.isabs(obsdir_path):
            return obsdir_path
        return os.path.join(self.group_path, obsdir_path)

    def parse_postprocessing(self):
        """
        reads postprocessing
        :return:
        """
        return self.fg.elementValue('postProcessing')

    def parse_group_description(self):
        """
        parses and sets the group name, fails loudly
        :param group_path: path of the top level folder to the forecast group
        :return:
        """
        # name is stored as attribute on root
        root = self.fg.root()
        name = root.attrib['name']
        return name

    def parse_group_name(self):
        """
        parses group name from group path string. name is assumed to be the basename of the path
        eg.) for group_path /home/csep/operations/SCEC-natural-laboratory/one-day-models-V9.1 the
        group_name is one-day-models-V9.1
        :return:
        """
        name = os.path.basename(self.group_path)
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

    def parse_expected_forecasts(self):
        """
        forecasts do not map 1 to 1 to models in CSEP. the expected filename of some forecasts may
        not directly correlate to the list of models and multiple forecasts may be produced by the
        same model, eg., ETAS_DROneDay -> [ETAS_DROneDayMd3, ETAS_DROneDayPPEMd3]

        this algorithm scans the archived forecast directory and extracts the forecast name using
        regular expressions. The list does not use any a priori information from the forecast group configuration
        file.

        :return: expected_forecasts [list] list of expected forecasts
        """

        unique_forecasts = {}
        expected_forecasts = []
        work_dir = self.forecast_dir

        # scan archive directory for files
        # if we need performance, this could be cached
        for root, dirs, names in os.walk(work_dir):
            regex = re.compile(r'(\S*)_\d+_\d+_\d+\S*')
            try:
                forecast_names = [regex.match(name).group(1) for name in names]
                for forecast_name in forecast_names:
                    if not forecast_name.startswith('scec.csep'):
                        unique_forecasts[forecast_name] = None
            except AttributeError:
                pass
        # if file has substring of model add to the expected forecasts
        found_forecasts = list(unique_forecasts.keys())
        # first add forecasts sharing the name as model
        # and remove from found files and listed models
        model_straglers = self.models
        for model in model_straglers:
            # model name is forecast name
            if model in found_forecasts:
                expected_forecasts.append(model)
                # hard-coded hack
                if model == 'ETAS':
                    continue
            for fc in found_forecasts:
                if model in fc:
                    expected_forecasts.append(fc)
        return expected_forecasts

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
        # FIXME: this should generate a list of schedule objects
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


class Forecasts(Model):

    # possible extensions for forecast files
    forecast_extensions = ['.xml', '-fromXML.xml', '.dat', '-fromXML.dat', '-fromXML.dat.targz']

    def __init__(self, schedule_id, group_id, name, archive_dir,
                 filepath=None, meta_filepath=None, runtime_testdate=None, waiting_period=None, logfile=None, status=None,
                 **kwargs):
        super().__init__(**kwargs)

        # database fields
        self.schedule_id = schedule_id
        self.group_id = group_id
        self.filepath = filepath
        self.meta_filepath = meta_filepath
        self.name = name
        self.logfile = logfile
        self.status = status
        self.waiting_period = waiting_period
        self.runtime_testdate = runtime_testdate

        # unique columns
        self._unique_columns.append('filepath')

        # should be passed in from forecast group generator
        self.archive_dir = archive_dir

        # default found extension
        self._filepaths = self.get_filenames()

        # look for filename on system
        self.status = 'Missing'
        found = False
        while not found and self._filepaths:
            self.filepath = self._filepaths.pop()
            if os.path.isfile(self.filepath):
                self.status = 'Complete'
                self.meta_filepath = self.filepath + '.meta'
                found = True

        if self.meta_filepath:
            self.waiting_period = self.parse_with_regex(r"--waitingPeriod=(\S*)'")
            self.runtime_testdate = self.parse_with_regex(r"--runtimeTestDate=(\S*)'")
            self.logfile = self.parse_with_regex(r"--logFile=(\S*)'")

        # assign forecasts as scheduled if greater than todays date
        if group_id.dispatcher_id and not self.waiting_period:
            self.waiting_period = group_id.dispatcher_id.waiting_period
        forecast_date = schedule_id.start_date
        current_date = datetime.today() - timedelta(days=int(self.waiting_period))
        if self.status == 'Missing' and forecast_date > current_date:
            self.status = 'Scheduled'

    def parse_with_regex(self, regex_string):
        p = re.compile(regex_string)
        try:
            with open(self.meta_filepath, 'r') as f:
                lines = f.readlines()
            para = ''.join(lines).strip()
            result = p.search(para)
            # returns result or None
            if result:
                return result.group(1)
            else:
                return result
        except FileNotFoundError:
            return None

    def get_filenames(self):
        """
        filename for a forecast file
        template -- <model_name>_<month>_<day>_<year>.xml
        :return: filename if found, None if not found
        """
        filepaths = []
        archive_subdir = self.schedule_id.start_date.strftime("%Y_%-m")
        for ext in self.forecast_extensions:
            relative_filepath = self.name + '_' + \
            self.schedule_id.start_date.strftime("%-m_%-d_%Y") + ext
            abs_path = os.path.join(self.archive_dir, 'archive', archive_subdir, relative_filepath)
            filepaths.append(abs_path)
        return filepaths

    def evaluations(self, **kwargs):
        """
        generator function to produce evaluations for a given forecast
        :return: evaluation object or empty iterator if none
        """
        if self.name and self.group_id.result_dir:
            for test in self.group_id.evaluation_tests:
                evaluation = Evaluations(self.schedule_id, self, self.group_id.result_dir, test, conn=self.conn)
                yield evaluation
        else:
            return iter([])


class Evaluations(Model):
    def __init__(self, schedule_id, forecast_id, archive_dir, evaluation_name, filepath='', status='',
                 creation_datetime='', runtime_dir='', full_list_of_files=[],
                 catalog_result_filepath='', catalog_status='', catalog_creation_datetime='', **kwargs):
        super().__init__(**kwargs)

        # database fields
        self.schedule_id = schedule_id
        self.forecast_id = forecast_id
        self.name = evaluation_name
        self.filepath = filepath
        self.status = status
        self.creation_datetime = creation_datetime
        self.runtime_dir = runtime_dir
        self.catalog_result_filepath = catalog_result_filepath
        self.catalog_status = catalog_status
        self.catalog_creation_datetime = catalog_creation_datetime

        self.daily_archive_dir = ''
        self.meta_filepath = []
        self.forecast_group_archive_dir = archive_dir
        self.relative_filepath = filepath

        self.date = schedule_id.start_date  # get from schedule_id
        self.forecast_name = forecast_id.name
        self._list_of_result_files = []
        self.full_list_of_files = full_list_of_files

        # need date and name for regex
        if self.full_list_of_files and self.date and self.name:
            self.filepath = self.determine_full_filepath(self._build_regex(),
                                                         full_list_of_paths=self.full_list_of_files)

        # cant build file path without date or result_dir
        elif self.date and self.forecast_group_archive_dir:
            self.daily_archive_dir = os.path.join(
                self.forecast_group_archive_dir,
                self.date.strftime("%Y-%m-%d")
            )
            try:
                self._list_of_result_files = os.listdir(self.daily_archive_dir)
            except FileNotFoundError:
                self.status = 'Missing'

            if self.name and self.daily_archive_dir:
                self.filepath = self.determine_full_filepath(self._build_regex())

        # even though creation_datetime exists in meta file we can use system cdate as a
        # fallback
        if self.filepath:
            self.meta_filepath = self.determine_meta_filepath()
            self.creation_datetime = self.parse_creation_datetime()

        # don't try to parse runtime dir if no meta file
        if self.meta_filepath:
            self.runtime_dir = self.parse_runtime_dir()

        # set status of evaluation
        if self.filepath:
            self.status = "Complete"
        else:
            self.status = "Missing"

        # get catalog information
        catalog = self.get_catalog()
        self.catalog_result_filepath = catalog.result_filepath
        self.catalog_status = catalog.status
        self.catalog_creation_datetime = catalog.creation_datetime

        # assign forecasts as scheduled if greater than today's date
        # evaluations happen the day after the forecast
        if self.forecast_id.waiting_period:
            waiting_period = self.forecast_id.waiting_period
        # FIXME: hard-coded for one-day models, don't ignore
        evaluation_date = self.schedule_id.start_date
        current_date = datetime.today() - timedelta(days=int(waiting_period)-1)
        if self.status == 'Missing' and evaluation_date > current_date:
            self.status = 'Scheduled'

    def parse_runtime_dir(self):
        p = re.compile(r"runtimeDirectory=(\S*)'")
        try:
            with open(self.meta_filepath, 'r') as f:
                lines = f.readlines()
            para = ''.join(lines).strip()
            result = p.search(para)
            if result:
                return result.group(1)
            else:
                return ''
        except FileNotFoundError:
            return ''

    def parse_creation_datetime(self):
        p = re.compile(r"CreationDateTime = (\S*)")
        try:
            with open(self.meta_filepath, 'r') as f:
                lines = f.readlines()
            para = ''.join(lines).strip()
            result = p.search(para)
            if result:
                datetime_string = result.group(1)
                dt = datetime.strptime(datetime_string, '%Y-%m-%dT%H:%M:%S')
                return dt.strftime('%Y-%m-%d')
        except FileNotFoundError:
            ctime = os.path.getctime(self.filepath)
            ctime_human = datetime.fromtimestamp(ctime).strftime('%Y-%m-%d')
            return ctime_human
        return ''

    def _build_regex(self):
        """
        internal function to create regex for evaluation files
        :return: regex object
        """
        year = self.date.strftime("%Y")
        month = self.date.strftime("%-m")
        day = self.date.strftime("%-d")
        p = re.compile(r"^\S*{}-Test_{}_{}_{}_{}\S*.xml"
                       .format(self.name, self.forecast_name, month, day, year))
        return p

    def determine_full_filepath(self, regex, full_list_of_paths=[]):
        """
        locates evaluation files in the results directory
        :param regex: regex object
        :param full_list_of_paths: optional, list of paths to search
        :return: list of found forecast files, empty list if none
        """
        paths = self._list_of_result_files
        if full_list_of_paths:
            paths = full_list_of_paths
        filters = [regex.match, lambda x: not x.endswith('.meta')]
        matches = [path for path in paths if all(f(path) for f in filters)]
        # apply full filename
        full = list(map(lambda x: os.path.join(self.daily_archive_dir, x), matches))
        full_newest = ''
        if full:
            full_newest = max(full, key=os.path.getctime)
        return full_newest

    def determine_meta_filepath(self):
        """
        locates evaluation files in the results directory
        :return: list of found forecast files, empty list if none
        """
        meta = self.filepath + '.meta'
        if os.path.isfile(meta):
            return meta
        else:
            return ''

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

        for field, value in fields_values:
            self._prepare_insert_values(field, value)

        fields, values = zip(*self._insert_values.items())

        fields = ', '.join(fields)
        values = ', '.join(values)

        try:

            cursor = self.conn.cursor()
            cursor.execute("INSERT INTO {0} ({1}) VALUES ({2})".format(self.table, fields, values))

            # update insert id for fk purposes
            self._insert_id = cursor.lastrowid
            self._inserted = True

        # handle the potential unique case
        except sqlite3.IntegrityError:
            # get rowid of table where the unique constraint was violated
            cursor = self.conn.cursor()
            cursor.execute('select rowid, status from Evaluations where forecast_id=? and name=?',
                           (self.forecast_id.insert_id, str(self.name)))
            result = cursor.fetchone()
            rowid = int(result[0])
            status_from_db = str(result[1])
            if result:
                # if new evaluation found, we want to update everything
                if self.status == "Complete":
                    cursor.execute('update Evaluations set filepath=?, status=?, runtime_dir=?, creation_datetime=?,' +
                                   ' catalog_result_filepath=?, catalog_status=?, catalog_creation_datetime=?' +
                                   ' where rowid=?', (self.filepath, self.status, self.runtime_dir, self.creation_datetime,
                                                      self.catalog_result_filepath, self.catalog_status,
                                                      self.catalog_creation_datetime, rowid))

                # if we have found a catalog, but not an evaluation only update the catalog
                # note: if there is an evaluation in the database, we don't want to overwrite it
                elif self.catalog_status == "Present" and status_from_db == 'Missing':
                    print(self.catalog_result_filepath)
                    cursor.execute('update Evaluations set catalog_result_filepath=?, catalog_status=?, ' +
                                   'catalog_creation_datetime=? where rowid=?',
                                   (self.catalog_result_filepath, self.catalog_status, self.catalog_creation_datetime,
                                    rowid))

                # update insert id for fk purposes
                self._insert_id = cursor.lastrowid
                self._inserted = True

        return True

    def get_catalog(self):
        catalog = Catalogs(self.schedule_id, self)
        return catalog


class Catalogs:
    """
    associates the catalog used for a particular evaluation, or the catalog that would be used in the
    case the evaluation does not exist.
    """
    def __init__(self, schedule_id, evaluation_id, filepath='', status='',
                 creation_datetime=''):

        # db fields
        self.schedule_id = schedule_id
        self.result_filepath = filepath
        self.status = status
        self.creation_datetime = creation_datetime

        # useful members not in db
        self.evaluation_id = evaluation_id
        self.fg = self.evaluation_id.forecast_id.group_id
        self.obs_dir = self.fg.observation_dir

        # observation date directory
        self.obs_dir_full = os.path.join(self.obs_dir, self.schedule_id.date_time)

        # parse information from catalogs dir
        self.creation_datetime, self.result_filepath = self.parse_result_filepath_and_creation_date()
        self.status = self.parse_status()

    def parse_result_filepath_and_creation_date(self):
        """
        initially return the catalog based on parsing metadata file

        meta datafile -> verify catalog type -> assign file name
        :return:
        """
        # in observation directory
        obs_dir = self.obs_dir_full
        meta_file_map = {}
        dates = []
        creation_datetime = ''
        result_filepath = ''

        meta_files = glob.glob(os.path.join(obs_dir, "*.meta"))
        if meta_files:
            for mfile in meta_files:
                metadata_dict = self.parse_data_from_metafiles(mfile)
                if metadata_dict['type'] == 'catalog.nodecl.dat':
                    meta_file_map[metadata_dict['creation_date']] = mfile
                    dates.append(metadata_dict['creation_date'])

            # eval.creation_date could be '' or some date string
            # if '' or date string not found in directory we get key error in dict
            try:
                meta_file = meta_file_map[self.evaluation_id.creation_datetime]
                creation_datetime = self.evaluation_id.creation_datetime
            # if does not exist, choose most recent
            except KeyError:
                creation_datetime = sorted(dates)[0]
                meta_file = meta_file_map[creation_datetime]

            result_filepath = meta_file[:-5]

        return creation_datetime, result_filepath

    def parse_status(self):
        """
        :return: (bool)
        """

        if not os.path.isfile(self.result_filepath):
            status = 'Missing'

        else:
            status = "Present"

        waiting_period = self.evaluation_id.forecast_id.waiting_period
        current_date = datetime.today() - timedelta(days=int(waiting_period)-1)
        if status == "Missing" and self.schedule_id.start_date > current_date:
            status = "Scheduled"

        return status

    @staticmethod
    def parse_data_from_metafiles(fname):
        """
        file type is stored as comment # in first line of the file
        :param fname: filename of file to object
        :return: dict() keys
                [type]
                [creation_date]
        """
        metadata = {}
        with open(fname) as f:
            lines = f.readlines()
        # filetype is first line of the file
        metadata['type'] = lines[0][1:].strip()

        p = re.compile(r"CreationDateTime = (\S*)")
        para = ''.join(lines).strip()
        result = p.search(para)
        if result:
            datetime_string = result.group(1)
            dt = datetime.strptime(datetime_string, '%Y-%m-%dT%H:%M:%S')
            metadata['creation_date'] = dt.strftime('%Y-%m-%d')
        else:
            metadata['creation_date'] = ''

        return metadata

