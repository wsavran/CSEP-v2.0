from datetime import datetime, timedelta
from functools import wraps
#from ForecastGroupInitFile import ForecastGroupInitFile
#from DispatcherInitFile import DispatcherInitFile

# 'global' params for utils functions
params = {'forecast_archive': 'forecasts/archive',
          'evaluation_archive': 'results',
          'forecast_extension': '-fromXML.dat',
          'forecast_meta_extension': 'fromXML.dat.meta'}

def get_dispatcher_config_filename(tcsh_scriptname):
    """
    returns the filename of the forecast group config file by parsing the command-line options
    in the run script used to call the dispatcher.
    :param tcsh_scriptname: name of .tcsh script used to call Dispatcher
    :return: string containing forecast group config file name, not found returns None
    """
    # regex meant to capture non-whitespace after --configFile=
    p = re.compile(r"(?<=--configFile=)\S+")
    with open(tcsh_scriptname, 'r') as f:
        lines = f.read()
    ds_name = p.search(lines).group(0)
    return ds_name


def get_forecastgroup_path(dscript_name):
    """
    get forecast group from dispatcher init file
    :param dscript_name: filepath of dispatcher init file
    :return: string containing the path of the forecast group
    """
    # create object to represent init file
    try:
        df = DispatcherInitFile(dscript_name)
    except RuntimeError:
        print("Warning: Could not create Dispatcher script object from script using configFile={}." \
              .format(dscript_name))
        if debug:
            raise
        return None
    # extract from init file
    try:
        dcf_name = df.elementValue(DispatcherInitFile.ForecastGroupElement)
    except RuntimeError:
        print("Warning: Could not extract ForecastGroup from Dispatcher config file {}".format(dscript_name))
        if debug:
            raise
        return None
    return dcf_name


def get_forecast_archive_dir(forecast_group_dir, forecast_date):
    """
    gets directory to search for forecast given forecast group and forecast date
    :param forecast_date:
    :return:
    """
    suffix = forecast_date.strftime("%Y_%-m")
    return os.path.join(forecast_group_dir, params['forecast_archive'], suffix)


def get_models_from_forecast_group(fgroup_path):
    """
    returns list of models called for each forecast group
    :param fgroup_path: file path to the directory containing the forecast group init file
    :return: list of models running in the forecast group or none
    """
    # create object representing forecast group
    try:
        fg = ForecastGroupInitFile(fgroup_path)
    except RuntimeError:
        print("Warning: Could create Forecast group object using the path {}".format(fgroup_path))
        if debug:
            raise
        return None
    # get list of models
    try:
        models = fg.elementValue(ForecastGroupInitFile.ModelElement)
    except RuntimeError:
        print("Warning: Could not extract models from ForecastGroup {}.".format(fgroup_path))
        if debug:
            raise
        return None
    return models


def text_to_datetime(datetime_string):
    """
    convert text in sqlite3 database to python datetime object
    :param datetime_string: string in format MM-DD-YYYY HH:MM:SS
    :return: datetime object
    """
    return datetime.strptime(datetime_string, '%Y-%m-%d %H:%M:%S')


def datetime_to_text(datetime_object):
        """
        covert python3 datetime object to text string to store in sqlite3 database
        :param datetime_object: python3 datetime object
        :return: string containing datetime in format MM-DD-YYYY HH:MM:SS
        """
        return str(datetime_object)


