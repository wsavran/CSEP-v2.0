import os, re
from ForecastGroupInitFile import ForecastGroupInitFile
from DispatcherInitFile import DispatcherInitFile

# logical workflow for csep_db (and max's request)

# 1) get list of run-scripts listed in the crontab
# 2) parse configFile argument of dispatcher script
# 3) read forecast groups from configFile
# 4) parse forecast group config file to get models
# 5) parse models from forecast group config and assign to list
# 6) use set feature in python to make unique list and count

debug = True
operational_run_scripts = ['/usr/local/csep/cronjobs/dispatcher_ANSS1985_forecasts.tcsh',
                           '/usr/local/csep/cronjobs/dispatcher_ANSS1985.tcsh',
                           '/usr/local/csep/cronjobs/dispatcher_ANSS1985_one_day.tcsh',
                           '/usr/local/csep/cronjobs/dispatcher_ANSS1932.tcsh',
                           '/usr/local/csep/cronjobs/dispatcher_ANSS1932_notFiltered.tcsh',
                           '/usr/local/csep/cronjobs/batch_ANSS1985_30min.tcsh',
                           '/usr/local/csep/cronjobs/batch_ANSS1985_30min_TWTests.tcsh',
                           '/usr/local/csep/cronjobs/dispatcher_ANSS1985_M2_95.tcsh',
                           '/usr/local/csep/cronjobs/dispatcher_ANSS1932_notFiltered_Md2_one_day.tcsh',
                           '/usr/local/csep/cronjobs/dispatcher_ANSS1932_notFiltered_Md2.tcsh']


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


if __name__ == "__main__":
    total_models=0
    for script in operational_run_scripts:
        ds = get_dispatcher_config_filename(script)
        fg = get_forecastgroup_path(ds)
        models = get_models_from_forecast_group(fg)
        total_models += len(models)
        # print("Found ForecastGroup at path {} from .tcsh script {} in configFile {}." \
        #       .format((fg, script, ds)))
        print('Found {} models in the testing center.'.format(total_models))




