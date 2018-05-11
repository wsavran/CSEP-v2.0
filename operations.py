import os, re, sys
from datetime import datetime
from utils import daterange
import models


# parse new zealand testing center. specific logic will be slightly different depending on the center
if __name__ == "__main__":

    # New Zealand testing center forecast groups, starting with one-day models
    # file paths have changed, don't want to rewrite all file names
    dispatch = {'One Day': '/mnt/nzcopy/csep_home/NewZealandCode/configuration/crontab/dispatcher_daily.tcsh'}

    # expected evaluations and forecasts for csep-nz, the forecast.init.xml files do not match up
    # with the forecast models in the directories
    expected_evaluations = {'One Day': ['N', 'L', 'R', 'ROC', 'MASS']}
    expected_forecasts = {'One Day': ['STEP', 'STEPJAVA_ABU', 'ETAS', 'PPEETAS']}

    # range of dates expected for csep-nz one-day-forecasts based on catalog information
    start = datetime(2007, 5, 18, 0, 0, 0)
    end = datetime(2017, 8, 6, 0, 0, 0)

    # database to store everything
    db_name = 'csep_db'

    for period, path in dispatch.items():
        dispatcher = models.Dispatchers(path)


        for day in daterange(start, end):
            # create data models, even if model not found...
            sch_fore = models.ScheduledForecasts(day, db=db_name)
            disp = models.Dispatchers(dispatch[period], db=db_name)

            # iterate over the expected forecasts
            for expected_forecast in expected_forecasts[period]:

                fc = models.Forecasts(period, sch_fore.date_time, expected_forecast, db=db_name)

                # handle special case where STEP=>STEPJAVA
                if not os.path.exists(fc.filepath) and expected_forecast == 'STEP':
                    fc.update_name('STEPJAVA')

                if not os.path.exists(fc.filename):
                    # handle missing forecast

                    fc.status = 'Missing'

                    commit_missing_forecast(sch_fore,  )

                else:
                    # start putting together forecast information

                    # parse metadata

                        # get runtime_testdate

                        # get waiting_period

                        # store filepath

                        # get dispatcher information

                            # get config_file_name

                            # get command_line_args

                            # get logfile

                            # get script_name (if possible, but not totally necessary with this approach)

                        # get catalog information

                            # get runtime dir from dispatcher

                            # store catalog filename

                            # get catalog meta data

                            # store catalog post_processing info













