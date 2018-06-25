# csep_db
Extract, transfer, and load utilities for meta data on CSEP testing centers. 

1. clone repo on desired CSEP server ```https://github.com/wsavran/csep_db.git```
2. run unit tests ```python3 -m unittest -v tests```
	if there are any errors, please contact software@scec.org
3. run ```python3 extract.py```

### notes
multiple dispatchers can be added to the ```extract.py``` script, or changed for different testing centers. please open an issue on the github repository to show any bugs or request new features.

#### useful queries

print count of missing forecasts in each group <br>
``` select name, count(Forecasts.rowid) from Forecasts where status='Missing' group by name; ```

list forecast groups and associated forecasts <br>
``` select ForecastGroups.group_path, group_concat(distinct Forecasts.name) from ForecastGroups join Forecasts on ForecastGroups.forecastgroup_id=Forecasts.group_id group by ForecastGroups.group_name;```

list name of missing forecasts and date <br>
```select ScheduledForecasts.date_time, group_concat(Forecasts.name) from Forecasts join ScheduledForecasts on Forecasts.schedule_id=ScheduledForecasts.scheduled_forecast_id where status='Missing' group by ScheduledForecasts.date_time;```

list expected evaluation for each forecast
```select ForecastGroups.group_name, group_concat(distinct Forecasts.name), group_concat(distinct Evaluations.name) from Evaluations join Forecasts on Evaluations.forecast_id=Forecasts.forecast_id join ForecastGroups on ForecastGroups.forecastgroup_id=Forecasts.group_id group by ForecastGroups.group_name order by ScheduledEvaluations.date_time;```

enumerate the missing evaluations for each forecast
```select Forecasts.name, Evaluations.name, count(Evaluations.rowid) from Evaluations join Forecasts on Evaluations.forecast_id=Forecasts.forecast_id where Evaluations.status="Missing" and not Forecasts.status="Missing" group by Forecasts.name, Evaluations.name;```
