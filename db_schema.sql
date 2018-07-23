CREATE TABLE IF NOT EXISTS Schedules (
    schedule_id INTEGER PRIMARY KEY,
    date_time TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS Dispatchers (
    dispatcher_id INTEGER PRIMARY KEY,
    script_name TEXT NOT NULL UNIQUE,
    waiting_period TEXT,
    config_file_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ForecastGroups (
    forecastgroup_id INTEGER PRIMARY KEY,
    group_name TEXT NOT NULL,
    group_path TEXT NOT NULL,
    group_description TEXT,
    config_filepath TEXT NOT NULL,
    dispatcher_id INTEGER NOT NULL,
    FOREIGN KEY(dispatcher_id) REFERENCES Dispatchers
);

CREATE TABLE IF NOT EXISTS Forecasts (
    forecast_id INTEGER PRIMARY KEY,
    schedule_id INTEGER NOT NULL,
    group_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    filepath TEXT,
    meta_filepath TEXT,
    waiting_period TEXT,
    logfile TEXT,
    status TEXT,
    FOREIGN KEY(schedule_id) REFERENCES Schedules,
    FOREIGN KEY(group_id) REFERENCES ForecastGroups,
    UNIQUE(filepath)
);

CREATE TABLE IF NOT EXISTS Evaluations (
    evaluation_id INTEGER PRIMARY KEY,
    schedule_id INTEGER NOT NULL,
    forecast_id INTEGER NOT NULL,
    filepath TEXT,
    name TEXT,
    status TEXT,
    runtime_dir TEXT,
    creation_datetime TEXT,
    catalog_result_filepath TEXT,
    catalog_status TEXT,
    catalog_creation_datetime TEXT,
    FOREIGN KEY(schedule_id) REFERENCES Schedules,
    FOREIGN KEY(forecast_id) REFERENCES Forecasts
    UNIQUE(forecast_id, name)
);
