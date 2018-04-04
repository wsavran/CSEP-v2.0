#
# CSEP DB Extract Transform Load
# Example script for creating sqlite3 db for csep
# Creates tables and loads sample dataset
#

import sqlite3
import sys
import configparser
import os.path
#
# Global Variables used
#
__version__ = "v18.3.28"
db_name = "csep_%s.db"%(__version__)
db_backup = "%s_backup"%(db_name)

def create_db_tables():
    #
    # Connect to db
    #
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    #
    # Construct the schema from bottom up, starting with smallest tables, and building up to
    # tables with multiple foreign keys
    #

    #
    # Create ScheduledE


    #
    # Create Models table
    c.execute('''
        create table if not exists models
        (model_id integer primary key,
        model_name varchar(250));
    ''')

    #
    # Load Models sample data
    c.execute('''
              INSERT INTO models(model_id,model_name) VALUES(NULL,'STEP')
    ''')
    conn.commit()


    #
    # Create Dispatcher Table
    #
    c.execute()

    c.execute('''
        CREATE TABLE if not exists forecasts
        (forecast_id INTEGER PRIMARY KEY,
        model_id integer not null,
        dispatcher_id integer not null,
        schedule_id integer not null,
        forecast_group_id integer not null,
        forecast_name varchar(100) not null,
        forecast_file_name varchar(250),
        foreign key(model_id) references models(model_id),
        foreign key(dispatcher_id) references dispatcher(dispatcher_id);
              ''')

    c.close()

if __name__ == "__main__":
    print("Starting Create and Load csep_db")
    # if db file exists, rename it
    if os.path.isfile(db_name):
        os.rename(db_name,db_backup)
    try:
        create_db_tables()
    except os.error as e:
        print(e)

    print("Completed loading %s"%(db_name))
    sys.exit(0)