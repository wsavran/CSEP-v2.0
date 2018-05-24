import unittest
import os
import sqlite3
from models import Model

"""
Testing model base class to ensure db functionality working properly.
"""


class TestInsert(unittest.TestCase):
    """
    should insert into the database testing the following behavior:
    1) insert_id corresponds to the lastinsertid from the database
    2) model with foreign key inserted correctly
    3) model with join table inserted correctly
    4) model with no foreign key inserted correctly
    """
    def setUp(self):
        """
        create test database for getting fields
        :return:
        """
        conn = sqlite3.connect('test_db')
        cursor = conn.cursor()

        tables = ["""CREATE TABLE IF NOT EXISTS Dispatchers_ForecastGroups (
                     dispatcher_id INTEGER NOT NULL,
                     group_id INTEGER NOT NULL,
                     PRIMARY KEY(dispatcher_id, group_id)
                     );""",
                  """CREATE TABLE IF NOT EXISTS Catalogs (
                     catalog_id INTEGER PRIMARY KEY,
                     data_filename TEXT NOT NULL,
                     creation_date TEXT NOT NULL,
                     post_processing TEXT NOT NULL
                     );""",
                  """CREATE TABLE IF NOT EXISTS Forecasts (
                     forecast_id INTEGER PRIMARY KEY,
                     name TEXT,
                     catalog_id INTEGER,
                     FOREIGN KEY(catalog_id) REFERENCES Catalogs
                     );""",
                  """CREATE TABLE IF NOT EXISTS Evaluations (
                     evaluation_id INTEGER PRIMARY KEY,
                     compute_datetime TEXT,
                     filepath TEXT,
                     catalog_id INTEGER,
                     forecast_id INTEGER,
                     FOREIGN KEY(catalog_id) REFERENCES Catalogs
                     FOREIGN KEY(forecast_id) REFERENCES Forecasts
                     );"""]

        for create_table in tables:
            cursor.execute(create_table)

        conn.commit()

    def tearDown(self):
        os.remove('test_db')

    def test_insert_no_foreign_key(self):
        """ tests the insert was correct
        by inserting then querying database and printing out results
        """
        db = sqlite3.connect('test_db')
        cursor = db.cursor()

        class Catalogs(Model):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.data_filename = 'test_filename'
                self.creation_date = '5-23-2018 12:00:00'
                self.post_processing = 'unknown'

        c = Catalogs(conn=db)
        status = c.insert()

        # fail test if insert fails
        self.assertEqual(True, status)

        # verify integrity in database
        cursor.execute('select * from Catalogs')
        result = list(cursor.fetchone())

        test_result = [1, 'test_filename', '5-23-2018 12:00:00', 'unknown']

        self.assertListEqual(result, test_result)

    def test_last_insert_id(self):
        """tests whether the last insert id is properly bound to the class after insert() is called
        and before save is called. this will be tested on a class with no foreign keys."""

        db = sqlite3.connect('test_db')
        cursor = db.cursor()

        class Catalogs(Model):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.data_filename = 'test_filename'
                self.creation_date = '5-23-2018 12:00:00'
                self.post_processing = 'unknown'

        c = Catalogs(conn=db)

        if c.insert():
            c.save()

        # generate query to obtain values
        cursor.execute('select catalog_id from Catalogs')

        self.assertEqual(1, c.insert_id)

    def test_insert_with_single_depth_foreign_key(self):
        """tests whether a model with simple foreign key relationship is correctly inserted into the database"""

        db = sqlite3.connect('test_db')
        cursor = db.cursor()

        class Catalogs(Model):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.data_filename = 'test_catalog_filename'
                self.creation_date = '5-23-2018 12:00:00'
                self.post_processing = 'unknown'

        class Forecasts(Model):
            def __init__(self, catalog_id=None, **kwargs):
                super().__init__(**kwargs)
                self.name = 'test_forecast'
                self.catalog_id = catalog_id

        c = Catalogs(conn=db)
        e = Forecasts(catalog_id=c, conn=db)

        if e.insert():
            e.save()

        cursor.execute("select * from Forecasts join Catalogs on Forecasts.catalog_id=Catalogs.catalog_id")
        result = list(cursor.fetchone())

        test_result = [1, 'test_forecast', 1,
                       1, 'test_catalog_filename', '5-23-2018 12:00:00', 'unknown']

        self.assertListEqual(result, test_result)

    def test_insert_with_dual_depth_foreign_key(self):
        """tests whether a model with simple foreign key relationship is correctly inserted into the database"""

        db = sqlite3.connect('test_db')
        cursor = db.cursor()

        class Catalogs(Model):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.data_filename = 'test_catalog_filename'
                self.creation_date = '5-23-2018 12:00:00'
                self.post_processing = 'unknown'

        class Forecasts(Model):
            def __init__(self, catalog_id=None, **kwargs):
                super().__init__(**kwargs)
                self.name = 'test_forecast'
                self.catalog_id = catalog_id

        class Evaluations(Model):
            def __init__(self, catalog_id=None, forecast_id=None, **kwargs):
                super().__init__(**kwargs)
                self.compute_datetime = '5-23-2018 12:00:00'
                self.filepath = 'test_evaluation_filepath'
                self.forecast_id = forecast_id
                self.catalog_id = catalog_id

        c = Catalogs(conn=db)
        f = Forecasts(catalog_id=c, conn=db)
        e = Evaluations(catalog_id=c, forecast_id=f, conn=db)

        if e.insert():
            e.save()

        cursor.execute("select * from Evaluations join Forecasts on Evaluations.forecast_id=Forecasts.forecast_id" +
                       " join Catalogs on Forecasts.catalog_id=Catalogs.catalog_id")

        result = list(cursor.fetchone())

        # rowid=2 for catalog because the db schema defines the catalog to be inserted before the forecast, otherwise
        # it would have rowid=1. catalog_id=2 in Forecasts for this same reason
        test_result = [1, '5-23-2018 12:00:00', 'test_evaluation_filepath', 1, 1,
                       1, 'test_forecast', 2,
                       2, 'test_catalog_filename', '5-23-2018 12:00:00', 'unknown']

        self.assertListEqual(result, test_result)


class TestDatabaseAccess(unittest.TestCase):
    """
    should return the fields listed in the database, for non-join tables the
    primary key should be omitted. for join tables, both primary keys should
    be returned.
    """
    def setUp(self):
        """
        create test database for getting fields
        :return:
        """
        conn = sqlite3.connect('test_db')
        cursor = conn.cursor()

        tables = ["""CREATE TABLE IF NOT EXISTS Dispatchers_ForecastGroups (
                     dispatcher_id INTEGER NOT NULL,
                     group_id INTEGER NOT NULL,
                     PRIMARY KEY(dispatcher_id, group_id)
                     );""",
                  """CREATE TABLE IF NOT EXISTS Model (
                     model_id INTEGER PRIMARY KEY,
                     data_filename TEXT NOT NULL,
                     creation_date TEXT NOT NULL,
                     post_processing TEXT NOT NULL );""",
                  """CREATE TABLE IF NOT EXISTS Catalogs (
                     catalog_id INTEGER PRIMARY KEY,
                     data_filename TEXT NOT NULL,
                     creation_date TEXT NOT NULL,
                     post_processing TEXT NOT NULL
                     );""",
                  """CREATE TABLE IF NOT EXISTS Evaluations (
                     evaluation_id INTEGER PRIMARY KEY,
                     compute_datetime TEXT,
                     scheduled_id INTEGER NOT NULL,
                     catalog_id INTEGER,
                     evaluation_type_id INTEGER,
                     forecast_id INTEGER,
                     filepath TEXT,
                     FOREIGN KEY(scheduled_id) REFERENCES ScheduledEvaluations,
                     FOREIGN KEY(evaluation_type_id) REFERENCES EvaluationTypes,
                     FOREIGN KEY(catalog_id) REFERENCES Catalogs,
                     FOREIGN KEY(forecast_id) REFERENCES Forecasts
                     );"""]

        for create_table in tables:
            cursor.execute(create_table)

        conn.commit()

    def tearDown(self):
        os.remove('test_db')

    def test_get_fields_from_model_no_foreign_key(self):
        """
        tests to make sure that fields are correctly recovered from database and binded to class attribute.
        """
        db = sqlite3.connect('test_db')

        class Catalogs(Model):
            pass

        c = Catalogs(conn=db)
        fields = ['data_filename', 'creation_date', 'post_processing']
        self.assertListEqual(c.fields, fields)

    def test_get_fields_from_model_with_foreign_key(self):
        """
        tests to verify that foreign keys are properly recovered from database and binded to class attribute
        """
        db = sqlite3.connect('test_db')

        class Evaluations(Model):
            pass

        e = Evaluations(conn=db)
        fields = ['compute_datetime',
                  'scheduled_id',
                  'catalog_id',
                  'evaluation_type_id',
                  'forecast_id',
                  'filepath']

        self.assertListEqual(e.fields, fields)

    def test_get_fields_from_model_with_join_table(self):
        db = sqlite3.connect('test_db')

        class Dispatchers_ForecastGroups(Model):
            _table_type = 'join'

        dfg = Dispatchers_ForecastGroups(conn=db)
        fields = ['dispatcher_id', 'group_id']
        self.assertListEqual(dfg.fields, fields)

    def test_get_table_name(self):
        """
        checks whether class name is properly stored in self.table. does not require database
        """
        db = sqlite3.connect('test_db')
        model = Model(conn=db)
        self.assertEqual('Model', model.table)


class TestGetValues(unittest.TestCase):
    """
    should return values from Model class as generator yielding either Model type
    or value of column.
    """
    def setUp(self):
        """
        create test database for getting fields
        :return:
        """
        conn = sqlite3.connect('test_db')
        cursor = conn.cursor()

        tables = ["""CREATE TABLE IF NOT EXISTS Catalogs (
                     catalog_id INTEGER PRIMARY KEY,
                     data_filename TEXT NOT NULL,
                     creation_date TEXT NOT NULL,
                     post_processing TEXT NOT NULL
                     );""",
                  """CREATE TABLE IF NOT EXISTS Evaluations (
                     evaluation_id INTEGER PRIMARY KEY,
                     catalog_id INTEGER, 
                     filepath TEXT,
                     FOREIGN KEY(catalog_id) REFERENCES Catalogs
                     );"""]

        for create_table in tables:
            cursor.execute(create_table)

        conn.commit()

    def tearDown(self):
        os.remove('test_db')

    def test_get_values_no_foreign_key(self):
        """
        return values from Model type where table has no foreign key
        """
        db = sqlite3.connect('test_db')

        class Catalogs(Model):
            data_filename = 'test_filename'
            creation_date = '5-23-2018 12:00:00'
            post_processing = 'test_postprocessing'

        c = Catalogs(conn=db)

        fields, values = zip(*list(c._db_values()))
        values = list(values)
        test_values = ['test_filename', '5-23-2018 12:00:00', 'test_postprocessing']

        self.assertListEqual(values, test_values)

    def test_get_values_with_foreign_key(self):
        """
        return values from Model type where table has foreign key
        """
        db = sqlite3.connect('test_db')

        class Catalogs(Model):
            data_filename = 'test_filename'
            creation_date = '5-23-2018 12:00:00'
            post_processing = 'test_postprocessing'

        c = Catalogs(conn=db)

        class Evaluations(Model):
            catalog_id = c
            filepath = 'test_filepath'

        e = Evaluations(conn=db)
        fields, values = zip(*list(e._db_values()))
        values = list(values)
        test_values = [c, 'test_filepath']

        self.assertListEqual(values, test_values)


class TestGenerateInsertValues(unittest.TestCase):
    """
    should generate dict of quoted strings ready to insert into database mapping field ->
    value
    """

    def test_get_insertable_value_strings_no_foreign_key(self):
        """
        tests the conversion of model attributes to dict values that can be inserted into sqlite3. namely represented
        as strings with quotes surrounding each value. also needs to handle the case where a model might have
        dependencies that need to be inserted.
        """

    def setUp(self):
        """
        create test database for getting fields
        """
        conn = sqlite3.connect('test_db')
        cursor = conn.cursor()

        tables = ["""CREATE TABLE IF NOT EXISTS Catalogs (
                     catalog_id INTEGER PRIMARY KEY,
                     data_filename TEXT NOT NULL,
                     creation_date TEXT NOT NULL,
                     post_processing TEXT NOT NULL
                     );""",
                  """CREATE TABLE IF NOT EXISTS Evaluations (
                     evaluation_id INTEGER PRIMARY KEY,
                     catalog_id INTEGER, 
                     filepath TEXT,
                     FOREIGN KEY(catalog_id) REFERENCES Catalogs
                     );"""]

        for create_table in tables:
            cursor.execute(create_table)

        conn.commit()

    def tearDown(self):
        os.remove('test_db')

    def test_get_values_no_foreign_key(self):
        """
        return values from Model type where table has no foreign keys
        """
        db = sqlite3.connect('test_db')

        class Catalogs(Model):
            data_filename = 'test_filename'
            creation_date = '5-23-2018 12:00:00'
            post_processing = 'test_postprocessing'

        c = Catalogs(conn=db)

        # manually populate dict of db insert values
        for field, value in c._db_values():
            c._prepare_insert_values(field, value)

        test_dict = {'data_filename': '"test_filename"',
                     'creation_date': '"5-23-2018 12:00:00"',
                     'post_processing': '"test_postprocessing"'}

        self.assertDictEqual(test_dict, c._insert_values)


if __name__ == "__main__":
    unittest.main()
