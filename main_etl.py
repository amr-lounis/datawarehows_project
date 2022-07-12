import math
import pandas as pd
import pymysql
import os
from datetime import datetime

# -----------------------------------------------------------------------------
dir_weather_data = './Weather Data'
dir_ataging_area = './Staging_Area/'
if not os.path.exists(dir_ataging_area):
    os.makedirs(dir_ataging_area)

file_csv_path_all_data = './Staging_Area/all_data.csv'

file_csv_path_station = './Staging_Area/station.csv'
file_csv_path_date = './Staging_Area/Dim_Date_1850-2050.csv'
file_csv_path_weather = './Staging_Area/weather.csv'

file_csv_path_cube_data ="./cube_data.csv"
# -----------------------------------------------------------------------------
my_host = 'localhost'
my_user = 'root'
my_password = ''
my_database = 'Weather_DataWarehouse'

# ----------------------------------------------------------------------------- all schemas
schema_table_weather_fact = {
    'STATION_ID': 'VARCHAR(20)',
    'Date_ID': 'VARCHAR(8)',
    'PRCP': 'float',
    'TAVG': 'float',
    'TMAX': 'float',
    'TMIN': 'float',
    'SNWD': 'float',
    'PGTM': 'float',
    'SNOW': 'float',
    'WDFG': 'float',
    'WSFG': 'float',
    'WT01': 'float',
    'WT02': 'float',
    'WT03': 'float',
    'WT05': 'float',
    'WT07': 'float',
    'WT08': 'float',
    'WT09': 'float',
    'WT16': 'float',
    'WT18': 'float',
    '': 'PRIMARY KEY(STATION_ID,Date_ID)'
}
# -----------------------------------------------------------------------------
schema_table_station_dim = {
    'STATION_ID': 'VARCHAR(20)',
    'CITY': 'VARCHAR(50)',
    'COUNTRY': 'VARCHAR(20)',
    'COUNTRY_ISO': 'VARCHAR(4)',
    'CODEN': 'INT',
    'LATITUDE': 'float',
    'LONGITUDE': 'float',
    'ELEVATION': 'float',
    '': 'PRIMARY KEY(STATION_ID)'
}
# -----------------------------------------------------------------------------
schema_table_date_dim = {
    'Date_ID': 'VARCHAR(8)',
    'Date': 'VARCHAR(10)',
    'Day_Name': 'VARCHAR(10)',
    'Day_Name_Abbrev': 'VARCHAR(3)',
    'Day_Of_Month': 'INT(2)',
    'Day_Of_Week': 'INT(1)',
    'Day_Of_Year': 'INT(3)',
    'Holiday_Name': 'VARCHAR(35)',
    'Is_Holiday': 'VARCHAR(5)',
    'Is_Weekday': 'VARCHAR(5)',
    'Is_Weekend': 'VARCHAR(5)',
    'Month_Abbrev': 'VARCHAR(3)',
    'Month_End_Flag': 'VARCHAR(5)',
    'Month_Name': 'VARCHAR(15)',
    'Month_Number': 'INT(2)',
    'Quarter': 'INT(1)',
    'Quarter_Name': 'VARCHAR(6)',
    'Quarter_Short_Name': 'VARCHAR(2)',
    'Same_Day_Previous_Year': 'VARCHAR(10)',
    'Same_Day_Previous_Year_ID': 'VARCHAR(8)',
    'Season': 'VARCHAR(10)',
    'Week_Begin_Date': 'VARCHAR(10)',
    'Week_Begin_Date_ID': 'INT(8)',
    'Week_Num_In_Month': 'INT(1)',
    'Week_Num_In_Year': 'INT(2)',
    'Year': 'VARCHAR(4)',
    'Year_And_Month': 'VARCHAR(7)',
    'Year_And_Month_Abbrev': 'VARCHAR(8)',
    'Year_And_Quarter': 'VARCHAR(7)',
    '': 'PRIMARY KEY(Date_ID)'
}

# ----------------------------------------------------------------------------- Database class

class Database:
    my_db = my_cursor = None

    # ------------------------------- constructor
    def __init__(self):
        global my_db, my_cursor
        self.my_host = my_host
        self.my_user = my_user
        self.my_password = my_password
        self.my_database = my_database

    # ------------------------------- run if this delete this object from memory
    def __del__(self):
        try:
            self.close()
        except:
            print('database object is deleted')

    # -------------------------------close databse
    def close(self):
        global my_db, my_cursor
        my_db.commit()
        my_cursor.close()
        my_db.close()

    def connect(self):
        global my_db, my_cursor
        my_db = pymysql.connect(host=self.my_host, user=self.my_user, password=self.my_password,
                                database=self.my_database, cursorclass=pymysql.cursors.DictCursor, charset='utf8mb4')
        my_cursor = my_db.cursor()

    # ------------------------------- execute sql
    def execute(self, _sql):
        global my_db, my_cursor
        try:
            my_cursor.execute(_sql)
        except:
            print('-------------------- : error execute sql')
            print(_sql)
            raise Exception(_sql)

    # ------------------------------- query sql
    def query(self, _sql):
        global my_db
        try:
            return pd.read_sql(_sql, my_db)
        except:
            print('-------------------- : error query sql')
            print(_sql)
            # raise Exception(_sql)

    # ------------------------------- confirm execute
    def commit(self):
        global my_db
        my_db.commit()

    # ------------------------------- create database
    def create_db(self):
        db_local = pymysql.connect(host=self.my_host, user=self.my_user, password=self.my_password,
                                   cursorclass=pymysql.cursors.DictCursor, charset='utf8mb4')
        cursor_local = db_local.cursor()
        cursor_local.execute('CREATE DATABASE IF NOT EXISTS {};'.format(self.my_database))
        cursor_local.close()
        db_local.commit()
        db_local.close()

    # ------------------------------- drop database
    def drop_db(self):
        db_local = pymysql.connect(host=self.my_host, user=self.my_user, password=self.my_password,
                                   cursorclass=pymysql.cursors.DictCursor, charset='utf8mb4')
        cursor_local = db_local.cursor()
        cursor_local.execute('DROP DATABASE IF EXISTS {};'.format(self.my_database))
        cursor_local.close()
        db_local.commit()
        db_local.close()

    # ------------------------------- create table from schema
    def create_table(self, _schema, _tableName):
        s = ",".join(" {} {} ".format(k, v) for k, v in _schema.items())
        sql = "CREATE TABLE IF NOT EXISTS {}  ( {} )".format(_tableName, s)
        self.execute(sql);
        self.commit();

    # ------------------------------- create Foreign key
    def create_FK(self, table_source, fk, table_ref, column_ref):
        sql = "ALTER TABLE {} ADD CONSTRAINT `FK_{}_{}` FOREIGN KEY (`{}`) REFERENCES `{}` (`{}`)" \
              " ON UPDATE NO ACTION ON DELETE NO ACTION;".format(table_source, table_source, table_ref, fk, table_ref,
                                                                 column_ref)
        self.execute(sql);
        self.commit();

    # -------------------------------

    def NAN_NULL(self, o):
        if (type(o).__name__ != 'str' and math.isnan(o)):
            return 'NULL'
        else:
            return "\"{0}\"".format(o)

    # ------------------------------- insert in table
    def insert_in_table(self, _tableName, _tableKeys, _values):
        keys_string = ",".join(str(k) for k in _tableKeys)
        values_string = ",".join(str(self.NAN_NULL(v)) for v in _values)
        sql = "INSERT INTO {} ({}) VALUES ({});".format(_tableName, keys_string, values_string)
        print('------------------------------')
        print(sql)
        self.execute(sql)

    # ------------------------------- data from csv to table
    def populate_table(self, _csv_path, _csv_keys, _tableName, _tableKeys):
        data = pd.read_csv(_csv_path, sep=",", encoding='utf-8-sig', usecols=_csv_keys)
        keys_string = ",".join(str(k) for k in _tableKeys)
        old_time = datetime.now()
        for index, row in data.iterrows():
            values = [self.NAN_NULL(row[k]) for k in _csv_keys]
            values_string = ",".join(str(v) for v in values)
            sql = "INSERT INTO {0} ({1}) VALUES ({2});".format(_tableName, keys_string, values_string)
            self.execute(sql)

            if ((index % 10000) == 0):
                print(_tableName, datetime.now() - old_time, ' load rows : ', str(index))
                # break
        print(_tableName, datetime.now() - old_time, ' end load rows : ', str(index))
        self.commit()

# --------------------------------------------------------------------- function get list files from dir
def FilesYield(_dir_path,_extension):
    for root, dirs, files in os.walk(os.path.abspath(_dir_path)):
        for file in files:
            file = str(file)
            if file.endswith(_extension):
                yield os.path.join(root, file)

# ---------------------------------------------------------------------- function get list files from dir
def getAllFiles(_dir_path,_extension):
    return [f for f in FilesYield(_dir_path,_extension)]

# ----------------------------------------------------------------------
def etl_extract():
    list_csv_path = getAllFiles(dir_weather_data,'csv')
    list_dataframes = []
    for csv_path in list_csv_path:
        print('read:' + csv_path)
        data = pd.read_csv(csv_path, sep=",", encoding='utf-8-sig', low_memory=False)
        list_dataframes.append(data)
    all_datafram = pd.concat(list_dataframes)
    print('save all file in one file')
    all_datafram.to_csv(file_csv_path_all_data, index=False, encoding='utf-8-sig')
    print('all success')

# ----------------------------------------------------------------------
def etl_transformation_station():
    cols = ['STATION', 'NAME', 'LATITUDE', 'LONGITUDE', 'ELEVATION']
    data = pd.read_csv(file_csv_path_all_data, sep=",", encoding='utf-8-sig', usecols=cols, low_memory=False)
    # get all station without duplicate
    all_datafram = data.drop_duplicates(subset=['STATION'])
    # split CITY
    all_datafram.insert(1, 'CITY', all_datafram['NAME'].str.split(',', expand=True)[0])
    # split COUNTRY_CODE
    all_datafram.insert(1, 'COUNTRY_CODE', all_datafram['NAME'].str.split(',', expand=True)[1])
    # split COUNTRY_ISO
    all_datafram.insert(1, 'COUNTRY_ISO',all_datafram['COUNTRY_CODE']\
                        .replace(' AG', 'DZ')\
                        .replace(' TS', 'TN')\
                        .replace(' MO','MA')\
                        .replace(' SP', 'MA'))
    # extract COUNTRY_ISO
    all_datafram.insert(1, 'COUNTRY_ISO3',all_datafram['COUNTRY_CODE']\
                        .replace(' AG', 'DZA')\
                        .replace(' TS', 'TUN')\
                        .replace(' MO','MAR')\
                        .replace(' SP', 'MAR'))

    # # extract COUNTRY_NUMBER
    all_datafram.insert(1, 'COUNTRY_NUMBER',all_datafram['COUNTRY_CODE']\
                        .replace(' AG', '012')\
                        .replace(' TS', '788') \
                        .replace(' MO', '504') \
                        .replace(' SP', '504'))
    # extract COUNTRY_NAME
    all_datafram.insert(1, 'COUNTRY_NAME',all_datafram['COUNTRY_CODE']\
                        .replace(' AG', 'Algeria')\
                        .replace(' TS', 'Tunisia') \
                        .replace(' MO', 'Morocco') \
                        .replace(' SP', 'Morocco'))
    # save file
    all_datafram.to_csv(file_csv_path_station, index=False, encoding='utf-8-sig')


# ----------------------------------------------------------------------
def etl_transformation_weather():
    cols = ['STATION', 'DATE', 'PRCP', 'TAVG', 'TMAX', 'TMIN', 'SNWD', 'PGTM', 'SNOW', 'WDFG', 'WSFG']
    cols = cols + ['WT01','WT02','WT03','WT05','WT07','WT08','WT09','WT16','WT18'] # wtXX
    data = pd.read_csv(file_csv_path_all_data, sep=",", encoding='utf-8-sig', usecols=cols, low_memory=False)
    # replae NAN values in colun PRCP by mean
    data['PRCP'].fillna(0, inplace=True)
    # replae NAN values in colun TMAX by mean
    data['TMAX'].fillna(data['TMAX'].mean(), inplace=True)
    # replae NAN values in colun TMIN by mean
    data['TMIN'].fillna(data['TMIN'].mean(), inplace=True)
    # replae NAN values in colun TAVG by TMAX+TMIN /2
    data['TAVG'].fillna(data['TMAX'] + data['TMIN'] / 2, inplace=True)
    data.insert(1, 'Date_ID', data['DATE'].str.replace('-', ''))
    all_datafram = data.round(1)

    # save file
    all_datafram.to_csv(file_csv_path_weather, index=False, encoding='utf-8-sig')

# ----------------------------------------------------------------------
def create_datawarehows():
    db = Database()
    db.drop_db()
    db.create_db()
    db.connect()

    db.create_table(schema_table_station_dim,'station_dim')
    db.create_table(schema_table_weather_fact,'weather_fact')
    db.create_table(schema_table_date_dim,'date_dim')

    db.create_FK('weather_fact','STATION_ID','station_dim','STATION_ID')
    db.create_FK('weather_fact','DATE_ID','date_dim','DATE_ID')

# ----------------------------------------------------------------------
def etl_load():
    db = Database()
    db.connect()

    # ---------------------------
    csv_keys_station_dim = ['STATION', 'CITY', 'COUNTRY_NAME', 'COUNTRY_ISO3', 'COUNTRY_NUMBER', 'LATITUDE',
                            'LONGITUDE', 'ELEVATION']
    table_keys_station_dim = list(schema_table_station_dim.keys())[:-1]
    db.populate_table(file_csv_path_station, csv_keys_station_dim, 'station_dim', table_keys_station_dim)
    # ---------------------------
    csv_keys_date_dim = list(schema_table_date_dim.keys())[:-1]
    table_keys_date_dim = list(schema_table_date_dim.keys())[:-1]

    db.populate_table(file_csv_path_date, csv_keys_date_dim, 'date_dim', table_keys_date_dim)
    # ---------------------------
    csv_keys_weather_fact = ['STATION', 'Date_ID', 'PRCP', 'TAVG', 'TMAX', 'TMIN', 'SNWD', 'PGTM', 'SNOW', 'WDFG','WSFG']
    csv_keys_weather_fact = csv_keys_weather_fact + ['WT01', 'WT02', 'WT03', 'WT05', 'WT07', 'WT08', 'WT09', 'WT16','WT18']  # wtXX
    table_keys_weather_fact = list(schema_table_weather_fact.keys())[:-1]

    db.populate_table(file_csv_path_weather, csv_keys_weather_fact, 'weather_fact', table_keys_weather_fact)
    # ---------------------------
    db.close()

# ---------------------------------------------------------------------- create file csv of cube multidimensional
def create_multidimensional_cube():
    db = Database()
    db.connect()
    db.execute("SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));")
    db.commit()

    sql = 'SELECT station_dim.*,date_dim.Year,date_dim.Season, ' \
          'ROUND(AVG(weather_fact.PRCP),2) as AVG_PRCP , ' \
          'ROUND(AVG(weather_fact.TAVG),2) as AVG_TAVG , ' \
          'ROUND(AVG(weather_fact.TMAX),2) as AVG_TMAX , ' \
          'ROUND(AVG(weather_fact.TMIN),2) as AVG_TMIN   ' \
          'FROM weather_fact,station_dim ,date_dim ' \
          'WHERE weather_fact.STATION_ID = station_dim.STATION_ID ' \
          'AND weather_fact.Date_ID = date_dim.Date_ID ' \
          'group BY weather_fact.STATION_ID ,date_dim.Year,date_dim.Season;'

    df = db.query(sql)
    db.close()

    print('cube : size :', df.size)
    print('cube : columns :', df.columns)
    df.to_csv(file_csv_path_cube_data, index=False, encoding='utf-8-sig')

# ---------------------------------------------------------------------- main run all functions
etl_extract();
etl_transformation_station()
etl_transformation_weather()
create_datawarehows()
etl_load()
create_multidimensional_cube()
