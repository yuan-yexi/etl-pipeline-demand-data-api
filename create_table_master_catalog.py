import mysql.connector as mysql
import csv
from decouple import config

DB_USERNAME = config('USER')
DB_PASSWORD = config('PASSWORD')
DB_HOST = config('HOST')
DB_PORT = config('PORT')
DB_DATABASE = config('DATABASE')
APIKEY = config('APIKEY')

db = mysql.connect(
    host = DB_HOST,
    user = DB_USERNAME,
    port = DB_PORT,
    database = DB_DATABASE,
    passwd = DB_PASSWORD
)
print(db)
cursor = db.cursor()

create_table_master_catalog = """
    USE parrot_analytics;
    DROP TABLE IF EXISTS master_catalog;
    CREATE TABLE master_catalog (
        title VARCHAR(255) DEFAULT NULL,
        display_name VARCHAR(255) DEFAULT NULL,
        parrot_id VARCHAR(255) NOT NULL,
        short_id VARCHAR(255) DEFAULT NULL,
        catalog_state VARCHAR(255) DEFAULT NULL,
        content_type VARCHAR(255) DEFAULT NULL,
        content_sub_type VARCHAR(255) DEFAULT NULL,
        original_language VARCHAR(255) DEFAULT NULL,
        onboarded_on VARCHAR(255) DEFAULT NULL,
        ratings_available_from VARCHAR(255) DEFAULT NULL,
        tms_series_id VARCHAR(255) DEFAULT NULL,
        release_year INT(10) DEFAULT NULL,
        main_genre VARCHAR(255) DEFAULT NULL,
        sub_genre VARCHAR(255) DEFAULT NULL,
        start_date VARCHAR(255) DEFAULT NULL,
        end_date VARCHAR(255) DEFAULT NULL,
        total_seasons INT(10)DEFAULT NULL,
        total_episodes INT(10) DEFAULT NULL,
        short_description TEXT DEFAULT NULL,
        countries VARCHAR(255) DEFAULT NULL,
        networks VARCHAR(255) DEFAULT NULL,
        imdb_id VARCHAR(255) DEFAULT NULL,
        created_on VARCHAR(255) DEFAULT NULL,
        updated_on VARCHAR(255) DEFAULT NULL,
        PRIMARY KEY (parrot_id)
    );
    """

cursor.execute(create_table_master_catalog, multi=True)
cursor.close()
print("Done")
