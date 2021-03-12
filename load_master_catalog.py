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

## Verify connection
print(db)

## Initialize cursor
cursor = db.cursor()

## INSERT INTO SQL statement
insert_into_table = """
        INSERT INTO master_catalog (title, display_name, parrot_id, short_id, catalog_state, content_type, content_sub_type, original_language, onboarded_on, ratings_available_from, tms_series_id, release_year, main_genre, sub_genre, start_date, end_date, total_seasons, total_episodes, short_description, countries, networks, imdb_id, created_on, updated_on) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

# Open and load CSV into master_catalog table
with open('./data/master_catalog.csv', encoding='utf-8') as master_catalog_csv:
    reader = csv.DictReader(master_catalog_csv)
    for row in reader:
        value = (f"{row['title']}",
        f"{row['display_name']}",
        f"{row['parrot_id']}",
        f"{row['short_id']}",
        f"{row['catalog_state']}",
        f"{row['content_type']}",
        f"{row['content_sub_type']}",
        f"{row['original_language']}",
        f"{row['onboarded_on']}",
        f"{row['ratings_available_from']}",
        f"{row['tms_series_id']}",
        row['release_year'],
        f"{row['main_genre']}",
        f"{row['sub_genre']}",
        f"{row['start_date']}",
        f"{row['end_date']}",
        row['total_seasons'],
        row['total_episodes'],
        f"{row['short_description']}",
        f"{row['countries']}",
        f"{row['networks']}",
        f"{row['imdb_id']}",
        f"{row['created_on']}",
        f"{row['updated_on']}")
        cursor.execute(insert_into_table, value)
        print(value)

db.commit()
cursor.close()
print("Done")