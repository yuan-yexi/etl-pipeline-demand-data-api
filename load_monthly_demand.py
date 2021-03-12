import mysql.connector as mysql
import pandas as pd
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

insert_into_table = """INSERT INTO monthly_demand (id, parrot_id, label, dex, overall_rank, rank_by_peak, date, market) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""

filenames = ['./data/data_jp_cleaned_2020.csv', './data/data_in_cleaned_2020.csv']
markets = ['JP', 'IN']

for filename, market in zip(filenames, markets):
    data = pd.read_csv(filename)

    data['market'] = market
    data['id'] = data['date'] + "-" + data['market'] + "-" + data['parrot_id']
    data['date'] = pd.to_datetime(data['date'])
    
    for index, row in data.iterrows():
        value = (
            row['id'],
            row['parrot_id'],
            row['label'],
            row['dex'],
            row['overall_rank'],
            row['rank_by_peak'],
            row['date'],
            row['market']
        )
        print(row['label'], row['dex'])
        cursor.execute(insert_into_table, value)

db.commit()
cursor.close()
print("Done")