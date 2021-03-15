import mysql.connector as mysql
import pandas as pd
from nltk.corpus import stopwords  
from nltk.tokenize import word_tokenize

from sqlalchemy import create_engine

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

engine = create_engine(f"mysql+mysqlconnector://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}")

data = pd.read_csv('./data/master_catalog.csv')

content_type = []

for index, row in data.iterrows():
    if row['main_genre'] in ['Action and Adventure', 'Horror', 'Animation', 'Drama', 'Children', 'Comedy']:
        content_type.append('Scripted')
    elif row['main_genre'] in ['Sports', 'Variety', 'Reality', 'Documentary', 'Factual']:
        content_type.append('Unscripted')
    else:
        content_type.append('None')

data['type'] = content_type

stop = stopwords.words('english')

data['short_description'] = data['short_description'].astype('string')
data['short_description'].fillna('a', inplace=True)

data['short_description_without_stopwords'] = data['short_description'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop)]))

data['title'] = data['title'].astype('string')
data['display_name'] = data['display_name'].astype('string')
data['parrot_id'] = data['parrot_id'].astype('string')
data['short_id'] = data['short_id'].astype('string')
data['tms_series_id'] = data['tms_series_id'].astype('string')

print(engine)
print(data.shape)
data.to_sql(name='master_catalog', con=engine, if_exists="append", index=False, chunksize=5000)

print("Done")
