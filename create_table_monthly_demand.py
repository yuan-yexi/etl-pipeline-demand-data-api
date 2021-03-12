import mysql.connector as mysql
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

create_table_monthly_demand = """
    USE parrot_analytics;
    DROP TABLE IF EXISTS monthly_demand;
    CREATE TABLE monthly_demand (
        id VARCHAR(500) NOT NULL,
        parrot_id VARCHAR(255) DEFAULT NULL,
        label VARCHAR(255) DEFAULT NULL,
        dex FLOAT DEFAULT NULL,
        overall_rank INT DEFAULT NULL,
        rank_by_peak INT DEFAULT NULL,
        date DATE DEFAULT NULL,
        market VARCHAR(10) DEFAULT NULL,
        PRIMARY KEY (id)
    );
    """

cursor.execute(create_table_monthly_demand)
