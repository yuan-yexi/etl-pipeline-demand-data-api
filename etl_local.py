import mysql.connector as mysql
import requests
import pandas as pd
import json
from datetime import datetime
from sqlalchemy import create_engine

from decouple import config

import configuration

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

def get_monthly_demand(start_end_dates, markets_iso, metric_type, interval):
    url = f"https://api.parrotanalytics.com/latest/demand?date_from={start_end_dates[0]}&date_to={start_end_dates[1]}&markets_iso={markets_iso}&content_ids=subscriptionshows&metric_type={metric_type}&interval={interval}"

    payload = {}
    headers = {'x-api-key': APIKEY}

    response = requests.get(url, headers=headers, data=payload)
    return json.loads(response.content)

def create_dataframe(data):
    label = []
    parrot_id = []
    dex = []
    overall_rank = []
    rank_by_peak = []

    num_of_records = data['data'][0]['records_count']
    datapoints = data['data'][0]['datapoints']

    for show in range(0, num_of_records):
        label.append(datapoints[show]['label'])
        parrot_id.append(datapoints[show]['parrot_id'])
        dex.append(datapoints[show]['value'])
        overall_rank.append(datapoints[show]['overall_rank'])
        rank_by_peak.append(datapoints[show]['rank_by_peak'])

    return pd.DataFrame(
        {
            'label': label,
            'parrot_id': parrot_id,
            'dex': dex,
            'overall_rank': overall_rank,
            'rank_by_peak': rank_by_peak
        }
    )

def generate_csv(dataframe, market, date):
    retrieve_date = datetime.today().strftime('%Y-%m-%d')
    filename = f'./data/exports/{date}_{market}_content_demand_ret_on_{retrieve_date}.csv'
    dataframe.to_csv(filename, index=False)


def generate_monthly_date_range(start, end):
    start_date = start
    end_date = end
    
    start_dates = pd.date_range(start=start_date, end=end_date, freq='MS').strftime("%Y-%m-%d").tolist()
    end_dates = pd.date_range(start=start_date, end=end_date, freq='M').strftime("%Y-%m-%d").tolist()
    
    monthly_dates = []

    for (sd, ed) in zip(start_dates, end_dates):
        monthly_dates.append([sd, ed])
    
    return monthly_dates

def insert_into_mysql(data, date, market, table_name, engine):
    data['market'] = market
    data['date'] = date
    data['id'] = data['date'] + "-" + data['market'] + "-" + data['parrot_id']
    data['date'] = pd.to_datetime(data['date'])
    print('Inserting Dataframe')
    data.to_sql(name=table_name, con=engine, if_exists="append", index=False)
    print("Done")

def etl(start_date, end_date, markets, metric_type, interval, table_name, engine):
    monthly_dates = generate_monthly_date_range(start_date, end_date)

    for market in markets:
        for monthly_date in monthly_dates:
            resp = get_monthly_demand(monthly_date, market, metric_type, interval)
            print("API Requested:" + " " + f"{market}" + " " + f"{monthly_date}")
            data = create_dataframe(resp)
            
            generate_csv(data, market, monthly_date[0])

            insert_into_mysql(data, monthly_date[0], market, table_name, engine)

if __name__ == "__main__":
    etl(configuration.START_DATE, configuration.END_DATE, configuration.MARKETS, configuration.METRIC_TYPE, configuration.INTERVAL, "monthly_demand", engine)