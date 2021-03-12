# python-mysql-parrot-analytics

ETL pipeline to call Parrot Analytics monthly demand data from API and INSERT INTO Mysql DB

Use configuration.py to configure settings

START_DATE = '2020-01-01' -> start date of data query

END_DATE = '2020-12-31' -> end date of data query

MARKETS = ['ID', 'PH'] -> markets to query

METRIC_TYPE = 'dexpercapita' -> metric type (dex / dexpercapita)

INTERVAL = 'overall' -> daily data or average of date range (daily / overall)
