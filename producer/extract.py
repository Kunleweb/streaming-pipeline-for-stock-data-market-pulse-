import requests
from config import logger, headers, url


## fetch data from api 
def connect_to_api():
    stocks = ['MSFT', 'TSLA', 'AAPL', 'GOOGL']
    json_response = []
    for stock in range(0, len(stocks)):
        querystring = {"function":"TIME_SERIES_INTRADAY",
               "symbol":f"{stocks[stock]}","outputsize":"compact",
               "interval": "5min", "datatype":"json"}
        try:
            response = requests.get(url, headers=headers, params=querystring)
            response.raise_for_status()
            data = response.json()
            logger.info("Stocks successfully loaded")
            json_response.append(data)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error on stock: {e}")
            break 



## extract particular data from api response 
def extract_json(response):
    records = []
    for data in response:
        symbol = data['Meta Data']['2. Symbol']
        for date_str, metrics in data['Time Series (5min)'].items():
            record = { "symbol": symbol, "date": date_str, "open": metrics["1. open"], "close":
                      metrics["2. high"], "low": metrics["3. low"]}
            records.append(record)
    return records
