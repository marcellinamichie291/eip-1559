# import ccxt
# import datetime 
# import logging

# ####### EXTRACT FUNCTIONS #######
# # Exchange instantiation
# binance = ccxt.binance()

# # Loading markets
# binance.load_markets()

# def get_ohlcv_data(symbol, limit, start_date, timeframe='1m'):
#     """
#     This uses CCXT to fetch raw ohlcv data from the exchange
#     """

#     try:
#         price_data = binance.fetch_ohlcv(symbol, timeframe, limit=limit, since=start_date)
#         return price_data
#     except Exception as e:
#         logging.info(e)
#     return

# #limit = 
# start_date = datetime.datetime.fromisoformat('2021-08-05 12:33:00')
# start_date = int((start_date - datetime.datetime(1970, 1, 1)).total_seconds()) * 1000 # utc time
# end_date = datetime.datetime.fromisoformat('2022-08-05 12:33:00')
# end_date = int((end_date - datetime.datetime(1970, 1, 1)).total_seconds()) * 1000 # utc time
# results = get_ohlcv_data('ETH/USDT', start_date=start_date, limit=end_date)
# print(results[0])


#max_time = get_last_timestamp('Binance')
#max_time = max_time + datetime.timedelta(minutes=1)
#max_time_int = int(datetime.datetime(max_time.year, max_time.month, max_time.day, max_time.hour, max_time.minute).timestamp() * 1000)
#now = datetime.datetime.now().replace(second=0, microsecond=0)
#limit = int((now - max_time).total_seconds() / 60) + 1
#JOBS_META_DATA = load_pools('Binance')
#    
#for job in JOBS_META_DATA:
#    try:
#        price_data = get_ohlcv_data(job['symbol'], limit, max_time_int)


import ccxt
import datetime
import pytz
import pandas as pd

def get_ohlcv_data(symbol, start, array_price, timeframe='1m'):
    """
    This uses CCXT to fetch raw ohlcv data from the exchange
    """
    try:
        price_data = binance.fetch_ohlcv(symbol, timeframe, start, limit=1000)
        inserted = len(price_data)
        price_data = array_price + price_data
        return price_data, inserted
    except Exception as e:
        print(e)
    return 





# Exchange instantiation
binance = ccxt.binance()

# Loading markets
binance.load_markets()

lp = 'ETH/USDT'
ohlcv = []
start_date = datetime.datetime(2021, 8, 5, 12, 33, tzinfo = pytz.utc)
end_date = datetime.datetime(2022, 8, 5, 12, 33, tzinfo = pytz.utc)
difference = end_date - start_date
print(difference)
start_date_int = binance.parse8601(str(start_date))
while start_date <= end_date:
    
    print(start_date)
    print(lp)
    ohlcv, minutes_added = get_ohlcv_data(symbol=lp, start=start_date_int, array_price = ohlcv)

    start_date = start_date + datetime.timedelta(minutes=minutes_added)
    if (start_date > end_date):
        start_date = start_date + datetime.timedelta(minutes=1)

    start_date_int = binance.parse8601(str(start_date))

eth_price = pd.DataFrame(columns = ['datetime', 'eth_price'])
for row in ohlcv:
    eth_price = eth_price.append({'datetime': datetime.datetime.utcfromtimestamp(row[0] / 1e3), 'eth_price': row[4]}, ignore_index=True)

print(eth_price)
eth_price.to_csv('eth_price.csv')


