import requests, json, os, logging
import ta.trend, ta.momentum
from alice_blue import *
import dateutil.parser
from datetime import datetime, timedelta
import pandas as pd

#logging.basicConfig(level=logging.DEBUG)  # Optional for getting debug messages.

# Config
idx = os.environ.get('idx') #BNF, NF
#country = 'india'
#product_type = 'index'
alice_user_id = os.environ.get('alice_user_id')
alice_password = os.environ.get('alice_password')
alice_two_FA = os.environ.get('alice_two_FA')
alice_api_secret = os.environ.get('alice_api_secret')
alice_app_id = os.environ.get('alice_app_id')

print(f'user_id = {alice_user_id}, password = {alice_password}, two_FA = {alice_two_FA}, api_secret = {alice_api_secret}, app_id = {alice_app_id}')

def get_historical(instrument, from_datetime, to_datetime, interval, indices=False):
    params = {"token": instrument.token,
              "exchange": instrument.exchange if not indices else "NSE_INDICES",
              "starttime": str(int(from_datetime.timestamp())),
              "endtime": str(int(to_datetime.timestamp())),
              "candletype": 3 if interval.upper() == "DAY" else (2 if interval.upper().split("_")[1] == "HR" else 1),
              "data_duration": None if interval.upper() == "DAY" else interval.split("_")[0]}
    lst = requests.get(
        f" https://ant.aliceblueonline.com/api/v1/charts/tdv?", params=params).json()["data"]["candles"]
    records = []
    for i in lst:
        record = {"date": dateutil.parser.parse(i[0]), "Open": i[1], "High": i[2], "Low": i[3], "Close": i[4], "Volume": i[5]}
        records.append(record)
    return records

def getAliceSignal(aliceSymbol,tFrame):
    access_token = AliceBlue.login_and_get_access_token(username=alice_user_id,
                                                        password=alice_password,
                                                        twoFA=alice_two_FA,
                                                        api_secret=alice_api_secret,
                                                        app_id=alice_app_id)
    alice = AliceBlue(username=alice_user_id, password=alice_password,
                      access_token=access_token)

    #print(alice.get_balance()) # get balance / margin limits
    #print(alice.get_profile()) # get profile
    #print(alice.get_daywise_positions()) # get daywise positions
    #print(alice.get_netwise_positions()) # get netwise positions
    #print(alice.get_holding_positions()) # get holding positions
    #instrument = alice.get_instrument_by_symbol("NSE", "Nifty 50")
    #instrument = alice.get_instrument_by_symbol("NSE", "Nifty Bank")
    instrument = alice.get_instrument_by_symbol("NSE", aliceSymbol)
    from_datetime = datetime.now() - timedelta(days=30)
    to_datetime = datetime.now()
    interval = tFrame   # ["DAY", "1_HR", "3_HR", "1_MIN", "5_MIN", "15_MIN", "60_MIN"]
    indices = True
    df = pd.DataFrame(get_historical(instrument, from_datetime, to_datetime, interval, indices))

    df.index = df["date"]
    df = df.drop("date", axis=1)
    #print(f'display dataframe: {df}')
    #df = dropna(df)
    df['ema21'] = ta.trend.ema_indicator(df['Close'], 21)
    df['macd'] = ta.trend.macd(df['Close'])
    df['macdsignal'] = ta.trend.macd_signal(df['Close'])
    df['macddiff'] = ta.trend.macd_diff(df['Close'])
    df['rsi'] = ta.momentum.rsi(df['Close'])
    #print(df)
    return df

'''
class getTechIndicators():

    global country
    global product_type
    global idx
    global name

    idx = 'BNF'
    if (idx == 'BNF'):
        name = 'nifty bank'
    elif (idx == 'NF'):
        name = 'nifty 50'
    else:
        name = 'NONE'

    #indices = investpy.indices.get_indices(country)
    #print(indices)

    def __init__(self, symbol, tf, macd=0):
        self.symbol = symbol
        self.tf = tf
        self.macd = macd


    def getMACDRSI(self):
        idx_name = name
        tmframe = self.tf
        #print(f"{idx_name},{country},{product_type},{tmframe}")
        data = investpy.technical.technical_indicators(idx_name, country, product_type, tmframe)
        #print(data)
        #df_macd['macdsig'] = ta.trend.ema_indicator(close=df_macd['close'], window=9)
        data_json = data.to_json()
        data_macd = json.loads(data_json)['value']['3']
        data_rsi = json.loads(data_json)['value']['0']
        return (data_macd, data_rsi)

    def getEMA1020(self):
        idx_name = name
        tmframe = self.tf
        data = investpy.technical.moving_averages(idx_name, country, product_type, tmframe)
        #print(data)
        data_json = data.to_json()
        data_ema10 = json.loads(data_json)['ema_value']['1']
        data_ema20 = json.loads(data_json)['ema_value']['2']
        return (data_ema10, data_ema20)


    def getMACDSignal(self):
        df_macd = pd.DataFrame({'close':[self.macd]})
        print(df_macd)
        df_macd['macdsig'] = ta.trend.ema_indicator(df_macd['close'], 9)
        print(df_macd)
        macdSignal = df_macd['macdsig']
        print(f'macdSingal: {macdSignal}')
        return macdSignal
'''

#techInd = getTechIndicators('NIFTY', "15mins")
#techInd.getMACDSignal()
