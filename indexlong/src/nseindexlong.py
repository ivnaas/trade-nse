import logging, os
from datetime import datetime, date, timedelta
import math, time, os, logging
from smartapi import SmartConnect
import requests
import pandas as pd
from dateutil.relativedelta import relativedelta, TH
from techind import *
from ta import add_all_ta_features
from ta.utils import dropna
# Config

username = os.environ.get('username')
password = os.environ.get('password')
api_key = os.environ.get('api_key')
idx = os.environ.get('idx') #BNF, NF
qty = float(os.environ.get('lot_size'))
qty_2 = round((qty/2),2)
instrument = os.environ.get('instrument') #OPT, FUT
stoploss = float(os.environ.get('stoploss')) #BNF-100, NF-50
hist = float(os.environ.get('hist')) #BNF > 20, NF > 10
TF_15MIN = "FIFTEEN_MINUTE"
TF_5MIN = "FIVE_MINUTE"

AL_15MIN = "15_MIN"
AL_5MIN = "5_MIN"

#logging.basicConfig(level=logging.DEBUG)  # Optional for getting debug messages.
#create logger
logger = logging.getLogger(__name__)
#set log level
logger.setLevel(logging.INFO)

orderLogger = logging.getLogger('orderlog')
orderLogger.setLevel(logging.WARNING)

#define file handler and set formatter
logFilePath = "./logs"
logFilePersist = "./logs/persistant"
logFileName = logFilePath + '/' + idx + 'short.log'
if(not os.path.exists(logFilePath)):
    os.makedirs(logFilePath)
if(not os.path.exists(logFilePersist)):
    os.makedirs(logFilePersist)
orderFileName = logFilePersist + '/' + idx + 'order.log'

file_handler = logging.FileHandler(logFileName)
order_handler = logging.FileHandler(orderFileName)
formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)
order_handler.setFormatter(formatter)

logger.addHandler(file_handler)
orderLogger.addHandler(order_handler)

orderLogger.warning(f'**Initalize Short Orderlog**')

# Config
angelObject = None
#def exitPos(entryPrice):
    #to do

def getTokens():
    url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
    d = requests.get(url).json()
    token_df = pd.DataFrame.from_dict(d)
    token_df['expiry'] = pd.to_datetime(token_df['expiry']).apply(lambda x: x.date())
    token_df = token_df.astype({'strike': float})
    return(token_df)

def nextThu_and_lastThu_expiry_date():

    todayte = datetime.today()

    cmon = todayte.month
    if_month_next = (todayte + relativedelta(weekday=TH(1))).month
    next_thursday_expiry = todayte + relativedelta(weekday=TH(1))

    if (if_month_next != cmon):
        month_last_thu_expiry = todayte + relativedelta(weekday=TH(5))
        if (month_last_thu_expiry.month != if_month_next):
            month_last_thu_expiry = todayte + relativedelta(weekday=TH(4))
    else:
        for i in range(1, 7):
            t = todayte + relativedelta(weekday=TH(i))
            if t.month != cmon:
                # since t is exceeded we need last one  which we can get by subtracting -2 since it is already a Thursday.
                t = t + relativedelta(weekday=TH(-2))
                month_last_thu_expiry = t
                break
    #print("month_last_thu_expiry:", month_last_thu_expiry)
    #print("next_thursday_expiry:", next_thursday_expiry)
    wkly_expiry_dt = int(next_thursday_expiry.strftime("%d"))
    wkly_expiry_month = int(next_thursday_expiry.strftime("%m"))
    monthly_expiry_dt = int(month_last_thu_expiry.strftime("%d"))
    monthly_expiry_month = int(month_last_thu_expiry.strftime("%m"))
    #print("weekly expiry date", wkly_expiry_dt)
    #print("weekly expiry month", wkly_expiry_month)
    #print("weekly expiry date", monthly_expiry_dt)
    #print("weekly expiry month", monthly_expiry_month)

    str_month_last_thu_expiry = str(int(month_last_thu_expiry.strftime("%d"))) + month_last_thu_expiry.strftime(
        "%b").upper() + month_last_thu_expiry.strftime("%Y")
    str_next_thursday_expiry = str(int(next_thursday_expiry.strftime("%d"))) + next_thursday_expiry.strftime(
        "%b").upper() + next_thursday_expiry.strftime("%Y")
    return (str_next_thursday_expiry, str_month_last_thu_expiry,wkly_expiry_dt,wkly_expiry_month,monthly_expiry_dt,monthly_expiry_month)

def getTokenInfo (symbol,exch_seg ='NSE',instrumenttype='OPTIDX',strike_price = '',pe_ce = 'PE',expiry_day = None):
    df = getTokens()
    #print("inside getTokenInfo", df)
    strike_price = strike_price*100
    if exch_seg == 'NSE':
        eq_df = df[(df['exch_seg'] == 'NSE') ]
        return eq_df[eq_df['name'] == symbol]
    elif exch_seg == 'NFO' and ((instrumenttype == 'FUTSTK') or (instrumenttype == 'FUTIDX')):
        return df[(df['exch_seg'] == 'NFO') & (df['instrumenttype'] == instrumenttype) & (df['name'] == symbol)].sort_values(by=['expiry'])
    elif exch_seg == 'NFO' and (instrumenttype == 'OPTSTK' or instrumenttype == 'OPTIDX'):
        return df[(df['exch_seg'] == 'NFO') & (df['expiry']==expiry_day) &  (df['instrumenttype'] == instrumenttype) & (df['name'] == symbol) & (df['strike'] == strike_price) & (df['symbol'].str.endswith(pe_ce))].sort_values(by=['expiry'])
    #expiry_day = date(2022,7,28)

def getIndexPrice(symbol):
    #symbol = 'BANKNIFTY'
    tokenInfo = getTokenInfo(symbol)
    #print(tokenInfo)
    spot_token = tokenInfo.iloc[0]['token']
    #print(spot_token)
    ltpInfo = angelObject.ltpData('NSE', symbol, spot_token)
    indexLtp = ltpInfo['data']['ltp']
    #print(indexLtp)
    return (spot_token, indexLtp)

def getATMStrike(indexPrice):
    rndval =  indexPrice % 100
    print(rndval)
    if rndval > 50:
        ATMStrike = math.ceil(indexPrice / 100) * 100
    else:
        ATMStrike = math.floor(indexPrice / 100) * 100
    return ATMStrike

def getHistoricalAPI(token,interval= 'FIVE_MINUTE'):
    to_date = datetime.now()
    from_date = to_date - timedelta(days=30)
    from_date_format = from_date.strftime("%Y-%m-%d %H:%M")
    to_date_format = to_date.strftime("%Y-%m-%d %H:%M")
    try:
        historicParam={
        "exchange": "NSE",
        "symboltoken": token,
        "interval": interval,
        "fromdate": from_date_format,
        "todate": to_date_format
        }
        candle_json = angelObject.getCandleData(historicParam)
        print(candle_json)
        return candle_json
    except Exception as e:
        print("Historic Api failed: {}".format(e.message))


def placeShortOrder(symbol,ATMStrike):

    str_next_thursday_expiry, str_month_last_thu_expiry, wkly_expiry_dt, wkly_expiry_month, monthly_expiry_dt, monthly_expiry_month = nextThu_and_lastThu_expiry_date()
    #print("================================================================================")
    #print("Next Expiry Date = " + str_next_thursday_expiry)
    #print("================================================================================")
    #print("Month End Expiry Date = " + str_month_last_thu_expiry)
    #print("================================================================================")
    wkly_mon = wkly_expiry_month
    wkly_dt = wkly_expiry_dt
    optSellWkly_dt = wkly_dt + 7
    optSellStrike = ATMStrike + ATMdiff

    marginBuyWkly_dt = wkly_dt
    marginBuyStrike = ATMStrike + marginDiff

    expiry_day = date(2022, wkly_mon, marginBuyWkly_dt)
    ce_tokeninfo = getTokenInfo(symbol, 'NFO', 'OPTIDX', marginBuyStrike, 'PE', expiry_day)
    print(ce_tokeninfo)
    ce_strike_token = ce_tokeninfo.iloc[0]['token']
    ce_strike_symbol = ce_tokeninfo.iloc[0]['symbol']
    ltp = angelObject.ltpData("NFO",ce_strike_symbol,ce_strike_token)['data']['ltp']
    print(f"{ce_strike_symbol} Price is: {ltp}")
    buy_order(ce_strike_symbol,symbol,qty)

    expiry_day = date(2022, wkly_mon, optSellWkly_dt)
    ce_tokeninfo = getTokenInfo(symbol, 'NFO', 'OPTIDX', optSellStrike, 'PE', expiry_day)
    print(ce_tokeninfo)
    ce_strike_symbol = ce_tokeninfo.iloc[0]['token']
    print(ce_strike_symbol)
    ce_strike_token = ce_tokeninfo.iloc[0]['token']
    ce_strike_symbol = ce_tokeninfo.iloc[0]['symbol']
    ltp = angelObject.ltpData("NFO", ce_strike_symbol, ce_strike_token)['data']['ltp']
    print(f"{ce_strike_symbol} Price is: {ltp}")
    sell_order(ce_strike_symbol,symbol,qty)

def buy_order(token,symbol,qty):
    try:
        orderparams = {
            "variety": "NORMAL",
            "tradingsymbol": symbol,
            "symboltoken": token,
            "transactiontype": "BUY",
            "exchange": "NFO",
            "ordertype": "MARKET",
            "producttype": "CARRYFORWARD", #INTRADAY, CARRYFORWARD
            "duration": "DAY",
            "price": "0",
            "squareoff": "0",
            "stoploss": "0",
            "quantity": qty,
            "triggerprice": "0"
            }
        print(orderparams)
        #orderId=angelObject.placeOrder(orderparams)
        #print("The order id is: {}".format(orderId))
    except Exception as e:
        print("Order placement failed: {}".format(e.message))

def sell_order(token,symbol,qty):
    try:
        orderparams = {
            "variety": "NORMAL",
            "tradingsymbol": symbol,
            "symboltoken": token,
            "transactiontype": "SELL",
            "exchange": "NFO",
            "ordertype": "MARKET",
            "producttype": "CARRYFORWARD", #INTRADAY, CARRYFORWARD
            "duration": "DAY",
            "price": "0",
            "squareoff": "0",
            "stoploss": "0",
            "quantity": qty,
            "triggerprice": "0"
            }
        print(orderparams)
        orderId=angelObject.placeOrder(orderparams)
        print("The order id is: {}".format(orderId))
        logger.info(f"The order id is: {.format(orderId)}")
    except Exception as e:
        print("Order placement failed: {}".format(e.message))

def exitPos(entryPrice):

    alice_df15min = getAliceSignal(aliceSymbol, AL_15MIN)
    # print(alice_df15min)
    latest_candle = alice_df15min.iloc[-1]

    ema21_15min = latest_candle['ema21']
    macdVal_15min = latest_candle['macd']
    macdSignal_15min = latest_candle['macdsignal']
    macdDiff_15min = latest_candle['macddiff']
    rsi_15min = latest_candle['rsi']
    close_15min = latest_candle['Close']

    # print(f"EMA21 Value - {AL_15MIN} is {ema21_15min}")
    # print(f"MACD Value - {AL_15MIN} is {macdVal_15min}")
    # print(f"MACD Signal Value - {AL_15MIN} is {macdSignal_15min}")
    # print(f"MACD diff - {AL_15MIN} is {macdDiff_15min}")
    # print(f"RSI Value - {AL_15MIN} is {rsi_15min}")
    # print(f"Close Value - {AL_15MIN} is {close_15min}")

    alice_df5min = getAliceSignal(aliceSymbol, AL_5MIN)
    # print(alice_df5min)
    latest_candle = alice_df5min.iloc[-1]

    ema21_5min = latest_candle['ema21']
    macdVal_5min = latest_candle['macd']
    macdSignal_5min = latest_candle['macdsignal']
    macdDiff_5min = float(latest_candle['macddiff'])
    rsi_5min = latest_candle['rsi']
    close_5min = latest_candle['Close']

    # print(f"EMA21 Value - {AL_5MIN} is {ema21_5min}")
    # print(f"MACD Value - {AL_5MIN} is {macdVal_5min}")
    # print(f"MACD Signal Value - {AL_5MIN} is {macdSignal_5min}")
    # print(f"MACD diff - {AL_5MIN} is {macdDiff_5min}")
    # print(f"RSI Value - {AL_5MIN} is {rsi_5min}")
    # print(f"Close Value - {AL_5MIN} is {close_5min}")
    halfExit = 0
    while True:
        takeProfit = entryPrice - (stoploss*4)
        exitPrice = ema21_5min + stoploss
        diff5min = close_5min - ema21_5min
        logger.info(
            f"In Sell Position => ema20 = {str(round(ema21_5min, 2))} closeprice= {str(round(close_5min, 2))} exitPrice= {str(round(exitPrice, 2))} diff= {str(round(diff5min, 2))} RSI-15min= {(round(rsi_15min, 2))}")
        #time.sleep(30)
        # exit half quantity
        if ((close_5min > takeProfit) and (halfExit == 0)):
            logger.info("postion Partail Exit")
            orderLogger.warning(f"position Partial Exit")
            try:
                # shortOrder = placeShortOrder(symbol,ATMStrike)
                # print(shortOrder)
                logger.info(shortOrder)
                profit = (round(exitprice, 2) - round(entryprice, 2) * (qty_2))
                now = datetime.now()
                logger.warning(
                    f"Partial Exit=> Entry Price:  {str(round(entryprice, 2))} Exit price: {str(round(exitprice, 2))} Profit: {str(round(profit, 2))} at {str(now)}")
                orderLogger.warning(
                    f"Partial Exit=> Entry Price:  {str(round(entryprice, 2))} Exit price: {str(round(exitprice, 2))} Profit: {str(round(profit, 2))} at {str(now)}")
                halfExit = 1
                # send notification
            except Exception as e:
                # error handling goes here
                logger.exception(e)
        if ((diff5min < stoploss) or (rsi_15min > 75)):
            logger.info("position full Exit")
            orderLogger.warning(f"position Full Exit")
            try:
                if (halfExit):
                    exitQty = qty_2
                    logger.info(f"Exiting remaining half quantity")
                else:
                    # do nothing
                    exitQty = qty
                    logger.info(f"Exiting full quantity")
                shortOrder = placeShortOrder(symbol,ATMStrike)
                print(shortOrder)
                logger.info(shortOrder)
                profit = (round(exitprice, 2) - round(entryprice, 2) * (exitQty))
                now = datetime.now()
                logger.warning(
                    f"pos Full Exit=> Entry Price:  {str(round(entryprice, 2))} Exit price: {str(round(exitprice, 2))} Profit: {str(round(profit, 2))} at {str(now)}")
                orderLogger.warning(
                    f"pos Full Exit=> Entry Price:  {str(round(entryprice, 2))} Exit price: {str(round(exitprice, 2))} Profit: {str(round(profit, 2))} at {str(now)}")
                # send notification
                sleep(180)
                break
            except Exception as e:
                # error handling goes here
                logger.exception(e)

def main():
    global angelObject
    global username
    global password
    global api_key
    global idx
    global qty
    global instrument
    global stoploss
    global hist
    global ATMdiff
    global marginDiff

    if (idx == 'BNF'):
        symbol = 'BANKNIFTY'
        aliceSymbol = 'Nifty Bank'
        ATMdiff = 400
        marginDiff = 1500
    elif (idx == 'NF'):
        symbol = 'NIFTY'
        aliceSymbol = 'Nifty 50'
        ATMdiff = 200
        marginDiff = 500
    else:
        symbol = 'NONE'

    angelObject = SmartConnect(api_key=api_key)
    data = angelObject.generateSession(username,password)

    refreshToken = data['data']['refreshToken']
    feedToken = angelObject.getfeedToken()

    userProfile = angelObject.getProfile(refreshToken)
    print (userProfile)

    position = angelObject.position()
    print(position)

    tokenInfo, indexPrice  = getIndexPrice(symbol)
    print(f"{symbol} Index Price is: {indexPrice} and token is: {tokenInfo}")

    ATMStrike = getATMStrike(indexPrice)
    print("ATM Strike is: ", ATMStrike)

    while True:

        alice_df15min = getAliceSignal(aliceSymbol, AL_15MIN)
        #print(alice_df15min)
        latest_candle = alice_df15min.iloc[-1]

        ema21_15min = latest_candle['ema21']
        macdVal_15min = latest_candle['macd']
        macdSignal_15min = latest_candle['macdsignal']
        macdDiff_15min = latest_candle['macddiff']
        rsi_15min = latest_candle['rsi']
        close_15min = latest_candle['Close']

        #print(f"EMA21 Value - {AL_15MIN} is {ema21_15min}")
        #print(f"MACD Value - {AL_15MIN} is {macdVal_15min}")
        #print(f"MACD Signal Value - {AL_15MIN} is {macdSignal_15min}")
        #print(f"MACD diff - {AL_15MIN} is {macdDiff_15min}")
        #print(f"RSI Value - {AL_15MIN} is {rsi_15min}")
        #print(f"Close Value - {AL_15MIN} is {close_15min}")

        alice_df5min = getAliceSignal(aliceSymbol, AL_5MIN)
        #print(alice_df5min)
        latest_candle = alice_df5min.iloc[-1]

        ema21_5min = latest_candle['ema21']
        macdVal_5min = latest_candle['macd']
        macdSignal_5min = latest_candle['macdsignal']
        macdDiff_5min = float(latest_candle['macddiff'])
        rsi_5min = latest_candle['rsi']
        close_5min = latest_candle['Close']

        #print(f"EMA21 Value - {AL_5MIN} is {ema21_5min}")
        #print(f"MACD Value - {AL_5MIN} is {macdVal_5min}")
        #print(f"MACD Signal Value - {AL_5MIN} is {macdSignal_5min}")
        #print(f"MACD diff - {AL_5MIN} is {macdDiff_5min}")
        #print(f"RSI Value - {AL_5MIN} is {rsi_5min}")
        #print(f"Close Value - {AL_5MIN} is {close_5min}")

        diff5min = close_5min - ema21_5min
        logger.info(
            f"Short side taking Position => ClosePrice = {str(round(close_15min, 2))}  RSI-15min={str(round(rsi_15min, 2))} Histogram=  {str(round(macdDiff_15min, 2))} diff5min={str(round(diff5min, 2))}")
        time.sleep(30)
        if ((macdDiff_15min > hist) and (diff5min > -stoploss) and (rsi_15min < 60)):
            try:
                # shortOrder = placeShortOrder(symbol,ATMStrike)
                # print(shortOrder)
                logger.info(shortOrder)
                now = datetime.now()
                logger.warning(f"Sell order executed at closeprice: {str(close_15min)} at {str(now)}")
                orderLogger.warning(f"Sell order executed at closeprice: {str(close_15min)} at {str(now)}")
                exitPos(close_15min)
                # send notification
                # exit position
                break
            except Exception as e:
                # error handling goes here
                logger.error(e)


if (__name__ == '__main__'):
    main()
