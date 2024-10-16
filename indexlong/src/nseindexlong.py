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
qty = int(os.environ.get('lot_size'))
qty_2 = qty/2
instrument = os.environ.get('instrument') #OPT, FUT
stoploss = float(os.environ.get('stoploss')) #BNF-100, NF-50
hist = float(os.environ.get('hist')) #BNF > 20, NF > 10
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
logFileName = logFilePath + '/' + idx + 'long.log'
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
angelObj = None
aliceObj = None

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
    global angelObj
    tokenInfo = getTokenInfo(symbol)
    #print(tokenInfo)
    spot_token = tokenInfo.iloc[0]['token']
    #print(spot_token)
    ltpInfo = angelObj.ltpData('NSE', symbol, spot_token)
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
    global angelObj
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
        candle_json = angelObj.getCandleData(historicParam)
        print(candle_json)
        return candle_json
    except Exception as e:
        print("Historic Api failed: {}".format(e.message))

def placeLongOrder(symbol,ATMStrike,entryQty):
    global angelObj
    global marginList
    global posList
    str_next_thursday_expiry, str_month_last_thu_expiry, wkly_expiry_dt, wkly_expiry_month, monthly_expiry_dt, monthly_expiry_month = nextThu_and_lastThu_expiry_date()
    #print("================================================================================")
    #print("Next Expiry Date = " + str_next_thursday_expiry)
    #print("================================================================================")
    #print("Month End Expiry Date = " + str_month_last_thu_expiry)
    #print("================================================================================")
    wkly_mon = wkly_expiry_month
    wkly_dt = wkly_expiry_dt
    optSellWkly_dt = wkly_dt + 7
    optSellStrike = ATMStrike - ATMdiff
    marginBuyWkly_dt = wkly_dt
    marginBuyStrike = ATMStrike + marginDiff
    expiry_day = date(2022, wkly_mon, marginBuyWkly_dt)
    pe_tokeninfo = getTokenInfo(symbol, 'NFO', 'OPTIDX', marginBuyStrike, 'PE', expiry_day)
    print(pe_tokeninfo)
    pe_strike_token = pe_tokeninfo.iloc[0]['token']
    pe_strike_symbol = pe_tokeninfo.iloc[0]['symbol']
    ltp = angelObj.ltpData("NFO",pe_strike_symbol,pe_strike_token)['data']['ltp']
    orderLogger.warning(f"{pe_strike_symbol} Price is: {ltp} and token is {pe_strike_token}")
    marginList = [pe_strike_token,pe_strike_symbol,marginBuyStrike]
    buy_order(pe_strike_token,pe_strike_symbol,entryQty*2)

    expiry_day = date(2022, wkly_mon, optSellWkly_dt)
    pe_tokeninfo = getTokenInfo(symbol, 'NFO', 'OPTIDX', optSellStrike, 'PE', expiry_day)
    print(pe_tokeninfo)
    pe_strike_symbol = pe_tokeninfo.iloc[0]['token']
    print(pe_strike_symbol)
    pe_strike_token = pe_tokeninfo.iloc[0]['token']
    pe_strike_symbol = pe_tokeninfo.iloc[0]['symbol']
    ltp = angelObj.ltpData("NFO", pe_strike_symbol, pe_strike_token)['data']['ltp']
    orderLogger.warning(f"{pe_strike_symbol} Price is: {ltp} and token is {pe_strike_token}")
    posList = [pe_strike_token,pe_strike_symbol,ATMStrike]
    sell_order(pe_strike_token,pe_strike_symbol,entryQty)

def placeExitOrder(symbol,ATMStrike,exitQty):
    global angelObj
    global marginList
    global posList
    orderLogger.warning(f"Inside Place ExitOrder. symbol: {symbol}, ATMStroke: {ATMStrike}, qty: {qty}, marginList: {marginList}, posList: {posList}")
    orderLogger.warning(f"margin buy symbol is {marginList[1]} and token is {marginList[0]}")
    sell_order(marginList[1],marginList[0],exitQty*2)

    orderLogger.warning(f"option sell symbol is {posList[1]} and token is {posList[0]}")
    buy_order(marginList[1],marginList[0],exitQty)

def buy_order(token,symbol,buyQty):
    global angelObj
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
            "quantity": buyQty,
            "triggerprice": "0"
            }
        print(orderparams)
        orderLogger.warning(f"orderparams for margin order: {orderparams}")
        orderId=angelObj.placeOrder(orderparams)
        orderLogger.warning(f"The order id is: {orderId}")
    except Exception as e:
        print(f"Order placement failed: {e}")
        orderLogger.warning(f"Order placement failed: {e}")

def sell_order(token,symbol,sellQty):
    global angelObj
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
            "quantity": sellQty,
            "triggerprice": "0"
            }
        print(orderparams)
        orderLogger.warning(f"orderparams for sell order: {orderparams}")
        orderId=angelObj.placeOrder(orderparams)
        orderLogger.warning(f"The order id is: {orderId}")
    except Exception as e:
        print(f"Order placement failed: {e}")
        orderLogger.warning(f"Order placement failed: {e}")

def exitPos(entryPrice):
    global angelObj
    global aliceObj
    global aliceSymbol
    global qty
    global qty_2
    global AL_15MIN
    global AL_5MIN

    halfExit = 0
    while True:
        alice_df15min = getAliceSignal(aliceObj, aliceSymbol, AL_15MIN)
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

        alice_df5min = getAliceSignal(aliceObj, aliceSymbol, AL_5MIN)
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
        takeProfit = entryPrice - (stoploss*4)
        exitPrice = ema21_5min + stoploss
        diff5min = close_5min - ema21_5min
        ATMStrike = 0
        time.sleep(30)
        logger.info(
            f"In Long Position => ema20 = {str(round(ema21_5min, 2))} closeprice= {str(round(close_5min, 2))} exitPrice= {str(round(exitPrice, 2))} diff= {str(round(diff5min, 2))} RSI-15min= {(round(rsi_15min, 2))}")
        print(
            f"In Long Position => ema20 = {str(round(ema21_5min, 2))} closeprice= {str(round(close_5min, 2))} exitPrice= {str(round(exitPrice, 2))} diff= {str(round(diff5min, 2))} RSI-15min= {(round(rsi_15min, 2))}")
        # exit half quantity
        if ((close_5min > takeProfit) and (halfExit == 0)):
            logger.info("postion Partail Exit")
            orderLogger.warning(f"position Partial Exit")
            try:
                exitOrder = placeExitOrder(symbol,ATMStrike,qty_2)
                print(exitOrder)
                logger.info(exitOrder)
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
                    orderLogger.warning(f"Exiting remaining half quantity")
                else:
                    # do nothing
                    exitQty = qty
                    logger.info(f"Exiting full quantity")
                    orderLogger.info(f"Exiting full quantity")
                exitOrder = placeExitOrder(symbol,ATMStrike,exitQty)
                print(exitOrder)
                logger.info(exitOrder)
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
                # errior handling goes here
                logger.exception(e)

@retry(stop_max_attempt_number=3)
def initAngel():
    logger.info(f"init Angel func")
    print("init Angel func")
    try:
        angelObject = SmartConnect(api_key=api_key)
        data = angelObject.generateSession(username,password)
        refreshtoken = data['data']['refreshToken']
        feedtoken = angelObject.getfeedToken()
        print(feedtoken)
        userprofile = angelObject.getProfile(refreshtoken)
        print (userprofile)
    except Exception as e:
        print(e)
        time.sleep(30)
    return angelObject

def main():
    global username
    global password
    global api_key
    global idx
    global qty
    global qty_2
    global instrument
    global stoploss
    global hist
    global ATMdiff
    global marginDiff
    global angelObj
    global aliceObj
    global aliceSymbol
    global AL_15MIN
    global AL_5MIN
    global marginList
    global posList

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

    angelObj = initAngel()
    aliceObj = initAlice()

    position = angelObj.position()
    print(position)

     while True:
        tokenInfo, indexPrice  = getIndexPrice(symbol)
        print(f"{symbol} Index Price is: {indexPrice} and token is: {tokenInfo}")
        ATMStrike = getATMStrike(indexPrice)
        print("ATM Strike is: ", ATMStrike)

        alice_df15min = getAliceSignal(aliceObj,aliceSymbol, AL_15MIN)
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

        alice_df5min = getAliceSignal(aliceObj,aliceSymbol, AL_5MIN)
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
            f"Long side taking Position => ClosePrice = {str(round(close_15min, 2))}  RSI-15min={str(round(rsi_15min, 2))} Histogram=  {str(round(macdDiff_15min, 2))} diff5min={str(round(diff5min, 2))}")
        print(
            f"Long side taking Position => ClosePrice = {str(round(close_15min, 2))}  RSI-15min={str(round(rsi_15min, 2))} Histogram=  {str(round(macdDiff_15min, 2))} diff5min={str(round(diff5min, 2))}")
        time.sleep(30)
        if ((macdDiff_15min > hist) and (diff5min > -stoploss) and (rsi_15min < 60)):
            try:
                longOrder = placeLongOrder(symbol,ATMStrike,qty)
                print(longOrder)
                logger.info(longOrder)
                now = datetime.now()
                logger.warning(f"Long order executed at closeprice: {str(close_15min)} at {str(now)}")
                orderLogger.warning(f"Long order executed at closeprice: {str(close_15min)} at {str(now)}")
                # send notification
                # exit position
                exitPos(close_15min)
                break
            except Exception as e:
                # error handling goes here
                logger.error(e)

if (__name__ == '__main__'):
    main()
