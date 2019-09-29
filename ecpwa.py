import datetime as dt
import dateutil as dtu
import numpy as np
import pandas as pd

from WindPy import *
w.start(waitTime=15) # timeout ~15s

# TIME CONSTS #
NOW = dt.datetime.today() # current time
TODAY = dt.datetime.today().date() # today's date(calendar day)
YESTERDAY = TODAY - dt.timedelta(days=1) # yesterday's date(calendar day)
TOMORROW = TODAY - dt.timedelta(days=-1) # tomorrow's date(calendar day)

# CONVERTING STRING(DATETIME) TO DATETIME(STRING)  #
def parsedate(date):
    if isinstance(date, str):
        return dtu.parser.parse(date, ignoretz=True, default=NOW).date() # return datetime.date
    elif isinstance(date, (dt.date, dt.datetime)):
        return date

def unparsedate(date):
    return parsedate(date).strftime('%Y-%m-%d')

# FUNCITONS ON TRADING DAYS #
# return a date series of trading days from start date to end date
def td_series(sdate, edate=TODAY, period="D", exchange="SSE"):
    wsd_data = w.tdays(parsedate(sdate), parsedate(edate), tradingcalendar=exchange, period=period, days="Trading")
    
    if wsd_data.Data:
        return pd.Series(wsd_data.Data[0], index=wsd_data.Times)
    else:
        return None

# return previous(most recent) trading day's date
# from today or a paticular date
def td_prev(date=TODAY, exchange="SSE", **kwargs):
    wind_data = w.tdays("ED-0TD", parsedate(date), tradingcalendar=exchange, **kwargs)
    return wind_data.Times[0] # datetime.date

# return next trading day's date
# from today or a paticular date
def td_next(date=TODAY, exchange="SSE", **kwargs):
    wind_data = w.tdays(parsedate(date), "SD+1TD", tradingcalendar=exchange, **kwargs)
    return wind_data.Times[0] # datetime.date

# determine whether the date is a trading day
def td_is(date=TODAY, exchange="SSE", **kwargs):
    wind_data = td_next(date, exchange=exchange, **kwargs)
    return True if wind_data == parsedate(date) else False

# return previous(most recent) trading day's date
# from a [OFFSET] of a paticular date
def td_offset(offset, date=TODAY, exchange="SSE", **kwargs):   
    # parsing offset(Wind Date Macro)
    if isinstance(offset, int):
        prd = "TD"
    elif offset[-2:].upper() == "TD":
        prd = "TD"  # trading day
        offset = int(offset[:-2])
    else:
        prd = offset[-1:].upper()
        offset = int(offset[:-1])

    wind_data = w.tdaysoffset(offset, parsedate(date), tradingcalendar=exchange, period=prd, **kwargs)
    if wind_data.Times[0] == parsedate("1899-12-30"):
        raise ValueError("Beyond the trading calendar of "+exchange.upper()+".")# datetime.date
    else:
        return wind_data.Times[0]

# return a date series of trading days from start date to end date
def td_series(sdate, edate=TODAY, period="D", exchange="SSE", days="Trading"):
    wsd_data = w.tdays(parsedate(sdate), parsedate(edate), tradingcalendar=exchange, period=period, days=days)
    
    if wsd_data.Data:
        return pd.Series(wsd_data.Data[0], index=wsd_data.Times)
    else:
        return None

# return the nearest trading day's date
# e.g: Sat -> Fri, Sun -> Mon
def td_nearest(date=TODAY, **kwargs):   
    prev_tdate = td_prev(date, **kwargs)
    next_tdate = td_next(date, **kwargs)

    # prev_tdate <- prev_delta -> date <- next_delta -> next_tdate
    prev_delta = date - prev_tdate
    next_delta = next_tdate - date
    
    if prev_delta.days == next_delta.days == 1:
        return date
    elif prev_delta <= next_delta:
        return prev_tdate
    else:
        return next_tdate

# count how many trading days from start date to end date
def td_count(sdate, edate=TODAY, exchange="SSE", **kwargs):
    return len(td_series(sdate, edate, exchange="SSE", **kwargs))

# fetch time series data via Wind API
def wind_series(wcodes, fields, sdate, edate, period="D", exchange="SSE", days="trading", fill="blank", **kwargs): 
    
    if isinstance(wcodes, str):
        wcodes = wcodes.split(',')
    if isinstance(fields, str):
        fields = fields.split(',')
        
    wcodes_len = len(wcodes)

    df = pd.DataFrame()
    
    for f in fields:
        wind_data = w.wsd(wcodes, f, parsedate(sdate), parsedate(edate),
                          period=period, tradingcalendar=exchange, days=days, fill=fill,**kwargs) # raw Wind data
        if wind_data.ErrorCode != 0: # Error Code: ref: https://www.windquant.com/
            raise Exception("Wind Error Code:" + str(wind_data.ErrorCode))
        
        fields_col = [f.upper()] * wcodes_len # like ["CLOSE", "HIGH", "LOW"]

        idx = pd.MultiIndex.from_arrays([fields_col, wind_data.Codes])
        sub_df = pd.DataFrame(wind_data.Data, index=idx, columns=wind_data.Times).T
        df = pd.concat([df, sub_df], axis=1)

    df.index = pd.DatetimeIndex(df.index)

    return df

# fetch panel data via Wind API
def wind_panel(wcodes, fields, sdate, edate=TODAY, period="Y", exchange="SSE", days="alldays", **kwargs):
    dfs = []
    df_time_idx = []
    for i in td_series(sdate, edate, period=period, exchange=exchange, days=days):
        df_time_idx.append(i)
        errorCode, df = w.wss(wcodes, fields, rptDate=i.strftime('%Y%m%d'), usedf=True, **kwargs)
        dfs.append(df)
        
    return pd.concat(dfs, keys=df_time_idx, axis=0)

# fetch OHLCV via Wind API
def wind_ohlcv(wcodes, stime, etime, barsize=2, fields="open, high, low, close, amt", **kwargs):
    if barsize >= 60:
        raise ValueError("Barsize cannot be longer than 60mins.")
    errorCode, df = w.wsi(wcodes, fields, parsedate(stime), parsedate(etime),
                          barsize=barsize, usedf=True, **kwargs)
    return df

# fetch intraday tick data via Wind API
def wind_tick(wcodes, stime, etime, **kwargs):
    errorCode, df = w.wst(wcodes, "last", parsedate(stime), parsedate(etime), usedf=True, **kwargs)
    return df
