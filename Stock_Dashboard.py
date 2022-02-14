#!/usr/bin/env python
# coding: utf-8

# In[15]:


import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import time
# %matplotlib inline
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime
from statistics import stdev
from statistics import mean
from pandas.tseries.offsets import DateOffset
import mplfinance as mpf
from sklearn.preprocessing import MinMaxScaler
import warnings 
from pandas.core.common import SettingWithCopyWarning
# from oauth2client.service_account import ServiceAccountCredentials
# import gspread
from gspread_pandas import Spread, Client
from datetime import date


api_key = ('QFZF2CEU5FAI6YIW')
# sym = input("Input your stock ticker: ")

sym = 'MGM'

ts = TimeSeries(key= api_key, output_format = 'pandas')
df, meta_data = ts.get_daily(symbol = sym, outputsize = 'full')


df.reset_index(inplace = True)
df.rename(columns = {'date': 'Date','1. open' : 'Open', '2. high': 'High', '3. low': 'Low', '4. close': 'Close', '5. volume': 'Volume'}, inplace = True)

# reverse
df = df.reindex(index = df.index[::-1])
df.reset_index(inplace = True, drop = True)


def percentage_change(High, Low):
    return(High - Low)

# Create 50 EMA column, and day of the week column 
df['50ema'] = df['Close'].rolling(window = 50).mean()
df['200ema'] = df['Close'].rolling(window = 200).mean()
df['DayOfWeek'] = df['Date'].dt.day_name()
df['PctChange'] = df['Close'].pct_change()*100
df['HLVolatility'] = percentage_change(df['High'], df['Low'])
df['GainLoss'] = df['PctChange']/100*df['Close']


# reverse back, and assign column in specific order
spec_order = ['GainLoss', 'DayOfWeek', 'Date', 'Open', 'Close', 'PctChange','High', 'Low','HLVolatility','50ema', '200ema', 'Volume']
df = df.reindex(index = df.index[::-1], columns = spec_order)
df.reset_index(inplace = True, drop = True)
pd.set_option('display.float_format', lambda x: '%.2f' % x)
stock_df = df.copy()
stock_df_shift = stock_df.copy()

#``````````````
# threshold of 1.01 pct change to classify bear bull openings. Then create bull, bear category on stock_df

stock_df_shift['Close'] = stock_df_shift['Close'].shift(-1)
bull_bear = 1 - (stock_df_shift['Close'][0:] / stock_df_shift['Open'][0:] )
slow_start = np.where(bull_bear < .01, 0, 1)
stock_df['bull_bear'] = slow_start

# -------------------------------creating initial dataframe 

# -----------------------------------------------------------------creating dataframe under 200 day moving average 


#@title Chance of Accomplishing Gain Based on Frequency

def narrow_down_between(n, n_2):
    if n > 0:
        movement = stock_df.loc[(stock_df['PctChange'] >= n) & (stock_df['PctChange'] <= n_2)]
    else:
        movement = stock_df.loc[(stock_df['PctChange'] <= n) & (stock_df['PctChange'] >= n_2)]

    movement = movement.loc[movement['200ema']* 1.05 > movement['Close']]
            
    movement_copy = movement.copy()
    movement_copy['Date'] = movement_copy['Date'].astype(str).str[5:7]
    movement_copy['Date'] = movement_copy['Date'].astype(int)
    global frequent_months 
    frequent_months = movement_copy['Date'].value_counts()
    
    bl_list = movement['Date'].to_list()
    return(bl_list)


# # --------------------

def narrow_down(n):

    if n > 0:
        movement = stock_df.loc[stock_df['PctChange'] > n] 
    else:
        movement = stock_df.loc[stock_df['PctChange'] < n]

    movement = movement.loc[movement['200ema'] * 1.05 > movement['Close']]

    
    movement_copy = movement.copy()
    movement_copy['Date'] = movement_copy['Date'].astype(str).str[5:7]
    movement_copy['Date'] = movement_copy['Date'].astype(int)
    global frequent_months 
    frequent_months = movement_copy['Date'].value_counts()
        
       
    bl_list = movement['Date'].to_list()
    return(bl_list)    
    
    
n_one = -6.01
run_one = narrow_down(n_one)    
    
    
n_two = -3.01
top = -6
run_two = narrow_down_between(n_two, top)
    

n_three = -1.01
top = -3
run_three = narrow_down_between(n_three, top)


n_four = 0
top = -1
run_four = narrow_down_between(n_four, top)


n_five = 0.1
top = 1
run_five = narrow_down_between(n_five, top)

n_six = 1.01
top = 3
run_six = narrow_down_between(n_six, top)

n_seven = 3.01
top = 6
run_seven = narrow_down_between(n_seven, top)

n_eight = 6.01
run_eight = narrow_down(n_eight)
    
    
# ----------------

# define the function

span = 25
limit = 5


def get_data(run):
    list_len = len(run)
    i = 0
    while i < list_len:
        start = run[i]
        end = start + pd.tseries.offsets.BusinessDay(n = span)
        end = str(end)[0:10]
        day_before = start - DateOffset(days = 0)
        day_before = str(day_before)[0:10]
        row_zero = stock_df[stock_df['Date'].astype(str).str[:10] == day_before]
        start = str(start)[0:10]
        query = stock_df[(stock_df['Date'] >= (start)) & (stock_df['Date'] <= (end))] 
        query = query.reindex(index = query.index[::-1])
        query.reset_index(inplace = True, drop = True)
        df = pd.concat([row_zero, query], ignore_index=True)
        
        begin_price.append(df['Close'].iloc[0])
        peak_num = df['Close'].max()
        min_num = df['Close'][0]
        gain = round(peak_num - min_num,2)
     
        if gain == 0:
            min_num = df['Close'].iloc[-1]
            gain = round(min_num - peak_num,2)
        else:
            pass
        gain_list.append(gain)
    
        initial_date = df.loc[df['Close']==peak_num]
        peak_date = []
        for stuff in initial_date['Date']:
            peak_date.append(stuff)
            
        peak_date_vars = pd.DataFrame(peak_date, columns = ['peak_dates'])   #change a list to a dataframe 
        begin_date = df['Date'].iloc[0]      # begin_date as a string, end is already a string in function.
        begin_date = str(begin_date)[0:10]    
        peak_date_vars['peak_dates'] = peak_date_vars['peak_dates'].apply(str)     # change datetime to strings
        peak_date_vars['peak_dates'] = peak_date_vars['peak_dates'].str.slice(0,10) # slice the strings 
        peak_date_vars.loc[peak_date_vars['peak_dates'] == begin_date, 'peak_dates'] = end  #located bad peak_dates 
        peak_date_vars['peak_dates'] = peak_date_vars['peak_dates'].apply(pd.to_datetime) # change back to datetime     
        begin_date = df['Date'].iloc[0]
        num_of_days = peak_date_vars['peak_dates'][0] - begin_date
        date_list.append(num_of_days)
        i +=1
    return(gain_list, date_list, begin_price)


# ------------ run one

gain_list = []
date_list = []
begin_price = []

gain_list, date_list, begin_price =get_data(run_one)   

def follow_up():
    df_scat = pd.DataFrame(list(zip(gain_list, date_list, begin_price)),
            columns = ['gains_under', 'time_elapsed_under', 'begin_price_under'])
    df_scat = df_scat.sort_values(by = ['time_elapsed_under'], ascending = True, ignore_index = True)
    df_scat['time_elapsed_under'] = df_scat['time_elapsed_under'].astype(str)
    df_scat['ratio_under'] = df_scat['gains_under'] / df_scat['begin_price_under'] * 100
    
    low_gains = df_scat['ratio_under'].loc[df_scat['ratio_under'] <= limit].values
    chance_of_gain_one = round(100-(len(low_gains)/len(df_scat)*100),2)
    
    return(chance_of_gain_one, df_scat)    

chance_of_gain_one, df_scat = follow_up()

    
# --------- run two

gain_list = []
date_list = []
begin_price = []

gain_list, date_list, begin_price =get_data(run_two)   

chance_of_gain_two, df_scat = follow_up()


# # ---------run three

gain_list = []
date_list = []
begin_price = []

gain_list, date_list, begin_price =get_data(run_three)   

chance_of_gain_three, df_scat = follow_up()

# # ---------run four

gain_list = []
date_list = []
begin_price = []

gain_list, date_list, begin_price =get_data(run_six)   

chance_of_gain_six, df_scat = follow_up()

# # ------------- run five

gain_list = []
date_list = []
begin_price = []

gain_list, date_list, begin_price =get_data(run_seven)   

chance_of_gain_seven, df_scat = follow_up()

# # ------------- run six

gain_list = []
date_list = []
begin_price = []

gain_list, date_list, begin_price =get_data(run_eight)   

chance_of_gain_eight, df_scat = follow_up()

# # ------------

data = {'Daily_Pct_Changes_under' : [str(n_one) + ' & less', str(n_one)+ ' to ' + str(n_two), str(n_two) + ' to ' + str(n_three), str(n_six) + ' to ' + str(n_seven), str(n_seven) + ' to '  + str(n_eight), str(n_eight) + ' & above ' + sym], 
        'Chance_of_Gain_under' : [chance_of_gain_one, chance_of_gain_two, chance_of_gain_three, chance_of_gain_six, chance_of_gain_seven, chance_of_gain_eight],
         'Total_Records_under' : [len(run_one), len(run_two), len(run_three), len(run_six), len(run_seven), len(run_eight)]}

frame = pd.DataFrame(data)

#@title Scatter Plot 

if frame.iloc[0]['Chance_of_Gain_under'] == frame['Chance_of_Gain_under'].max():
    gain_list = []
    date_list = []
    begin_price = []
    
    gain_list, date_list, begin_price =get_data(run_one) 
    
    chance_of_gain_one, df_scat = follow_up()
    
    plug_chance = chance_of_gain_one
    plug_pct = (str(n_one) + ' & less')
    
elif frame.iloc[1]['Chance_of_Gain_under'] == frame['Chance_of_Gain_under'].max():
    gain_list = []
    date_list = []
    begin_price = []
    
    gain_list, date_list, begin_price =get_data(run_two) 
    
    chance_of_gain_two, df_scat = follow_up()
    
    plug_chance = chance_of_gain_two
    plug_pct = (str(n_one)+ ' to ' + str(n_two))
    
elif frame.iloc[2]['Chance_of_Gain_under'] == frame['Chance_of_Gain_under'].max():
    gain_list = []
    date_list = []
    begin_price = []    
    
    gain_list, date_list, begin_price =get_data(run_three)  
    
    chance_of_gain_three, df_scat = follow_up()
    
    plug_chance = chance_of_gain_three
    plug_pct = (str(n_two) + ' to ' + str(n_three))
    
elif frame.iloc[3]['Chance_of_Gain_under'] == frame['Chance_of_Gain_under'].max():
    gain_list = []
    date_list = []
    begin_price = []
    gain_list, date_list, begin_price =get_data(run_six)  
    
    chance_of_gain_six, df_scat = follow_up()
    
    plug_chance = chance_of_gain_six
    plug_pct = (str(n_six) + ' to ' + str(n_seven))

elif frame.iloc[4]['Chance_of_Gain_under'] == frame['Chance_of_Gain_under'].max():
    gain_list = []
    date_list = []
    begin_price = []
    gain_list, date_list, begin_price =get_data(run_seven) 
    
    chance_of_gain_seven, df_scat = follow_up()
    
    plug_chance = chance_of_gain_seven
    plug_pct = (str(n_seven) + ' to '  + str(n_eight))
else:
    gain_list = []
    date_list = []
    begin_price = []
    gain_list, date_list, begin_price =get_data(run_eight)
    
    chance_of_gain_eight, df_scat = follow_up()
    
    plug_chance = chance_of_gain_eight
    plug_pct = (str(n_eight) + ' & above')
    
df_scat_under = pd.concat([df_scat, frame], axis = 1)


spread = Spread('Stock_Dashboard')

spread.df_to_sheet(df_scat_under, index = False, sheet = 'Under_200', replace = True)





# ------------------------------------------------------------------Creating DataFrame over 200 day moving average

def narrow_down_between(n, n_2):
    if n > 0:
        movement = stock_df.loc[(stock_df['PctChange'] >= n) & (stock_df['PctChange'] <= n_2)]
    else:
        movement = stock_df.loc[(stock_df['PctChange'] <= n) & (stock_df['PctChange'] >= n_2)]

    movement = movement.loc[movement['200ema'] < movement['Close']]
            
    movement_copy = movement.copy()
    movement_copy['Date'] = movement_copy['Date'].astype(str).str[5:7]
    movement_copy['Date'] = movement_copy['Date'].astype(int)
    global frequent_months 
    frequent_months = movement_copy['Date'].value_counts()
    
    bl_list = movement['Date'].to_list()
    return(bl_list)


# # --------------------

def narrow_down(n):

    if n > 0:
        movement = stock_df.loc[stock_df['PctChange'] > n] 
    else:
        movement = stock_df.loc[stock_df['PctChange'] < n]

    movement = movement.loc[movement['200ema'] < movement['Close']]
       
    movement_copy = movement.copy()
    movement_copy['Date'] = movement_copy['Date'].astype(str).str[5:7]
    movement_copy['Date'] = movement_copy['Date'].astype(int)
    global frequent_months 
    frequent_months = movement_copy['Date'].value_counts()
        
       
    bl_list = movement['Date'].to_list()
    return(bl_list)    

    
n_one = -6.01
run_one = narrow_down(n_one)    
    
    
n_two = -3.01
top = -6
run_two = narrow_down_between(n_two, top)
    

n_three = -1.01
top = -3
run_three = narrow_down_between(n_three, top)


n_four = 0
top = -1
run_four = narrow_down_between(n_four, top)


n_five = 0.1
top = 1
run_five = narrow_down_between(n_five, top)

n_six = 1.01
top = 3
run_six = narrow_down_between(n_six, top)

n_seven = 3.01
top = 6
run_seven = narrow_down_between(n_seven, top)

n_eight = 6.01
run_eight = narrow_down(n_eight)
    
    
# ----------------

# define the function

span = 25
limit = 5


def get_data(run):
    list_len = len(run)
    i = 0
    while i < list_len:
        start = run[i]
        end = start + pd.tseries.offsets.BusinessDay(n = span)
        end = str(end)[0:10]
        day_before = start - DateOffset(days = 0)
        day_before = str(day_before)[0:10]
        row_zero = stock_df[stock_df['Date'].astype(str).str[:10] == day_before]
        start = str(start)[0:10]
        query = stock_df[(stock_df['Date'] >= (start)) & (stock_df['Date'] <= (end))] 
        query = query.reindex(index = query.index[::-1])
        query.reset_index(inplace = True, drop = True)
        df = pd.concat([row_zero, query], ignore_index=True)
        
        begin_price.append(df['Close'].iloc[0])
        peak_num = df['Close'].max()
        min_num = df['Close'][0]
        gain = round(peak_num - min_num,2)
     
        if gain == 0:
            min_num = df['Close'].iloc[-1]
            gain = round(min_num - peak_num,2)
        else:
            pass
        gain_list.append(gain)
    
        initial_date = df.loc[df['Close']==peak_num]
        peak_date = []
        for stuff in initial_date['Date']:
            peak_date.append(stuff)
            
        peak_date_vars = pd.DataFrame(peak_date, columns = ['peak_dates'])   #change a list to a dataframe 
        begin_date = df['Date'].iloc[0]      # begin_date as a string, end is already a string in function.
        begin_date = str(begin_date)[0:10]    
        peak_date_vars['peak_dates'] = peak_date_vars['peak_dates'].apply(str)     # change datetime to strings
        peak_date_vars['peak_dates'] = peak_date_vars['peak_dates'].str.slice(0,10) # slice the strings 
        peak_date_vars.loc[peak_date_vars['peak_dates'] == begin_date, 'peak_dates'] = end  #located bad peak_dates 
        peak_date_vars['peak_dates'] = peak_date_vars['peak_dates'].apply(pd.to_datetime) # change back to datetime     
        begin_date = df['Date'].iloc[0]
        num_of_days = peak_date_vars['peak_dates'][0] - begin_date
        date_list.append(num_of_days)
        i +=1
    return(gain_list, date_list, begin_price)


# ------------ run one

gain_list = []
date_list = []
begin_price = []

gain_list, date_list, begin_price =get_data(run_one)   

def follow_up():
    df_scat = pd.DataFrame(list(zip(gain_list, date_list, begin_price)),
            columns = ['gains_over', 'time_elapsed_over', 'begin_price_over'])
    df_scat = df_scat.sort_values(by = ['time_elapsed_over'], ascending = True, ignore_index = True)
    df_scat['time_elapsed_over'] = df_scat['time_elapsed_over'].astype(str)
    df_scat['ratio_over'] = df_scat['gains_over'] / df_scat['begin_price_over'] * 100
    
    low_gains = df_scat['ratio_over'].loc[df_scat['ratio_over'] <= limit].values
    chance_of_gain_one = round(100-(len(low_gains)/len(df_scat)*100),2)
    
    return(chance_of_gain_one, df_scat)    

chance_of_gain_one, df_scat = follow_up()

    
# --------- run two

gain_list = []
date_list = []
begin_price = []

gain_list, date_list, begin_price =get_data(run_two)   

chance_of_gain_two, df_scat = follow_up()


# # ---------run three

gain_list = []
date_list = []
begin_price = []

gain_list, date_list, begin_price =get_data(run_three)   

chance_of_gain_three, df_scat = follow_up()

# # ---------run four

gain_list = []
date_list = []
begin_price = []

gain_list, date_list, begin_price =get_data(run_six)   

chance_of_gain_six, df_scat = follow_up()

# # ------------- run five

gain_list = []
date_list = []
begin_price = []

gain_list, date_list, begin_price =get_data(run_seven)   

chance_of_gain_seven, df_scat = follow_up()

# # ------------- run six

gain_list = []
date_list = []
begin_price = []

gain_list, date_list, begin_price =get_data(run_eight)   

chance_of_gain_eight, df_scat = follow_up()

# # ------------
# # success rate is classified as 1 percent return within the timeframe. 
data = {'Daily_Pct_Changes_over' : [str(n_one) + ' & less', str(n_one)+ ' to ' + str(n_two), str(n_two) + ' to ' + str(n_three), str(n_six) + ' to ' + str(n_seven), str(n_seven) + ' to '  + str(n_eight), str(n_eight) + ' & above ' + sym], 
        'Chance_of_Gain_over' : [chance_of_gain_one, chance_of_gain_two, chance_of_gain_three, chance_of_gain_six, chance_of_gain_seven, chance_of_gain_eight],
         'Total_Records_over' : [len(run_one), len(run_two), len(run_three), len(run_six), len(run_seven), len(run_eight)]}

frame = pd.DataFrame(data)

#@title Scatter Plot 

if frame.iloc[0]['Chance_of_Gain_over'] == frame['Chance_of_Gain_over'].max():
    gain_list = []
    date_list = []
    begin_price = []
    
    gain_list, date_list, begin_price =get_data(run_one) 
    
    chance_of_gain_one, df_scat = follow_up()
    
    plug_chance = chance_of_gain_one
    plug_pct = (str(n_one) + ' & less')
    
elif frame.iloc[1]['Chance_of_Gain_over'] == frame['Chance_of_Gain_over'].max():
    gain_list = []
    date_list = []
    begin_price = []
    
    gain_list, date_list, begin_price =get_data(run_two) 
    
    chance_of_gain_two, df_scat = follow_up()
    
    plug_chance = chance_of_gain_two
    plug_pct = (str(n_one)+ ' to ' + str(n_two))
    
elif frame.iloc[2]['Chance_of_Gain_over'] == frame['Chance_of_Gain_over'].max():
    gain_list = []
    date_list = []
    begin_price = []    
    
    gain_list, date_list, begin_price =get_data(run_three)  
    
    chance_of_gain_three, df_scat = follow_up()
    
    plug_chance = chance_of_gain_three
    plug_pct = (str(n_two) + ' to ' + str(n_three))
    
elif frame.iloc[3]['Chance_of_Gain_over'] == frame['Chance_of_Gain_over'].max():
    gain_list = []
    date_list = []
    begin_price = []
    gain_list, date_list, begin_price =get_data(run_six)  
    
    chance_of_gain_six, df_scat = follow_up()
    
    plug_chance = chance_of_gain_six
    plug_pct = (str(n_six) + ' to ' + str(n_seven))

elif frame.iloc[4]['Chance_of_Gain_over'] == frame['Chance_of_Gain_over'].max():
    gain_list = []
    date_list = []
    begin_price = []
    gain_list, date_list, begin_price =get_data(run_seven) 
    
    chance_of_gain_seven, df_scat = follow_up()
    
    plug_chance = chance_of_gain_seven
    plug_pct = (str(n_seven) + ' to '  + str(n_eight))
else:
    gain_list = []
    date_list = []
    begin_price = []
    gain_list, date_list, begin_price =get_data(run_eight)
    
    chance_of_gain_eight, df_scat = follow_up()
    
    plug_chance = chance_of_gain_eight
    plug_pct = (str(n_eight) + ' & above')
    

df_scat_over = pd.concat([df_scat, frame], axis = 1)


spread.df_to_sheet(df_scat_over, index = False, sheet = 'Over_200', replace = True)


# ----------------------------------------------------------------test

def narrow_down_between(n, n_2):
    if n > 0:
        movement = stock_df.loc[(stock_df['PctChange'] >= n) & (stock_df['PctChange'] <= n_2)]
    else:
        movement = stock_df.loc[(stock_df['PctChange'] <= n) & (stock_df['PctChange'] >= n_2)]
            
    movement_copy = movement.copy()
    movement_copy['Date'] = movement_copy['Date'].astype(str).str[5:7]
    movement_copy['Date'] = movement_copy['Date'].astype(int)
    global frequent_months 
    frequent_months = movement_copy['Date'].value_counts()
    
    bl_list = movement['Date'].to_list()
    return(bl_list)


# # --------------------

def narrow_down(n):

    if n > 0:
        movement = stock_df.loc[stock_df['PctChange'] > n] 
    else:
        movement = stock_df.loc[stock_df['PctChange'] < n]
    
    movement_copy = movement.copy()
    movement_copy['Date'] = movement_copy['Date'].astype(str).str[5:7]
    movement_copy['Date'] = movement_copy['Date'].astype(int)
    global frequent_months 
    frequent_months = movement_copy['Date'].value_counts()
        
       
    bl_list = movement['Date'].to_list()
    return(bl_list)    
# ---------------------


# specify instances of percentage changes in specific intervals. Takes into 
# account relation to EMA.

n_one = -6.01
run_one = narrow_down(n_one)
frequent_month_one = frequent_months.to_frame(name = n_one)

n_two = -3.01
top = -6
run_two = narrow_down_between(n_two, top)
frequent_month_two = frequent_months.to_frame(name = n_two)

n_three = -1.01
top = -3
run_three = narrow_down_between(n_three, top)
frequent_month_three = frequent_months.to_frame(name = n_three)

n_four = 0
top = -1
run_four = narrow_down_between(n_four, top)
frequent_month_four = frequent_months.to_frame(name = n_four)

n_five = 0.1
top = 1
run_five = narrow_down_between(n_five, top)
frequent_month_five = frequent_months.to_frame(name = n_five)

n_six = 1.01
top = 3
run_six = narrow_down_between(n_six, top)
frequent_month_six = frequent_months.to_frame(name = n_six)

n_seven = 3.01
top = 6
run_seven = narrow_down_between(n_seven, top)
frequent_month_seven = frequent_months.to_frame(name = n_seven)

n_eight = 6.01
run_eight = narrow_down(n_eight)
frequent_month_eight = frequent_months.to_frame(name = n_eight)

# -----------------

given_freq = pd.concat([frequent_month_one, frequent_month_two, frequent_month_three, frequent_month_four, frequent_month_five, frequent_month_six, frequent_month_seven, frequent_month_eight], axis = 1)
dic = { 1 : 'January' , 2: 'February', 3: 'March', 4 : 'April', 5: 'May', 6 : 'June', 7: 'July', 8: 'August', 9 : 'September', 10 : 'October' , 11 : 'November', 12:  'December'}

given_freq = given_freq.rename(index = dic)
given_freq = given_freq.replace(np.nan, 0)
given_freq = given_freq.astype(int)

given_freq['NEG_VOLATILITY'] = -((given_freq[n_one]*3 + given_freq[n_two]*2 + given_freq[n_three]) + (given_freq[n_four]))
given_freq['POS_VOLATILITY'] = (given_freq[n_five] + given_freq[n_six] + given_freq[n_seven]*2 + given_freq[n_eight]*3)
given_freq['WEIGHTING'] = (given_freq['NEG_VOLATILITY'] + given_freq['POS_VOLATILITY'])
scaler = MinMaxScaler(feature_range = (0,10))
given_freq ['WEIGHTING'] = scaler.fit_transform(given_freq['WEIGHTING'].values.reshape(-1,1))

column_names = [str(n_one) + ' & less', str(n_one)+ ' to ' + str(n_two), str(n_two) + ' to ' + str(n_three), str(n_three) + ' to ' + str(n_four), str(n_five) + ' to '  + str(n_six), str(n_six) + ' to '  + str(n_seven), str(n_seven) + ' to '  + str(n_eight), str(n_eight) + ' & above' , 'NEG_VOLATILITY','POS_VOLATILITY', 'WEIGHTING']
given_freq.columns = column_names

given_freq

given_freq.sort_values(by = 'WEIGHTING', ascending = False)
given_freq.reset_index(inplace = True)

spread.df_to_sheet(given_freq, index = False, sheet = 'Weightings', replace = True)

# now = datetime.now()
# dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
# print(dt_string)((((((((((((((((((((((((((((((((((((((((((((((()))))))))))))))))))))))))))))))))))))))))))))))



ticker = sym
date = 'year1month1'

df_1 = pd.read_csv('https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol='+ticker+'&interval=15min&slice='+date+'&apikey='+api_key+'&datatype=csv&outputsize=full')

date = 'year1month2'
df_2 = pd.read_csv('https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol='+ticker+'&interval=15min&slice='+date+'&apikey='+api_key+'&datatype=csv&outputsize=full')

date = 'year1month3'
df_3 = pd.read_csv('https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol='+ticker+'&interval=15min&slice='+date+'&apikey='+api_key+'&datatype=csv&outputsize=full')

date = 'year1month4'
df_4 = pd.read_csv('https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol='+ticker+'&interval=15min&slice='+date+'&apikey='+api_key+'&datatype=csv&outputsize=full')

# -------
df = pd.concat([df_1, df_2, df_3, df_4], ignore_index = True)
df['time'] = df['time'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
df['Day_of_the_Week'] = df['time'].dt.day_name()

time_end = df['time'].iloc[0]
time_begin = df['time'].iloc[-1]

TB = str(time_begin)[0:10]
time_begin = TB +  str(' 6:30:00')

TE = str(time_end)[0:10]
time_end = TB +  str(' 13:00:00')

df.set_index('time')

# ---------------
# Function to Create Intraday DataFrames 


beginning = pd.date_range(start = time_begin, end = TE, freq = 'D')
the_end = pd.date_range(start = time_end , end = TE, freq = 'D')

beginning = beginning.to_frame(index = False)
beginning.rename(columns={0 : 't'}, inplace = True)
the_end = the_end.to_frame(index = False)
the_end.rename(columns = {0 : 'ti'}, inplace = True)

first = beginning['t'].tolist()
second = the_end['ti'].tolist()

low = []

def go(beginning_time, end_time):
    total_time = len(beginning_time)
    i = 0
    while i < total_time:
        one_day = pd.date_range(start = beginning_time[i], end = end_time[i] , freq = '15min')
        single_day = one_day.to_frame(index = True)
        single_day.rename(columns={0 : 'time'}, inplace = True)
        combo = pd.merge(single_day, df, on = 'time', how = 'inner') 
        if not combo.empty:
            data = combo
            ind = data[['low']].idxmin()
            record = data.iloc[ind]
            low.append(record)        
        i += 1
go(first, second)   
df_low = pd.concat(low)

# ------
high = []


def go(beginning_time, end_time):
    total_time = len(beginning_time)
    i = 0
    while i < total_time:
        one_day = pd.date_range(start = beginning_time[i], end = end_time[i] , freq = '15min')
        single_day = one_day.to_frame(index = True)
        single_day.rename(columns={0 : 'time'}, inplace = True)
        combo = pd.merge(single_day, df, on = 'time', how = 'inner') 
        if not combo.empty:
            data = combo
            ind = data[['low']].idxmax()
            record = data.iloc[ind]
            high.append(record)
            
        i += 1
        
go(first, second)
df_high = pd.concat(high)


#########################################

def merge_func(daily_df, intraday_df):
    daily_df['Date'] = daily_df['Date'].astype(str)
    intraday_df['time'] = intraday_df['time'].astype(str)
    
    sauce = []
    times = []

    for stuff in intraday_df['time']:
        sauce.append(stuff)
    for stuff in sauce:
        times.append(stuff[0:10])

    intraday_df['Date'] = times
    merged_df = pd.merge(daily_df, intraday_df, on= 'Date')
    merged_df['time'] = merged_df['time'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    merged_df = merged_df[['time', 'PctChange', 'open', 'close','high', 'low','volume','bull_bear']]
    
    return(merged_df)


df_low = merge_func(stock_df, df_low)
df_high = merge_func(stock_df, df_high)


def just_time(intraday_df):

    intraday_df['time'] = intraday_df['time'].astype(str)


    sauce = []
    times = []

    for stuff in intraday_df['time']:
        sauce.append(stuff)
    for stuff in sauce:
        times.append(stuff[11:16])
            
    altered = ([s.replace(':', '') for s in times])
    new_alter = [int(i) for i in altered]

    spec_day = pd.DataFrame(new_alter)
    
    intraday_df = pd.concat([intraday_df, spec_day], axis = 1)
    
    return(intraday_df)

df_high = just_time(df_high)
df_high.rename(columns = {0: 'intra'}, inplace = True)

df_low = just_time(df_low)
df_low.rename(columns = {0: 'intra'}, inplace = True)

spread.df_to_sheet(df_low, index = False, sheet = 'df_low', replace = True)

spread.df_to_sheet(df_high, index = False, sheet = 'df_high', replace = True)

now = datetime.now()
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
print(dt_string)

