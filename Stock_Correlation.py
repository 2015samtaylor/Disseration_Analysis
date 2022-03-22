#!/usr/bin/env python
# coding: utf-8

# In[420]:


import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from alpha_vantage.timeseries import TimeSeries
import numpy as np
from functools import reduce

api_key = ('QFZF2CEU5FAI6YIW')
sym = input("Input your stock ticker: ")

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


years = []

def find_date(df):
    
    first_ = len(df) - 1                         # 5631 length of the df
    first = int(str(df['Date'][first_])[0:4])    # 1999 as an integer
    
    last = (first)+1                             # 2000 as an integer
    
    uptodate = int(str(df['Date'][0])[0:4])     # 2022 as an integer
    
    i = 0
    list_len = uptodate - first                 # 23 as an integer
    
    
    while i < list_len:
        
        mask = (df['Date'] >= str(first))& (df['Date'] <= str(last))
        output = df.loc[mask]
        
        first += 1
        last += 1
        i += 1
        years.append(output)
        
find_date(df)

rank = []


def pipe_frame():
    
    list_len = len(years)
    
    i= 0
    
    while i < list_len:
        
    
        stock_df = years[i]
        stock_df.reset_index(inplace = True, drop = True)

        label = str(stock_df['Date'][0])[0:4]
        
        
        
        def narrow_down_between(n, n_2, stock_df):
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

        def narrow_down(n, stock_df):

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
        
        

        n_one = -6.01
        run_one = narrow_down(n_one, stock_df)
        frequent_month_one = frequent_months.to_frame(name = n_one)

        n_two = -3.01
        top = -6
        run_two = narrow_down_between(n_two, top, stock_df)
        frequent_month_two = frequent_months.to_frame(name = n_two)

        n_three = -1.01
        top = -3
        run_three = narrow_down_between(n_three, top, stock_df)
        frequent_month_three = frequent_months.to_frame(name = n_three)

        n_four = 0
        top = -1
        run_four = narrow_down_between(n_four, top, stock_df)
        frequent_month_four = frequent_months.to_frame(name = n_four)

        n_five = 0.1
        top = 1
        run_five = narrow_down_between(n_five, top, stock_df)
        frequent_month_five = frequent_months.to_frame(name = n_five)

        n_six = 1.01
        top = 3
        run_six = narrow_down_between(n_six, top, stock_df)
        frequent_month_six = frequent_months.to_frame(name = n_six)

        n_seven = 3.01
        top = 6
        run_seven = narrow_down_between(n_seven, top, stock_df)
        frequent_month_seven = frequent_months.to_frame(name = n_seven)

        n_eight = 6.01
        run_eight = narrow_down(n_eight, stock_df)
        frequent_month_eight = frequent_months.to_frame(name = n_eight)

#         # -----------------

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


        given_freq.reset_index(inplace = True)
        given_freq = given_freq.sort_values(by = 'WEIGHTING', ascending = False)
        
        
        o = given_freq['index'].values
        oo = given_freq['WEIGHTING'].values

        new_vals = list(zip(o, oo))
        new_vals = pd.DataFrame(new_vals, columns = ['months', label])

#         o = given_freq['index'].values
#         oo = given_freq['index'][::-1].values
#         new_vals = list(zip(o, oo))[::-1]
      
        rank.append(new_vals)    
        i+= 1
    

pipe_frame()      

merged_df = reduce(lambda l, r: pd.merge(l, r, on = 'months', how = 'outer'), rank)
merged_df.dropna(axis = 1, inplace = True)
merged_df.set_index('months', inplace = True)
month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
merged_df = merged_df.transpose()
merged_df.loc['Monthly_Average', :] = merged_df.mean(axis = 0)
merged_df_1 = merged_df.reindex(month_list, axis = 1)



import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from alpha_vantage.timeseries import TimeSeries
import numpy as np
from functools import reduce

api_key = ('QFZF2CEU5FAI6YIW')
sym = input("Input your stock ticker: ")

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


years = []

def find_date(df):
    
    first_ = len(df) - 1                         # 5631 length of the df
    first = int(str(df['Date'][first_])[0:4])    # 1999 as an integer
    
    last = (first)+1                             # 2000 as an integer
    
    uptodate = int(str(df['Date'][0])[0:4])     # 2022 as an integer
    
    i = 0
    list_len = uptodate - first                 # 23 as an integer
    
    
    while i < list_len:
        
        mask = (df['Date'] >= str(first))& (df['Date'] <= str(last))
        output = df.loc[mask]
        
        first += 1
        last += 1
        i += 1
        years.append(output)
        
find_date(df)

rank = []


def pipe_frame():
    
    list_len = len(years)
    
    i= 0
    
    while i < list_len:
        
    
        stock_df = years[i]
        stock_df.reset_index(inplace = True, drop = True)

        label = str(stock_df['Date'][0])[0:4]
        
        
        
        def narrow_down_between(n, n_2, stock_df):
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

        def narrow_down(n, stock_df):

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
        
        

        n_one = -6.01
        run_one = narrow_down(n_one, stock_df)
        frequent_month_one = frequent_months.to_frame(name = n_one)

        n_two = -3.01
        top = -6
        run_two = narrow_down_between(n_two, top, stock_df)
        frequent_month_two = frequent_months.to_frame(name = n_two)

        n_three = -1.01
        top = -3
        run_three = narrow_down_between(n_three, top, stock_df)
        frequent_month_three = frequent_months.to_frame(name = n_three)

        n_four = 0
        top = -1
        run_four = narrow_down_between(n_four, top, stock_df)
        frequent_month_four = frequent_months.to_frame(name = n_four)

        n_five = 0.1
        top = 1
        run_five = narrow_down_between(n_five, top, stock_df)
        frequent_month_five = frequent_months.to_frame(name = n_five)

        n_six = 1.01
        top = 3
        run_six = narrow_down_between(n_six, top, stock_df)
        frequent_month_six = frequent_months.to_frame(name = n_six)

        n_seven = 3.01
        top = 6
        run_seven = narrow_down_between(n_seven, top, stock_df)
        frequent_month_seven = frequent_months.to_frame(name = n_seven)

        n_eight = 6.01
        run_eight = narrow_down(n_eight, stock_df)
        frequent_month_eight = frequent_months.to_frame(name = n_eight)

#         # -----------------

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


        given_freq.reset_index(inplace = True)
        given_freq = given_freq.sort_values(by = 'WEIGHTING', ascending = False)
        
        
        o = given_freq['index'].values
        oo = given_freq['WEIGHTING'].values

        new_vals = list(zip(o, oo))
        new_vals = pd.DataFrame(new_vals, columns = ['months', label])

#         o = given_freq['index'].values
#         oo = given_freq['index'][::-1].values
#         new_vals = list(zip(o, oo))[::-1]
      
        rank.append(new_vals)    
        i+= 1
    

pipe_frame()      

merged_df = reduce(lambda l, r: pd.merge(l, r, on = 'months', how = 'outer'), rank)
merged_df.dropna(axis = 1, inplace = True)
merged_df.set_index('months', inplace = True)
month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
merged_df = merged_df.transpose()
merged_df.loc['Monthly_Average', :] = merged_df.mean(axis = 0)
merged_df = merged_df.reindex(month_list, axis = 1)

print('\n')
print(merged_df_1.corrwith(merged_df))

