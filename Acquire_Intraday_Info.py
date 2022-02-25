#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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
    merged_df = merged_df[['time', 'PctChange', 'open', 'close','high', 'low','volume','bull_bear', 'Day_of_the_Week']]
    
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

