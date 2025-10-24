import pandas as pd
import numpy as np
import datetime
import time


def add_prev_date_customer(group: pd.DataFrame) -> pd.DataFrame:
    group = group.copy()
    # μοναδικές ημερομηνίες ανά πελάτη
    unique_dates = group["date"].drop_duplicates().sort_values().reset_index(drop=True)
    # mapping ημερομηνίας → προηγούμενη ημερομηνία
    prev_date_map = dict(zip(unique_dates[1:], unique_dates.shift()[1:]))
    # νέα στήλη με την προηγούμενη ημερομηνία
    group["previous_purchase_date_customer"] = group["date"].map(prev_date_map)
    
    # διαφορά ημερών
    #group["DaysSincePrevdate"] = (group["date"] - group["previous_purchase_date_customer"]).dt.days
    return group


def add_prev_date_customer_product(group: pd.DataFrame) -> pd.DataFrame:
    group = group.copy()
    # μοναδικές ημερομηνίες ανά πελάτη
    unique_dates = group["date"].drop_duplicates().sort_values().reset_index(drop=True)
    # mapping ημερομηνίας → προηγούμενη ημερομηνία
    prev_date_map = dict(zip(unique_dates[1:], unique_dates.shift()[1:]))
    # νέα στήλη με την προηγούμενη ημερομηνία
    group["previous_purchase_date_customer_product"] = group["date"].map(prev_date_map)
    
    # διαφορά ημερών
    #group["DaysSincePrevdate"] = (group["date"] - group["previous_purchase_date_customer"]).dt.days
    return group


def add_prev_date_product(group: pd.DataFrame) -> pd.DataFrame:
    group = group.copy()
    # μοναδικές ημερομηνίες ανά πελάτη
    unique_dates = group["date"].drop_duplicates().sort_values().reset_index(drop=True)
    # mapping ημερομηνίας → προηγούμενη ημερομηνία
    prev_date_map = dict(zip(unique_dates[1:], unique_dates.shift()[1:]))
    # νέα στήλη με την προηγούμενη ημερομηνία
    group["previous_purchase_date_product"] = group["date"].map(prev_date_map)
    
    # διαφορά ημερών
    #group["DaysSincePrevdate"] = (group["date"] - group["previous_purchase_date_customer"]).dt.days
    return group

    
def aggr(dataframe, user_col, date_col, col_of_interest, aggre):

  for i in range(len(aggre)):
      difference_df  =  (dataframe.groupby([user_col, date_col])[col_of_interest].agg([aggre[i]])
                          .reset_index()
                          .groupby(user_col)[aggre[i]].shift(0) - 
                          dataframe.groupby([user_col, date_col])[col_of_interest].agg([aggre[i]])
                          .reset_index()
                          .groupby(user_col)[aggre[i]].shift(1))

      #
      user_df = dataframe.groupby([user_col, date_col])[col_of_interest].agg([aggre[i]]).reset_index()

      df =  user_df.merge(difference_df, how='inner', right_index=True, left_index=True)
      df = df.rename(columns = {aggre[i] + '_y': col_of_interest + '_' + aggre[i] + '_difference', aggre[i] + '_x': col_of_interest + '_' + aggre[i] + '_per_dayorder'})
      col_to_keep = [user_col, date_col,col_of_interest + '_' + aggre[i] + '_difference', col_of_interest + '_' + aggre[i] + '_per_dayorder']
      dataframe = dataframe.merge(df[col_to_keep], how = 'inner', on = [user_col, date_col])
      
  return dataframe
    

def time_difference_between_transactions(dataframe, user_col, date_col):
    
    dataframe[date_col] = pd.to_datetime(dataframe[date_col])
    difference_df = (dataframe.sort_values([user_col, date_col])
                    .drop_duplicates([user_col, date_col])
                    .groupby(user_col).date.diff()
                    .reset_index(drop=True))

    user_date_df = dataframe.groupby([user_col, date_col])[date_col].count().reset_index(name = 'Count')

    user_time_df =  user_date_df.merge(difference_df, how='inner', right_index=True, left_index=True).drop('Count', axis = 1)
    user_time_df = user_time_df.rename(columns = {date_col + '_x' : date_col , date_col + '_y':'time_difference'})
    user_time_df.time_difference = user_time_df.time_difference.apply(lambda x: pd.Timedelta(x).days)
  #  user_time_df.time_differene = user_time_df.time_difference.\
   #     fillna(np.round(user_time_df[~user_time_df.time_difference.isna()]['time_difference'].mean()))
    #user_time_df.loc[:, 'time_difference'] = user_time_df.groupby(user_col, group_keys=False)['time_difference'].apply(lambda x: x.fillna(x.mean()))
    
    df = dataframe.merge(user_time_df, how='inner', on = [user_col, date_col])   
    df = df.sort_values(by = [user_col, date_col])

    return df


def apply_differences(df, user_col, date_col, **kwargs):
    margin_col = kwargs.get('margin_col', 'margin')
    menge_col = kwargs.get('menge_col', 'quantity')
    sales_col = kwargs.get('sales_col', 'price')

    df = aggr(col_of_interest = sales_col, dataframe=df, user_col=user_col, date_col=date_col, aggre=['mean', 'sum', 'max', 'min', 'median'])
    df = aggr(col_of_interest = menge_col, dataframe=df, user_col=user_col, date_col=date_col, aggre=['mean', 'sum', 'max', 'min', 'median'])
    df = aggr(col_of_interest = margin_col, dataframe=df, user_col=user_col, date_col=date_col, aggre=['mean', 'sum', 'max', 'min', 'median'])
    df = time_difference_between_transactions(df, user_col=user_col, date_col=date_col)
    df = df.drop_duplicates([user_col, date_col], keep = 'last')
    
    return df
