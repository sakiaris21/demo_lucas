import pandas as pd


def find_churn_soft(df, user_col, **kwargs):
    sales_col = kwargs.get('sales_col', 'price')


    df['average_sales_12_transactions'] = (df.groupby(user_col)[sales_col + "_sum_per_dayorder"]
        .rolling(12, min_periods=1)
        .mean()
        .reset_index(0, drop=True)
)
    df.loc[
        df[sales_col + "_sum_per_dayorder"] < (df.average_sales_12_transactions / 10), "close_to_zero"
    ] = 1
    df.loc[df["close_to_zero"] == 1, "churn_soft"] = 1
    df.fillna(0, inplace=True)
    df.drop("close_to_zero", axis=1, inplace=True)
    return df



def find_churn(df):

    df.loc[
        df.days_from_last_order > 2 * df.time_bounds, "churn"
    ] = 1
    #df.loc[df["close_to_zero"].isnull()[::-1].idxmax(), "churn_soft"] = 1
    df.fillna(0, inplace=True)
    #df.drop("close_to_zero", axis=1, inplace=True)
    return df


def find_bounds(x):
    d_75 = x.quantile(0.75)
    d_25 = x.quantile(0.25)
    iqr = d_75 - d_25
    
    return d_75 + (1.5 * iqr)


def find_days_to_order(df, **kwargs):
    margin_col = kwargs.get('margin_col', 'margin')
    menge_col = kwargs.get('menge_col', 'quantity')
    sales_col = kwargs.get('sales_col', 'price')
    date_col = kwargs.get('date_col', 'date')
    user_col = kwargs.get('user_col', 'customer_number')
    df[user_col] = df[user_col].astype('str')
    time_df = df.loc[df["quantity"] != 0, [user_col, date_col, 'time_difference']].drop_duplicates(subset = date_col)  
    time_bounds_per_user = (time_df.sort_values(by = date_col)
                                    .groupby(user_col).time_difference
                                    .expanding(min_periods=2)
                                    .mean()
                                    .reset_index(name = 'time_bounds')
                                    .set_index('level_1')
                                    )

    time_bounds_per_user = (time_bounds_per_user
                                            .groupby(user_col, group_keys=False)['time_bounds']
                                            .apply(lambda x: x.fillna(x.mean()))
    )

    df_new = pd.concat([df, time_bounds_per_user], axis = 1)
    df_new["max_date_for_user"] = df_new.groupby([user_col]).date.transform('max')
    max_date = df_new.date.max()
    df_new['days_from_last_order'] = df_new.max_date_for_user.apply(lambda x: max_date - x)
    df_new['days_from_last_order'] = df_new['days_from_last_order'].apply(lambda x: pd.Timedelta(x).days)
    df_new = df_new.drop(['max_date_for_user'], axis = 1)
    df_new["time_bounds"] = df_new.groupby(user_col).time_bounds.transform(lambda x: x.ffill()).fillna(0)


    return df_new

def new_bounds(df):

    df.loc[
    df.time_bounds >= df.time_difference, 'time_bounds_2'
        ] = df.time_bounds

    df.loc[
    df.time_bounds < df.time_difference, 'time_bounds_2'
    ] = df.time_difference

    t = pd.DataFrame(df.groupby('userid')['time_bounds_2'].apply(lambda x: x.max())).reset_index()
    df_2 = df.merge(t, how='inner', on = 'userid')

    df_2 = df_2.drop(['time_bounds', 'time_bounds_2_x'], axis = 1).\
                    rename(columns = {'time_bounds_2_y':'time_bounds'})

    return df_2


def total_churn_per_user(df, user_col):

    df = find_churn_soft(df, user_col)
    soft_churn_per_user = (df.groupby([user_col]).churn_soft.sum()
                           .reset_index(name = 'total_soft_churn')
                          )
    df_new = df.merge(soft_churn_per_user, how = 'inner', on = user_col).drop('churn_soft', axis = 1)
    return df_new


def find_churn_label(df):
    df['churn'] = 0
    df.loc[df.days_from_last_order > 1.5 * df.time_bounds, "churn"] = -1
    df.loc[(df.days_from_last_order > df.time_bounds)&(df.days_from_last_order < 1.5 * df.time_bounds), "churn"] = 0
    df.loc[df.days_from_last_order < df.time_bounds, "churn"] = 1
    df.loc[df.days_from_last_order > 120, "churn"] = -1

    df.loc[df.time_bounds.isna(), "churn"] = -1
    return df 


def churn_label(x):
    if x > 1.3:
        return 'low_risk'
    elif (x <= 1.3) & (x >= 0.7):
        return 'medium_risk'
    else:
        return 'high_risk'
     