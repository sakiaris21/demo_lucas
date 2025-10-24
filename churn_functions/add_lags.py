import pandas as pd


def add_lags(churn: pd.DataFrame) -> pd.DataFrame:

    # build the lagged columns
    not_lag_cols = [
            "customer_id",
            "year",
            "month",
            "quarter",
            "date",
            "date_to_check",
            "risk",
            "risk_value",
            "max_months",
            'lenght_last_zero_months',
            'last_zero_months_start_at',
            'last_zero_months_end_at',
            'last_zero_month',
            'first_zero_month',
            'max_consecutive_zero_months',
            'avgPriceYTD',
            'avgPriceHYTD',
            'avgPrice2Year',
            'ppb',
            'customer_sales_category'       
    ]

    cols_to_shift = [
        col
        for col in churn.columns
        if ((col not in not_lag_cols) )
    ]

    lags = list(range(1, 13, 1))
    for lag in lags:
        new_cols_name = [col + " (t-{})".format(lag) for col in cols_to_shift]
        churn[new_cols_name] = churn[cols_to_shift]
        churn[new_cols_name] = churn.groupby(["customer_id"])[
            new_cols_name
        ].shift(lag)

    churn.date_to_check = pd.to_datetime(churn.date_to_check)

    churn.sort_values("date_to_check", inplace=True)
    churn = churn.reset_index(drop=True)

    return churn
