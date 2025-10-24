# score_feature_engineer.py

import pandas as pd
from scipy import stats
from churn_functions.differences import (
    add_prev_date_customer,
    add_prev_date_customer_product,
    add_prev_date_product,
)
from recommendations_score.feature_helpers import (
    assign_order,
    assign_product_order,
    calculate_customer_reordered,
    calculate_product_reordered,
    customer_product_preference,
)


class SalesFeatureEngineer:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.df["date"] = pd.to_datetime(self.df["date"])

    # ==========================================================
    def add_time_features(self):
        df = self.df
        df["order_id"] = (
            pd.factorize(pd.MultiIndex.from_frame(df[["invoice","customer_id","date"]]))[0] + 1
        )

        df['last_transaction_date'] = df.groupby(['customer_id']).date.transform('max')

        df = df.groupby("customer_id", group_keys=False).apply(add_prev_date_customer)
        df['time_diff_customer'] = df['date'] - df['previous_purchase_date_customer']
        df['time_diff_customer'] = df['time_diff_customer'].fillna(pd.Timedelta(days=-1))
        df["time_diff_customer"] = df["time_diff_customer"].dt.days
        df = df.groupby(['customer_id', 'product_id'], group_keys=False).apply(add_prev_date_customer_product)
        df['time_diff_customer_product'] = df['date'] - df['previous_purchase_date_customer_product']
        df['time_diff_customer_product'] = df['time_diff_customer_product'].fillna(pd.Timedelta(days=-1))
        df["time_diff_customer_product"] = df["time_diff_customer_product"].dt.days
        df = df.groupby('product_id', group_keys=False).apply(add_prev_date_product)
        df['time_diff_product'] = df['date'] - df['previous_purchase_date_product']
        df['time_diff_product'] = df['time_diff_product'].fillna(pd.Timedelta(days=-1))
        df["time_diff_product"] = df["time_diff_product"].dt.days
        df['reordered'] = df.apply(lambda x: 1 if x['time_diff_customer_product'] != -1 else 0, axis=1)
        self.df = df
        return self

    # ==========================================================
    def add_customer_features(self):
        df = self.df

        # Customer-level
        df["last_transaction_date"] = df.groupby("customer_id").date.transform("max")
        df["first_transaction_date"] = df.groupby("customer_id").date.transform("min")
        df["total_transaction_dates"] = df.groupby("customer_id").date.transform(lambda x: x.nunique())

        # Customer-Product level
        df["last_transaction_date_cxp"] = df.groupby(["customer_id","product_id"]).date.transform("max")
        df["first_transaction_date_cxp"] = df.groupby(["customer_id","product_id"]).date.transform("min")
        df["total_transaction_dates_cxp"] = df.groupby(["customer_id","product_id"]).date.transform(lambda x: x.nunique())

        # Product-level
        df["last_transaction_date_product"] = df.groupby("product_id").date.transform("max")
        df["first_transaction_date_product"] = df.groupby("product_id").date.transform("min")
        df["total_transaction_dates_product"] = df.groupby("product_id").date.transform(lambda x: x.nunique())

        # Days between first and last
        df["days_between_first_last_cxp"] = (df["last_transaction_date_cxp"] - df["first_transaction_date_cxp"]).dt.days
        df["days_between_first_last_customer"] = (df["last_transaction_date"] - df["first_transaction_date"]).dt.days
        df["days_between_first_last_product"] = (df["last_transaction_date_product"] - df["first_transaction_date_product"]).dt.days

        self.df = df
        return self

    # ==========================================================
    def add_behavioral_features(self):
        df = self.df

        # Order numbering
        df["order_number"] = df.groupby("customer_id", group_keys=False).apply(assign_order)
        df["product_order_number"] = df.groupby("product_id", group_keys=False).apply(assign_product_order)

        # Temporal / behavioral
        df['limit_order_number_customer'] = df.groupby('customer_id').order_number.transform('max')# - 2
        df['order_dow'] = df.date.dt.day_of_week
        df['order_day_name'] = df.date.dt.day_name()
        df['total_orders_customer'] = df.groupby('customer_id').order_id.transform(lambda x: x.nunique())
        df['total_orders_product'] = df.groupby('product_id').order_id.transform(lambda x: x.nunique())
        df['total_products_in_order'] = df.groupby(by=['customer_id', 'order_id'])['product_id'].transform('count')
        df["avg_number_of_products_per_customer_order"] = (
                df.groupby(by=['customer_id'])['total_products_in_order'].transform('mean')
        )
        df['dow_with_most_orders_per_customer'] = df.groupby(by=['customer_id'])['order_dow'].transform(lambda x : stats.mode(x)[0])

        df["product_reordered_by_customer"] = df.groupby(["customer_id","product_id"])["reordered"].transform("max")
        df["total_customer_products"] = df.groupby("customer_id")["product_id"].transform("nunique")
        df["total_product_customers"] = df.groupby("product_id")["customer_id"].transform("nunique")

        # Reordered ratios
        customer_reordered = calculate_customer_reordered(df)
        df = df.merge(customer_reordered, how="inner", on="customer_id")
        df["customer_reordered_product_ratio"] = df["total_reordered_customer"] / df["total_customer_products"]

        product_reordered = calculate_product_reordered(df)
        df = df.merge(product_reordered, how="inner", on="product_id")
        df["product_reordered_by_customer_ratio"] = df["total_reordered_product"] / df["total_product_customers"]

        # More behavioral features (order_range, cxp_times_bought, ux_p)
        df['cxp_times_bought'] = df.groupby(['customer_id', 'product_id'])['order_id'].transform(lambda x: x.nunique())
        df['cxp_first_order_number'] = df.groupby(by=['customer_id', 'product_id'])['order_number'].transform('min')
        df['order_range'] = df['total_orders_customer'] - df['cxp_first_order_number'] + 1
        df['uxp_reorder_ratio'] = df['cxp_times_bought'] / df['order_range']
        df['mean_uxp_reorder_ratio_for_product'] = df.groupby('product_id').uxp_reorder_ratio.transform('mean')
        df['mean_order_range_for_product'] = df.groupby('product_id').order_range.transform('mean')
        df['mean_cxp_times_bought_for_product'] = df.groupby('product_id').cxp_times_bought.transform('mean')
        #
        df['median_uxp_reorder_ratio_for_product'] = df.groupby('product_id').uxp_reorder_ratio.transform('median')
        df['median_order_range_for_product'] = df.groupby('product_id').order_range.transform('median')
        df['median_cxp_times_bought_for_product'] = df.groupby('product_id').cxp_times_bought.transform('median')
        df['order_number_back'] = df.groupby(by=['customer_id'])['order_number'].transform(max) - df.order_number + 1
        df['median_days_between_trans_customer'] = df.groupby(['customer_id']).time_diff_customer.transform('median')
        df['mean_days_between_trans_customer'] = df.groupby(['customer_id']).time_diff_customer.transform('mean')
        df['max_days_between_trans_customer'] = df.groupby(['customer_id']).time_diff_customer.transform('max')
        df['min_days_between_trans_customer'] = df.groupby(['customer_id']).time_diff_customer.transform('min')
        df['std_days_between_trans_customer'] = df.groupby(['customer_id']).time_diff_customer.transform('std')
        #
        df['median_days_between_trans_product'] = df.groupby(['customer_id']).time_diff_product.transform('median')
        df['mean_days_between_trans_product'] = df.groupby(['customer_id']).time_diff_product.transform('mean')
        df['max_days_between_trans_product'] = df.groupby(['customer_id']).time_diff_product.transform('max')
        df['min_days_between_trans_product'] = df.groupby(['customer_id']).time_diff_product.transform('min')
        df['std_days_between_trans_product'] = df.groupby(['customer_id']).time_diff_product.transform('std')
        #
        df['median_days_between_trans_customer_product'] = df.groupby(['customer_id']).time_diff_customer_product.transform('median')
        df['mean_days_between_trans_customer_product'] = df.groupby(['customer_id']).time_diff_customer_product.transform('mean')
        df['max_days_between_trans_customer_product'] = df.groupby(['customer_id']).time_diff_customer_product.transform('max')
        df['min_days_between_trans_customer_product'] = df.groupby(['customer_id']).time_diff_customer_product.transform('min')
        df['std_days_between_trans_customer_product'] = df.groupby(['customer_id']).time_diff_customer_product.transform('std')
        df['quarter'] = df.date.dt.quarter
        df = df.drop(columns=['limit_order_number_customer'])
        df['total_orders_in_quarter'] = df.groupby(['customer_id', 'quarter']).order_id.transform(lambda x: x.nunique())
        df['total_orders_in_month'] = df.groupby(['customer_id', 'month']).order_id.transform(lambda x: x.nunique())
        df['orders_ratio_in_quarter'] = df['total_orders_in_quarter'] / df['total_orders_customer']
        df['orders_ratio_in_month'] = df['total_orders_in_month'] / df['total_orders_customer']
        df['mean_ratio_in_quarter_customer'] = df.groupby(['customer_id',  'product_id']).orders_ratio_in_quarter.transform('mean')
        df['mean_ratio_in_month_customer'] = df.groupby(['customer_id', 'product_id']).orders_ratio_in_month.transform('mean')
        df['mean_quantity_cxp'] = df.groupby(['customer_id', 'product_id']).quantity.transform('mean')
        df['median_quantity_cxp'] = df.groupby(['customer_id', 'product_id']).quantity.transform('median')
        df['max_quantity_cxp'] = df.groupby(['customer_id', 'product_id']).quantity.transform('max')
        df['total_orders_in_quarter_cxp'] = df.groupby(['customer_id', 'product_id', 'quarter']).order_id.transform(lambda x: x.nunique())
        df['total_orders_in_month_cxp'] = df.groupby(['customer_id', 'product_id', 'month']).order_id.transform(lambda x: x.nunique())
        df['orders_ratio_in_quarter_cxp'] = df['total_orders_in_quarter_cxp'] / df['cxp_times_bought']
        df['orders_ratio_in_month_cxp'] = df['total_orders_in_month_cxp'] / df['cxp_times_bought']
        df['mean_ratio_in_quarter_cxp'] = df.groupby(['customer_id',  'product_id']).orders_ratio_in_quarter_cxp.transform('mean')
        df['mean_ratio_in_month_cxp'] = df.groupby(['customer_id', 'product_id']).orders_ratio_in_month_cxp.transform('mean')

        self.df = df
        return self

    # ==========================================================
    def add_preference_score(self):
        df = self.df
        df["preference_score"] = df.apply(customer_product_preference, axis=1)
        self.df = df
        return self

    # ==========================================================
    def get_dataframe(self) -> pd.DataFrame:
        return self.df
