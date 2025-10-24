# feature_helpers.py

import pandas as pd
import numpy as np
from scipy import stats

# === Order numbering per customer ===
def assign_order(group: pd.DataFrame) -> pd.Series:
    group = group.copy()
    group["first_date"] = group.groupby(["invoice"])["date"].transform("min")
    group["first_pos"] = group.groupby(["invoice"])["date"].transform(lambda x: x.idxmin())
    keys = pd.MultiIndex.from_arrays([group["first_date"], group["first_pos"]])
    return pd.Series(pd.factorize(keys)[0] + 1, index=group.index)

# === Order numbering per product ===
def assign_product_order(group: pd.DataFrame) -> pd.Series:
    group = group.copy()
    group["first_date"] = group.groupby(["invoice"])["date"].transform("min")
    group["first_pos"] = group.groupby(["invoice"])["date"].transform(lambda x: x.idxmin())
    keys = pd.MultiIndex.from_arrays([group["first_date"], group["first_pos"]])
    return pd.Series(pd.factorize(keys)[0] + 1, index=group.index)

# === Mode for day-of-week ===
def most_common_dow(series: pd.Series) -> int:
    return stats.mode(series)[0][0]

# === Customer reordered summary ===
def calculate_customer_reordered(df: pd.DataFrame) -> pd.DataFrame:
    customer_reordered = (
        df.drop_duplicates(subset=["customer_id", "product_id", "product_reordered_by_customer"])
        .groupby(["customer_id", "product_reordered_by_customer"])
        .product_reordered_by_customer.value_counts()
        .unstack(fill_value=0)
        .reset_index()
        .rename(
            columns={
                "product_reordered_by_customer": "index",
                0: "total_not_reordered_customer",
                1: "total_reordered_customer",
            }
        )
    )
    return customer_reordered

# === Product reordered summary ===
def calculate_product_reordered(df: pd.DataFrame) -> pd.DataFrame:
    product_reordered = (
        df.drop_duplicates(subset=["product_id", "customer_id", "product_reordered_by_customer"])
        .groupby(["product_id"])
        .product_reordered_by_customer.value_counts()
        .unstack(fill_value=0)
        .reset_index()
        .rename(
            columns={
                "product_reordered_by_customer": "index",
                0: "total_not_reordered_product",
                1: "total_reordered_product",
            }
        )
        .sort_values(by="total_reordered_product")
    )
    return product_reordered

# === Preference score ===
def customer_product_preference(row: pd.Series) -> float:
    w_times_bought = 0.3
    w_reorder_ratio = 0.3
    w_days_between = 0.2
    w_customer_ratio = 0.2

    times_bought = np.log1p(row.get("cxp_times_bought", 0)) / np.log1p(50)
    reorder_ratio = row.get("uxp_reorder_ratio", 0)
    days_between = 1 - min(row.get("mean_days_between_trans_customer_product", 999)/100, 1)
    customer_ratio = row.get("customer_reordered_product_ratio", 0)

    score = (
        w_times_bought * times_bought
        + w_reorder_ratio * reorder_ratio
        + w_days_between * days_between
        + w_customer_ratio * customer_ratio
    )
    return np.clip(score * 5, 1, 5)
