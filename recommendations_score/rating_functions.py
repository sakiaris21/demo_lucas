import numpy as np
import pandas as pd


def safe_log1p(series):
    # deals with NAs etc
    s = series.fillna(0).astype(float)
    s[s < 0] = 0
    return np.log1p(s)

def minmax_scale(series):
    s = series.fillna(series.min() if not pd.isna(series.min()) else 0).astype(float)
    denom = (s.max() - s.min()) + 1e-9
    return (s - s.min()) / denom

def compute_rating(df, weights=None, recency_decay_days=30):
    """
    Returns df with new column 'rating' into [1,5] range.
    Uses adapted weights (dictionary).
    Moreover it returns temporary scores as columns for debugging.
    """
    df = df.copy()

    # default weights (values can be changes)
    default_w = {
        "freq": 0.26,
        "recency": 0.21,
        "reorder": 0.16,
        "quantity": 0.11,
        "monetary": 0.11,
        "user_activity": 0.08,
        "product_popularity": 0.07,
    }
    if weights is None:
        weights = default_w
    else:
        # combination with defaults for missing keys
        tmp = default_w.copy()
        tmp.update(weights)
        weights = tmp

    # --- Score components (checking if column exists) ---
    # Frequency: cxp_times_bought , total_transaction_dates_customer_product κλπ.
    if "cxp_times_bought" in df.columns:
        freq = safe_log1p(df["cxp_times_bought"])
    elif "total_transaction_dates_cxp" in df.columns:
        freq = safe_log1p(df["total_transaction_dates_cxp"])
    else:
        freq = pd.Series(0, index=df.index)

    # Recency: time_diff_customer_product (in days)
    if "time_diff_customer_product" in df.columns:
        recency = df["time_diff_customer_product"].astype(float).fillna(recency_decay_days * 3)
    elif "time_diff_customer" in df.columns:
        recency = df["time_diff_customer"].astype(float).fillna(recency_decay_days * 3)
    else:
        recency = pd.Series(recency_decay_days * 3, index=df.index)

    # modify into score: recently -> higher score
    recency_score = np.exp(- recency / float(recency_decay_days))

    # Reorder tendency
    if "product_reordered_by_customer_ratio" in df.columns:
        reorder = df["product_reordered_by_customer_ratio"].fillna(0).astype(float)
    elif "product_reordered_by_customer" in df.columns and "cxp_times_bought" in df.columns:
        reorder = df["product_reordered_by_customer"].fillna(0) / (df["cxp_times_bought"].replace(0,np.nan).fillna(1))
    else:
        reorder = pd.Series(0, index=df.index)

    # Quantity intensity
    if "quantity" in df.columns:
        quantity = safe_log1p(df["quantity"])
    elif "mean_quantity_cxp" in df.columns:
        quantity = safe_log1p(df["mean_quantity_cxp"])
    else:
        quantity = pd.Series(0, index=df.index)

    # Monetary: margin, price, cost
    if "margin" in df.columns:
        monetary = df["margin"].fillna(0)
    elif "price" in df.columns and "cost" in df.columns:
        monetary = (df["price"].fillna(0) - df["cost"].fillna(0)).clip(lower=0)
    else:
        monetary = pd.Series(0, index=df.index)

    # User activity and product popularity
    user_activity = df["total_orders_customer"] if "total_orders_customer" in df.columns else pd.Series(0, index=df.index)
    product_pop = df["total_orders_product"] if "total_orders_product" in df.columns else pd.Series(0, index=df.index)


    # --- into [0,1] ---
    freq_s = minmax_scale(freq)
    recency_s = minmax_scale(recency_score)  # recency_score already into 0-1 but minmax for stability
    reorder_s = minmax_scale(reorder)
    quantity_s = minmax_scale(quantity)
    monetary_s = minmax_scale(monetary)
    user_act_s = minmax_scale(user_activity)
    product_pop_s = minmax_scale(product_pop)

    # --- Combination with weights ---
    combined = (
        weights["freq"] * freq_s +
        weights["recency"] * recency_s +
        weights["reorder"] * reorder_s +
        weights["quantity"] * quantity_s +
        weights["monetary"] * monetary_s +
        weights["user_activity"] * user_act_s +
        weights["product_popularity"] * product_pop_s
    )

    # --- Rescaling into [1,5] decimal ---
    combined_norm = minmax_scale(combined)  # 0..1
    rating = 1.0 + combined_norm * 4.0  # 1..5

    # adding to df
    df["rating"] = rating.round(3)  # round to 3 digits

    # optional: add sub-scores for debugging/tuning
    df["_score_freq"] = freq_s
    df["_score_recency"] = recency_s
    df["_score_reorder"] = reorder_s
    df["_score_quantity"] = quantity_s
    df["_score_monetary"] = monetary_s
    df["_score_user_activity"] = user_act_s
    df["_score_product_pop"] = product_pop_s
    df["_combined_raw"] = combined

    return df