# main.py
from extract import customer_dim        # function from your extract.py
from transform import transform_simple  # function from your transform.py
from load import load                   # function from your load.py
import pandas as pd

if __name__ == "__main__":
    # -------------------------
    # 1️⃣ Extract
    # -------------------------
    dim_customer_df, updates_df = customer_dim()
    print(f"[EXTRACT] Loaded dim_customer ({len(dim_customer_df)} rows) and updates ({len(updates_df)} rows)")

    # -------------------------
    # 2️⃣ Transform
    # -------------------------
    expire_df, insert_df = transform_simple(dim_customer_df, updates_df)
    print(f"[TRANSFORM] Expire rows: {len(expire_df)}, Insert rows: {len(insert_df)}")

    # Create a simple change log DataFrame
    change_list = []

    for _, row in insert_df.iterrows():
        cust_id = row['customer_id']
        change_list.append({
            'customer_id': cust_id,
            'change_type': 'NEW_OR_UPDATED',
            'columns_changed': 'city/loyalty_tier/income_band' 
        })

    change_df = pd.DataFrame(change_list)
    print(f"[TRANSFORM] Change log rows: {len(change_df)}")

    # -------------------------
    # 3️⃣ Load
    # -------------------------
    load(expire_df, insert_df, change_df)
    print("[MAIN] ETL finished successfully!")