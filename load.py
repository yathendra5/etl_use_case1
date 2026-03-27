# load.py
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
 
# DB Config
DB_USER = "root"
DB_PASS = "123"
DB_HOST = "localhost"
DB_NAME = "etl_db"
 
def get_engine():
    password = quote_plus(DB_PASS)   
    conn_str = f"mysql+pymysql://{DB_USER}:{password}@{DB_HOST}/{DB_NAME}"
    return create_engine(conn_str)
 
def load(expire_df, insert_df, change_df):
    engine = get_engine()
 
    with engine.begin() as conn:  
 
        if not expire_df.empty:
            for _, r in expire_df.iterrows():
                conn.execute(text("""
                    UPDATE dim_customer
                    SET is_current = 0,
                        end_date = :end_date
                    WHERE customer_id = :customer_id
                    AND is_current = 1
                """), {
                    "end_date": r["end_date"],
                    "customer_id": r["customer_id"]
                })
 
            print(f"[LOAD] Expired {len(expire_df)} rows")
 
        
        if not insert_df.empty:
            if 'effective_date' in insert_df.columns:
                insert_df = insert_df.drop(columns=['effective_date'])
 
            insert_df.to_sql(
                'dim_customer',
                conn,
                if_exists='append',
                index=False
            )
 
            print(f"[LOAD] Inserted {len(insert_df)} rows")
 
        
        if not change_df.empty:
            change_df.to_sql(
                'dim_customer_change_log',
                conn,
                if_exists='replace',
                index=False
            )
 
            print(f"[LOAD] Logged {len(change_df)} changes")
 
 