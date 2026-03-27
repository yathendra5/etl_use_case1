from sqlalchemy import create_engine

def load_to_mysql(df, table_name, user="root", password="Yathendra#9346", host="localhost", database="etl_db"):
    conn_str = f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"
    engine = create_engine(conn_str)
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    print(f"Loaded {len(df)} rows into table '{table_name}'")

def load(customer_summary, product_summary, daily_summary, cancellation_report):
    load_to_mysql(customer_summary, "customer_revenue")
    load_to_mysql(product_summary, "product_revenue")
    load_to_mysql(daily_summary, "daily_revenue")
    load_to_mysql(cancellation_report, "cancellation_report")
