import pandas as pd
import mysql.connector

def read_orders():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Yathendra#9346",
        database="etl_db"
    )
    df = pd.read_sql("SELECT * FROM orders", conn)
    conn.close()
    return df

def read_order_items():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Yathendra#9346",
        database="etl_db"
    )
    df = pd.read_sql("SELECT * FROM order_items", conn)
    conn.close()

    return df
