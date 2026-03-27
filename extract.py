import pandas as pd
import mysql.connector

def customer_dim():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123",
        database="etl_db"
    )
    df = pd.read_sql("SELECT * FROM dim_customer ", conn)
    conn.close()
    df1=pd.read_csv('task2.26_03/customer_updates.csv')
    return df ,df1


