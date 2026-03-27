from extract import read_orders, read_order_items
from transform import transform
from Load import load

def main():
    print("Extracting data...")
    orders_df = read_orders()
    order_items_df = read_order_items()

    print("Transforming data...")
    customer_summary, product_summary, daily_summary, cancellation_report = transform(orders_df, order_items_df)

    print("Loading data into MySQL...")
    load(customer_summary, product_summary, daily_summary, cancellation_report)

    print("ETL pipeline completed successfully!")

if __name__ == "__main__":
    main()
