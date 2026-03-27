import pandas as pd

def transform(orders_df, order_items_df):

    cancelled_df = orders_df[orders_df['status'] == 'Cancelled']
    valid_orders = orders_df[orders_df['status'] != 'Cancelled']

 
    merged = valid_orders.merge(order_items_df, on='order_id', how='inner')

    merged['total_revenue'] = merged['quantity'] * merged['unit_price']
    merged['discounted_revenue'] = (merged['total_revenue'] * (1 - merged['discount_pct']/100))
    merged['discounted_revenue']=merged['discounted_revenue'].round(2)

    # Customer Revenue Summary
    customer_revenue_summary = merged.groupby('customer_id').agg(
        total_orders=('order_id','nunique'),
        total_items_purchased=('quantity','sum'),
        total_revenue=('discounted_revenue','sum')
    ).reset_index()
    customer_revenue_summary = customer_revenue_summary.sort_values(by='total_revenue', ascending=False)
    customer_revenue_summary['rank'] = customer_revenue_summary['total_revenue'].rank(ascending=False, method='dense')

    # Product Revenue Summary
    product_revenue_summary = merged.groupby('product_id').agg(
        total_units_sold=('quantity','sum'),
        total_revenue=('discounted_revenue','sum')
    ).reset_index()
    product_revenue_summary = product_revenue_summary.sort_values(by='total_units_sold', ascending=False)

    # Daily Revenue Summary
    daily_revenue_summary = merged.groupby('order_date').agg(
        total_orders=('order_id','nunique'),
        total_revenue=('discounted_revenue','sum')
    ).reset_index()

    if not cancelled_df.empty:
        cancelled_merged = cancelled_df.merge(order_items_df, on='order_id', how='inner')
        # Calculate discounted revenue for cancelled orders as well
        cancelled_merged['total_revenue'] = cancelled_merged['quantity'] * cancelled_merged['unit_price']
        cancelled_merged['discounted_revenue'] = (cancelled_merged['total_revenue'] * (1 - cancelled_merged['discount_pct']/100)).round(2)

        cancellation_report = cancelled_merged.groupby('customer_id').agg(
            order_count=('order_id','nunique'),
            total_value=('discounted_revenue','sum')
        ).reset_index()
    else:
        cancellation_report = pd.DataFrame(columns=['customer_id', 'order_count', 'total_value'])

    return customer_revenue_summary, product_revenue_summary, daily_revenue_summary, cancellation_report
