import pandas as pd
from datetime import timedelta
def transform_simple(dim_df,updates_df):
    updates_df = updates_df.apply(lambda col:col.str.strip() if col.dtype  == 'object' else col)

    current_df =dim_df[dim_df['is_current'] == 1]

    expire_rows=[]
    insert_rows =[]

    for _,new in updates_df.iterrows():

        cust_id = new['customer_id']
        effective_date=pd.to_datetime(new['effective_date'])
        old=current_df[current_df['customer_id'] == cust_id]


        if old.empty:
            new_row = new.copy()
            new_row['is_current'] = 1
            new_row['start_date']= effective_date
            new_row['end_date'] = None
            insert_rows.append(new_row)
        else:
            old_row=old.iloc[0]

        if(
            old_row['city'] != new['city']or
            old_row['loyalty_tier'] != new['loyalty_tier'] or
            old_row['annual_income_band'] !=new['annual_income_band']
        ):
            expire_rows.append({
                'customer_id':cust_id,
                "end_date":effective_date-timedelta(days=1)
            } )
            new_row = new.copy()
            new_row['is_current'] = 1
            new_row['start_date']= effective_date
            new_row['end_date'] = None
            insert_rows.append(new_row)


    expire_df = pd.DataFrame(expire_rows)
    insert_df = pd.DataFrame(insert_rows)

    return expire_df,insert_df




