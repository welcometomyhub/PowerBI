from pandasql import sqldf
import pandas as pd

loc = r'E:\DA_Assignment\data_set_da_test.csv'
df = pd.read_csv(loc)
df = df.drop_duplicates(subset=['session', 'user', 'page_type', 'event_type'])
df.to_csv(r'E:\DA_Assignment\CleanedData.csv', index=False)

df_first_sessions = sqldf("SELECT DISTINCT(user) as user, MIN(event_date) AS first_session_date, "
                          "page_type as first_session_page, event_type as first_session_event FROM df GROUP by user")

df_last_sessions = sqldf("SELECT DISTINCT(user) as user, MAX(event_date) AS last_session_date FROM df GROUP by user")

df_orders_per_customer = sqldf("SELECT DISTINCT(user) as user, count(event_type) as total_orders FROM df"
                               " WHERE event_type='order' GROUP BY user")


df_sessions_per_customer = sqldf("SELECT DISTINCT(user) as user, count(session) as total_sessions FROM df"
                                 " GROUP BY user")

df_final = df.merge(df_first_sessions, on='user', how='left')\
        .merge(df_last_sessions, on='user', how='left')\
        .merge(df_orders_per_customer, on='user', how='left')\
        .merge(df_sessions_per_customer, on='user', how='left')

df_final.to_csv(r'E:\DA_Assignment\CleanedDataProcessed.csv', index=False)
