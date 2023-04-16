import sqlite3
import pandas as pd

# Create a connection to the database
conn = sqlite3.connect('store_activity.db')

# Create a table to store the store status data
conn.execute('''
CREATE TABLE IF NOT EXISTS store_status (
    store_id INTEGER,
    timestamp_utc TEXT,
    status TEXT,
    PRIMARY KEY (store_id, timestamp_utc)
);
''')

# Create a table to store the store business hours data
conn.execute('''
CREATE TABLE IF NOT EXISTS store_business_hours (
    store_id INTEGER,
    day_of_week INTEGER,
    start_time_local TEXT,
    end_time_local TEXT,
    PRIMARY KEY (store_id, day_of_week)
);
''')

# Create a table to store the store timezones data
conn.execute('''
CREATE TABLE IF NOT EXISTS store_timezones (
    store_id INTEGER,
    timezone_str TEXT,
    PRIMARY KEY (store_id)
);
''')

# Load the data into the tables
store_status = pd.read_csv('store_status.csv')
store_status.to_sql('store_status', conn, if_exists='replace', index=False)

store_business_hours = pd.read_csv('store_hours.csv')
store_business_hours.to_sql('store_business_hours', conn, if_exists='replace', index=False)

store_timezones = pd.read_csv('store_timezone.csv')
store_timezones.to_sql('store_timezones', conn, if_exists='replace', index=False)

# Commit the changes and close the connection
conn.commit()
conn.close()
