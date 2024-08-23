import snowflake.connector
import matplotlib.pyplot as plt
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from the specified .env file
dotenv_path = r'C:\Users\AdrienSourdille\Documents\GitHub\RTE-project\.venv\Scripts\.env'
load_dotenv(dotenv_path)

# Retrieve Snowflake credentials from environment variables
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

# Query to get data from Snowflake
QUERY = """
SELECT START_DATE, END_DATE, VALUE
FROM ELECTRICITY_CONSUMPTION
ORDER BY START_DATE DESC
LIMIT 100
"""

def fetch_data():
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    try:
        df = pd.read_sql(QUERY, conn)
    finally:
        conn.close()
    return df

def generate_chart(df):
    plt.figure(figsize=(10, 5))
    plt.plot(pd.to_datetime(df['START_DATE']), df['VALUE'], marker='o', linestyle='-')
    plt.xlabel('Start Date')
    plt.ylabel('Value')
    plt.title('Electricity Consumption Over Time')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('chart.png')

if __name__ == "__main__":
    df = fetch_data()
    generate_chart(df)
