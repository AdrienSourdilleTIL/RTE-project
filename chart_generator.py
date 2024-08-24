import os
import pandas as pd
import snowflake.connector
import matplotlib.pyplot as plt
from dotenv import load_dotenv


# Environment variables from GitHub Secrets will override .env values
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
SNOWFLAKE_TABLE = os.getenv('ELECTRICITY_CONSUMPTION')

def generate_chart():
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    query = f"""
    SELECT START_DATE, END_DATE, VALUE
    FROM {SNOWFLAKE_TABLE}
    ORDER BY START_DATE DESC
    LIMIT 100
    """
    
    df = pd.read_sql(query, conn)
    
    if df.empty:
        print("No data to plot.")
        return

    plt.figure(figsize=(10, 6))
    plt.plot(df['START_DATE'], df['VALUE'], marker='o', linestyle='-')
    plt.xlabel('Start Date')
    plt.ylabel('Value')
    plt.title('Electricity Consumption Over Time')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    chart_path = 'electricity_consumption_chart.png'
    plt.savefig(chart_path)
    print(f"Chart saved as {chart_path}")
    
    conn.close()
