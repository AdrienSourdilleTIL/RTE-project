import os
import snowflake.connector
from dotenv import load_dotenv
from api_call import extract_values_to_dataframe

# Attempt to load .env file if it exists (for local development)
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

# Environment variables from GitHub Secrets will override .env values
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
SNOWFLAKE_TABLE = os.getenv('ELECTRICITY_CONSUMPTION')

# upload to snowflake

def upload_dataframe_to_snowflake(df):
    if df is None:
        print("No DataFrame to upload.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    try:
        temp_table = "TEMP_ELECTRICITY_CONSUMPTION"
        conn.cursor().execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE {temp_table} LIKE {SNOWFLAKE_TABLE}
        """)
        
        from snowflake.connector.pandas_tools import write_pandas
        success, nchunks, nrows, _ = write_pandas(conn, df, temp_table)
        
        merge_query = f"""
            MERGE INTO {SNOWFLAKE_TABLE} AS target
            USING {temp_table} AS source
            ON target.START_DATE = source.START_DATE 
               AND target.END_DATE = source.END_DATE
            WHEN NOT MATCHED THEN
            INSERT (START_DATE, END_DATE, UPDATED_DATE, VALUE)
            VALUES (source.START_DATE, source.END_DATE, source.UPDATED_DATE, source.VALUE);
        """
        
        conn.cursor().execute(merge_query)
        print(f"Data merged successfully: {nrows} new rows inserted.")
        
    except Exception as e:
        print(f"Failed to upload DataFrame to Snowflake: {e}")
    finally:
        conn.close()
