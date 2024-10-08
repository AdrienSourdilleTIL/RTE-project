import os
import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta
import pytz
import requests
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


TOKEN_URL = 'https://digital.iservices.rte-france.com/token/oauth/'
API_URL = 'https://digital.iservices.rte-france.com/open_api/consumption/v1/short_term'

from dotenv import load_dotenv
from pathlib import Path
from dotenv import dotenv_values


# Specify the path to your .env file
dotenv_path = Path(r'C:\Users\AdrienSourdille\Documents\GitHub\RTE-project\.venv\.env')

config = dotenv_values(dotenv_path)
print(config)

# Load the .env file
load_dotenv(dotenv_path=dotenv_path)

# Retrieve API credentials from environment variables
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

# Retrieve Snowflake credentials from environment variables
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
SNOWFLAKE_TABLE = os.getenv('SNOWFLAKE_TABLE')  # Table name you created


# Function to get the OAuth2 token
def get_token():
    response = requests.post(
        TOKEN_URL,
        data={'grant_type': 'client_credentials'},
        auth=(CLIENT_ID, CLIENT_SECRET)
    )
    print(f"Token request URL: {TOKEN_URL}")
    print(f"Token request status code: {response.status_code}")
    if response.status_code == 200:
        print('token retrieved succesfully')
        return response.json().get('access_token')
    else:
        print(f"Failed to retrieve token: {response.status_code}")
        return None

# Function to fetch data from API
def fetch_data():
    token = get_token()
    if token is None:
        print("No token available. Exiting.")
        return None

    now = datetime.now(pytz.UTC)
    end_date = now.strftime('%Y-%m-%dT%H:%M:%S%z')
    start_date = (now - timedelta(hours=48)).strftime('%Y-%m-%dT%H:%M:%S%z')

    # Adjust timezone format to ±HH:MM
    end_date = end_date[:22] + ":" + end_date[22:]
    start_date = start_date[:22] + ":" + start_date[22:]

    params = {
        'type': 'REALISED,ID',
        'start_date': start_date,
        'end_date': end_date
    }

    headers = {
        'Authorization': f'Bearer {token}',
        'Host': 'digital.iservices.rte-france.com'
    }

    response = requests.get(API_URL, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

# Function to extract and convert lists under 'values' to DataFrame
def extract_values_to_dataframe(data):
    if data is None:
        print("No data to convert.")
        return None

    records = data.get('short_term', [])

    # Initialize list to hold values
    all_values = []

    for record in records:
        values = record.get('values', [])
        all_values.extend(values)  # Flatten the list of lists

    # Create DataFrame from the flattened list of values
    df = pd.DataFrame(all_values)
    
    # Capitalize the column names
    df.columns = [col.upper() for col in df.columns]
    
    return df

# Function to upload DataFrame to Snowflake using a merge operation
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
        # Use a temporary table to store the incoming data
        temp_table = "TEMP_ELECTRICITY_CONSUMPTION"
        
        # Create temporary table (if not exists)
        conn.cursor().execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE {temp_table} LIKE {SNOWFLAKE_TABLE}
        """)
        
        # Use the Snowflake Connector's write_pandas function to insert data into the temp table
        from snowflake.connector.pandas_tools import write_pandas
        success, nchunks, nrows, _ = write_pandas(conn, df, temp_table)
        
        # Perform a merge to insert only the new rows
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

# Function to generate a chart from the DataFrame
def generate_chart(df):
    if df is None:
        print("No DataFrame to plot.")
        return

    # Convert 'START_DATE' to datetime
    df['START_DATE'] = pd.to_datetime(df['START_DATE'], utc=True)

    # Find the latest date in 'START_DATE'
    latest_date = df['START_DATE'].dt.date.max()
    print(f"Latest date identified: {latest_date}")

    # Filter the DataFrame to keep only rows with the latest date
    df = df[df['START_DATE'].dt.date == latest_date]

    # Remove duplicates based on 'START_DATE'
    df = df.drop_duplicates(subset='START_DATE', keep='first')

    # Check if the DataFrame is empty after filtering
    if df.empty:
        print("No data available after filtering. No chart will be generated.")
        return

    # Remove 'END_DATE' and 'UPDATED_DATE' columns
    df = df.drop(columns=['END_DATE', 'UPDATED_DATE'])

    # Set 'START_DATE' as the index
    df.set_index('START_DATE', inplace=True)

    # Create the plot
    plt.figure(figsize=(12, 8))
    plt.plot(df.index, df['VALUE'], marker='o', linestyle='-', color='royalblue', linewidth=2, markersize=6, label='Energy Consumption')

    # Add titles and labels
    plt.title(f'Energy Consumption on {latest_date}', fontsize=16, fontweight='bold')
    plt.xlabel('Time (UTC)', fontsize=14)
    plt.ylabel('Total Consumption (MWh)', fontsize=14)

    # Format the x-axis to show hours and minutes with AM/PM
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%I %p'))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=1))  # Show a tick for every hour

    # Improve the appearance of the x-axis and y-axis ticks
    plt.xticks(rotation=45, fontsize=12)
    plt.yticks(fontsize=12)

    # Add a legend and gridlines
    plt.legend(fontsize=12)
    plt.grid(True, which='both', linestyle='--', linewidth=0.7)

    # Save the chart
    plt.tight_layout()
    plt.savefig(f'electricity_consumption_chart.png', dpi=300)
    plt.close()

    print(f"New chart saved as 'electricity_consumption_chart.png'.")

# Main execution
def main():
    print("Fetching data from API...")
    data = fetch_data()
    
    print("Extracting values and converting to DataFrame...")
    df = extract_values_to_dataframe(data)
    
    if df is not None:
        print("DataFrame created successfully.")
        print(df.tail())  # Display the last few rows of the DataFrame
        
        print("Uploading DataFrame to Snowflake...")
        upload_dataframe_to_snowflake(df)

        print("Generating chart...")
        generate_chart(df)
    else:
        print("Failed to create DataFrame.")

if __name__ == "__main__":
    main()
