import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv

# Load environment variables
dotenv_path = r'C:\Users\AdrienSourdille\Documents\GitHub\RTE-project\.venv\Scripts\.env'
load_dotenv(dotenv_path)

CLIENT_ID = os.getenv('ID_CLIENT')
CLIENT_SECRET = os.getenv('ID_SECRET')
TOKEN_URL = 'https://digital.iservices.rte-france.com/token/oauth/'
API_URL = 'https://digital.iservices.rte-france.com/open_api/consumption/v1/short_term'

def get_token():
    response = requests.post(
        TOKEN_URL,
        data={'grant_type': 'client_credentials'},
        auth=(CLIENT_ID, CLIENT_SECRET)
    )
    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        print(f"Failed to retrieve token: {response.status_code}")
        return None

def fetch_data():
    token = get_token()
    if token is None:
        print("No token available. Exiting.")
        return None

    now = datetime.now(pytz.UTC)
    end_date = now.strftime('%Y-%m-%dT%H:%M:%S%z')
    start_date = (now - timedelta(hours=48)).strftime('%Y-%m-%dT%H:%M:%S%z')

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

def extract_values_to_dataframe(data):
    if data is None:
        print("No data to convert.")
        return None

    records = data.get('short_term', [])
    all_values = []

    for record in records:
        values = record.get('values', [])
        all_values.extend(values)

    df = pd.DataFrame(all_values)
    df.columns = [col.upper() for col in df.columns]
    
    return df
