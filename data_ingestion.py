import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials from environment variables
CLIENT_ID = os.getenv('ID_CLIENT')
CLIENT_SECRET = os.getenv('ID_SECRET')
TOKEN_URL = 'https://digital.iservices.rte-france.com/token/oauth/'  # Update if necessary
API_URL = 'https://digital.iservices.rte-france.com/open_api/consumption/v1/short_term'

# Function to get the OAuth2 token
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

# Function to fetch data
def fetch_data():
    token = get_token()
    if token is None:
        print("No token available. Exiting.")
        return
    
    # Calculate the current time and the start time 48 hours ago
    now = datetime.now(pytz.UTC)  # Use timezone-aware UTC time
    end_date = now.strftime('%Y-%m-%dT%H:%M:%S%z')  # Format in ISO8601 with timezone
    start_date = (now - timedelta(hours=48)).strftime('%Y-%m-%dT%H:%M:%S%z')
    
    # Adjust timezone format to ±HH:MM
    end_date = end_date[:22] + ":" + end_date[22:]
    start_date = start_date[:22] + ":" + start_date[22:]
    
    # Define the parameters for the API request
    params = {
        'type': 'REALISED,ID',
        'start_date': start_date,
        'end_date': end_date
    }
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Host': 'digital.iservices.rte-france.com'
    }
    
    print(f"Making request to {API_URL} with params {params}")
    response = requests.get(API_URL, headers=headers, params=params)

    # Print status code and response content for debugging
    print(f"Status Code: {response.status_code}")
    print("Response Content:", response.text)
    
    if response.status_code == 200:
        print("Data fetched successfully!")
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

# Run the function to test
if __name__ == "__main__":
    data = fetch_data()
    if data:
        print(data)  # Process and store data as needed
