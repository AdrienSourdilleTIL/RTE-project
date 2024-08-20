import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials from environment variables
CLIENT_ID = os.getenv('ID_CLIENT')
CLIENT_SECRET = os.getenv('ID_SECRET')
TOKEN_URL = 'https://digital.iservices.rte-france.com/token/oauth/'  # Verify the exact endpoint URL
API_URL = 'https://digital.iservices.rte-france.com/open_api/consumption/v1/short_term'  # Correct endpoint URL

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
        print("Response Content:", response.text)
        return None

def fetch_data(token):
    headers = {
        'Authorization': f'Bearer {token}',
        'Host': 'digital.iservices.rte-france.com'
    }
    
    print(f"Making request to {API_URL}")
    response = requests.get(API_URL, headers=headers)

    # Print status code and response content for debugging
    print(f"Status Code: {response.status_code}")
    print("Response Content:", response.text)
    
    if response.status_code == 200:
        print("Data fetched successfully!")
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

# Run the functions to get token and fetch data
if __name__ == "__main__":
    token = get_token()
    if token:
        data = fetch_data(token)
        if data:
            print(data)  # Process and store data as needed
