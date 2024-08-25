import os
import requests
from base64 import b64encode

# GitHub credentials and repository information
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
REPO_OWNER = 'AdrienSourdilleTIL'  # Replace with your GitHub username
REPO_NAME = 'RTE-project'  # Replace with your repository name
FILE_PATH = 'electricity_consumption_chart.png'  # Path to the file in the repo

# Function to update the chart in the GitHub repository
def update_chart_on_github(file_path):
    # Read the new chart file
    with open(file_path, 'rb') as file:
        content = file.read()

    # Encode the file content in base64
    encoded_content = b64encode(content).decode('utf-8')

    # Define the GitHub API URL for the file
    url = f'https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{FILE_PATH}'

    # Fetch the current file information
    response = requests.get(url, headers={'Authorization': f'token {GITHUB_TOKEN}'})
    
    if response.status_code == 200:
        file_info = response.json()
        sha = file_info['sha']  # SHA of the current file

        # Prepare the data to update the file
        data = {
            'message': 'Update electricity consumption chart',
            'content': encoded_content,
            'sha': sha
        }
    else:
        # Prepare the data to create the file
        data = {
            'message': 'Add electricity consumption chart',
            'content': encoded_content
        }
    
    # Update or create the file
    response = requests.put(url, json=data, headers={'Authorization': f'token {GITHUB_TOKEN}'})
    
    if response.status_code in [200, 201]:
        print("Chart updated successfully in the GitHub repository.")
    else:
        print(f"Failed to update chart on GitHub: {response.content}")

# Call the function to update the chart
update_chart_on_github('electricity_consumption_chart.png')
