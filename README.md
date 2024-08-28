# RTE Energy Consumption Dashboard

This project showcases an automated pipeline that queries the RTE (Réseau de Transport d'Électricité) API to retrieve daily energy consumption data. The data is then stored in a Snowflake database and visualized as a line graph, which is updated daily. The entire process is automated using GitHub Actions.

## Key Features

- **Automated Data Retrieval**: A Python script queries the RTE API daily to retrieve the latest energy consumption data.
- **Data Storage**: The retrieved data is securely stored in a Snowflake database.
- **Daily Updates**: A GitHub Actions workflow runs a YAML script that automates the entire process—fetching new data, updating the database, generating the graph, and committing the updated graph to the repository.
- **Continuous Visualization**: The line graph showing daily energy consumption is updated automatically and committed to the repo, ensuring that it always displays the most recent data.

## Project Workflow

1. **Data Extraction**: Python script queries the RTE API for daily energy consumption data.
2. **Data Storage**: The data is stored in Snowflake, ensuring a scalable and reliable database solution.
3. **Graph Generation**: A Python script generates a line graph based on the stored data.
4. **Automation**: GitHub Actions trigger the entire process daily, updating the graph automatically in the repository.

## Example Graph

The latest energy consumption graph is displayed below and is updated daily:

![Daily Energy Consumption](.electricity_consumption_chart.png)