# RTE Project

This project involves building a comprehensive data pipeline from ingestion to visualization using the RTE API for French electricity data.

## 1. Project Scope and Objectives

- **Goal:** Develop a complete data pipeline from data ingestion to visualization using the RTE API for French electricity data.
- **Timeframe:** 1 month

### Key Skills to Showcase:
- API Data Ingestion
- Data Storage (using Snowflake)
- Data Transformation and Enrichment (using DBT)
- Orchestration (using Airflow or Prefect)
- Visualization (using Python or Tableau)
- CI/CD for Automation and Deployment
- Cloud Storage Management
- Security (API Key Management)
- Documentation and Presentation

## 2. Data Ingestion

### API Integration:
- Use the RTE API to pull live French electricity data.
- Decide on a data pull frequency (e.g., hourly) based on data update rates.
- **Tools:** Python for API requests.
- **API Security:** Securely store API keys using environment variables or a secrets manager (e.g., AWS Secrets Manager, .env files).

### Batch vs. Stream Processing:
- **Batch Processing:** Ingest data at fixed intervals (e.g., hourly). Suitable for non-real-time updates.
- **Stream Processing:** Real-time data processing for immediate updates. More complex but beneficial for up-to-date information.

## 3. Data Storage

### Snowflake:
- Store both raw and processed data in Snowflake.
- Set up a Snowflake account and create tables for data storage.
- **Cost Management:** Utilize Snowflake’s free tier to manage costs for small projects.

## 4. Data Transformation and Enrichment

### DBT (Data Build Tool):
- Set up DBT to transform raw data into a clean, structured format within Snowflake.
- Implement data cleaning (e.g., handling missing values, correcting data types) and enrichment processes.
- **Enrichment Example:** Integrate external data (e.g., weather data) to analyze correlations with electricity usage.

## 5. Data Orchestration

### Airflow/Prefect:
- Use Apache Airflow or Prefect for orchestrating the data pipeline.
- **Example Workflow:**
  1. Ingest data from the RTE API.
  2. Load raw data into Snowflake.
  3. Run DBT transformations.
  4. Update the visualization.

## 6. Visualization

### Python (Plotly/Dash) or Tableau:
- If building a dashboard, use Python’s Plotly/Dash to create interactive visualizations hosted on a web server.
- If creating a workbook, use Tableau to design the visualizations, pulling data directly from Snowflake.
- **Recommendation:** Dash offers more control and is a great way to showcase programming skills, while Tableau is excellent for data storytelling and is widely recognized.

## 7. Cloud Storage and Hosting

- **Data Storage:** Snowflake for data storage.
- **Hosting:** If using Dash, consider deploying the dashboard on a free Heroku tier or an AWS EC2 instance with careful monitoring to avoid costs.
- **CI/CD:** Use GitHub Actions for automating tests, deployments, and updates. This demonstrates your ability to maintain and deploy your pipeline consistently.

## 8. Security

### API Key Management:
- Store API keys securely using environment variables or a secrets manager.
- Ensure sensitive information is not exposed in your codebase or GitHub repository.

## 9. Documentation

- **README:** Create a comprehensive README file in your GitHub repository explaining the project, its objectives, and how to set it up.
- **Technical Documentation:** Document the pipeline, including the architecture, tools used, and how each component interacts.
- **Presentation:** Consider writing a blog post or recording a video walkthrough of your project to explain your work and showcase your skills.

## 10. Project Timeline

- **Week 1:** Set up data ingestion (API integration and Snowflake).
- **Week 2:** Implement data transformation (DBT) and begin orchestration setup.
- **Week 3:** Finalize orchestration, build visualization, and set up CI/CD.
- **Week 4:** Final testing, security checks, documentation, and project deployment.

