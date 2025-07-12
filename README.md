# Netflix End to End Automated ELT Data Pipeline: Airflow, Snowflake, and dbt


1. Introduction : This project demonstrates the implementation of a fully automated data pipeline using Apache Airflow, Snowflake, and dbt (data build tool). The pipeline extracts, transforms, and loads (ETL) Netflix data into Snowflake, performs transformations using dbt, and prepares the data for analysis.

2. Project Overview : The objective of this project is to build a robust and scalable ELT workflow that ingests raw Netflix data, performs data cleaning and transformations, and creates analytical data models. This workflow simulates a production-ready pipeline used in modern data engineering workflows.

3. Key Features :

- Data Extraction: Extracts raw Netflix data from a CSV file.
- Data Loading: Loads the raw data into a Snowflake staging table.
- Data Transformation: Cleans and transforms the raw data using dbt models.
- Automation: Orchestrates the complete pipeline with Apache Airflow.
- Data Modeling: Creates marts for analysis, such as top genres, release trends, and average movie duration.

4. Tech Stack :

- Apache Airflow: Workflow orchestration and automation.
- Snowflake: Cloud data warehouse for storage and computation.
- dbt: Data transformations and modeling.
- Python: Custom scripts and Airflow DAGs.
- Git/GitHub: Version control.

5. Pipeline Architecture

- Extract: Reads raw Netflix data from a CSV file.
- Load: Inserts the raw data into Snowflake’s AUTO_NETFLIX_DB.AUTO_OGDATA table.
- Transform: dbt applies cleaning and modeling to produce analytical datasets in Snowflake.
- Orchestrate: Airflow schedules and manages the workflow as a Directed Acyclic Graph (DAG).

6. Folder Structure

Netflix_Auto/
├── airflow/                # Airflow configuration and DAGs
│   └── dags/
├── netflix_dbt/            # dbt project for transformations
│   ├── models/
│   ├── macros/
│   ├── dbt_project.yml
├── Scripts/                # Practice scripts (optional)
├── sql/                    # Practice SQL queries (optional)
├── README.md
├── requirements.txt        # Python dependencies
├── .gitignore

7. Setup Instructions

1) Clone the Repository :
- git clone https://github.com/payalnavsupe/netflix-pipeline.git
- cd netflix-pipeline

3) Set Up Virtual Environment :
- python -m venv airflow_env
- source airflow_env/bin/activate

4) Install Dependencies :
- pip install -r requirements.txt

5) Configure Airflow :
Initialize Airflow:
- airflow db init
- Start the Airflow webserver and scheduler.

6) Set Up Snowflake :
- Create a database, schema, and warehouse as required.
- Update connection details in Airflow.

7) Run the Pipeline :
- Trigger the DAG from the Airflow UI or CLI.

8. Outcomes :

- Automated loading of raw data into Snowflake.
- Cleaned and transformed datasets in Snowflake for analytics.
- Modular and scalable architecture for ELT workflows.

9. Future Enhancements :

- Integrate cloud storage (e.g., AWS S3) for raw data ingestion.
- Implement data quality checks and monitoring.
- Build dashboards using BI tools like Tableau or Power BI.
