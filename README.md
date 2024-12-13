# savannah-etl-pipeline-mini-project-assignment

Pipeline Design
===============

The ETL pipeline is orchestrated using Apache Airflow deployed on Astronomer and integrated with Google Cloud Platform (GCP) services. The DAG is structured with the following tasks:

- **Extract Data**: Ingest data from Google Cloud Storage (GCS).
- **Transform Data**: Process and transform data using Python.
- **Load Data**: Load the transformed data into BigQuery.

The task dependencies ensure that the data flows sequentially from extraction to transformation and finally loading into BigQuery.

Codebase Overview
=================

The codebase comprises the following scripts and modules:

- **dags/savannah_etl_dag.py**: Defines the Airflow DAG and sets up task dependencies.
- **include/transform_functions.py**: Contains Python functions for data transformation.
- **plugins/**: Holds any custom Airflow plugins utilized in the project.

BigQuery Queries
================

The SQL logic employed in BigQuery involves data insertion and aggregation:

Deploy Your Project Locally
===========================

1. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

2. Verify that all 4 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either [stop your existing Docker containers or change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://www.astronomer.io/docs/astro/deploy-code/

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.
