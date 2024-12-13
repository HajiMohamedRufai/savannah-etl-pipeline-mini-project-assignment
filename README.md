# savannah-etl-pipeline-mini-project-assignment

Pipeline Design
===============

The ETL pipeline is orchestrated using Apache Airflow deployed on Astronomer and integrated with Google Cloud Platform (GCP) services. The DAG is structured with mainly the following tasks:

- **Extract Data**: Ingest data from the provided APIs and store them as JSON in Cloud Storage (GCS).
- **Transform Data**: Process and transform the json data using Python and load the intermediate results as CSVs file in GCS.
- **Load Data**: Load the transformed data CSVs into BigQuery as tables.

The task dependencies ensure that the data flows sequentially from extraction to transformation and finally loading into BigQuery.

The final pipeline can be seen as follows which ran sucesfully:
![airflow data pipeline image](<savannah-etl-pipeline-airflow.png>)

Codebase Overview
=================

The codebase comprises the following scripts and modules:

- **dags/savannah_etl_dag.py**: Defines the Airflow DAG and sets up task dependencies.
- **include/*.py**: Contains Python functions (scripts) which assist in the savannah_etl_dag.py to reduce the number of lines in a file.


BigQuery Queries
================

The SQL logic employed in BigQuery involves data insertion and aggregation.

They were used to answer the questions as provided in the assignment at [./Savannah Informatics Data Engineering Assessement.pdf].

The queries used are found in include/helper_functions.py

The resultant when run in BigQuery is as follows creation of 3 more tables.

![big query tables](<Big Query generated tables.png>) of detailed:
- category_summary
- user_summary
- card_details

HOW TO RUN THE PROJECT
===========================
1. You can use Google virtual machine or your local machine.

2. Ensure you have docker installed in your machine. Verify it by: 
```docker-compose --version```

3. Install astronomer to run airflow. verify astronomer installation by
```astro version```

4. Create a new google project in your console and enable these APIs which will be used:
- Google Cloud Storage
- BigQuery
- Cloud Storage JSON API

5. Create a bucket which will be used as the data lake to store the files in the project

6. Create a service account with the permission which will be used. Roles used:
- Storage Admin
- BigQuery Admin 

7. Download the json key file from above

8. Initialize an astronomer project
9. Create a docker-compose.override file to mount the json file into the containers
10. Create your DAG!

Copyright Haji Rufai 2024