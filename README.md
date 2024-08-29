# pintrest-data-pipeline753

# Project Title

## Table of Contents
1. [Project Description](#project-description)
2. [Installation Instructions](#installation-instructions)
3. [Usage Instructions](#usage-instructions)
4. [File Structure](#file-structure)
5. [License Information](#license-information)
6. [AWS Services and Kafka Setup](#aws-services-and-kafka-setup)
7. [Databricks and Airflow Integration](#databricks-and-airflow-integration)

## Project Description

This project involves data processing and integration using AWS services, Kafka, and Databricks. The main objectives are to:

- Fetch data from an AWS RDS instance and post it to a Kafka API.
- Process and clean data using Databricks notebooks.
- Schedule tasks using Airflow and integrate with AWS Managed Workflows for Apache Airflow (MWAA).

Key features of the project include:

- Data extraction from AWS RDS and posting to Kafka.
- Data cleaning and transformation in Databricks.
- Scheduling and orchestration with Airflow.

## Installation Instructions

1. **AWS Setup**:
   - Ensure you have an AWS account and necessary permissions to create and manage services.
   - Set up an RDS instance, Kafka cluster, and S3 buckets as described in the AWS services and Kafka setup section.

2. **Databricks Setup**:
   - Create a Databricks workspace and cluster.
   - Upload your notebooks to Databricks.

3. **Airflow Setup**:
   - Create an MWAA environment and configure it with the necessary permissions.
   - Upload the Airflow DAG to the specified S3 bucket.

4. **Dependencies**:
   - Install required Python packages:
     ```bash
     pip install requests sqlalchemy pymysql boto3 pyspark
     ```

## Usage Instructions

1. **Kafka API Integration**:
   - Use the provided script to fetch data from AWS RDS and post it to the Kafka API.

2. **Databricks Notebook Execution**:
   - Run the Databricks notebooks to process and clean the data.

3. **Airflow Scheduling**:
   - Ensure the DAG is scheduled to run daily and triggers the Databricks notebook execution.

## File Structure


## License Information

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## AWS Services and Kafka Setup

1. **AWS RDS**:
   - Created an RDS instance to store and read data.

2. **Kafka Setup**:
   - Deployed Kafka on an EC2 instance.
   - Configured Kafka REST Proxy to interact with Kafka topics via HTTP requests.

3. **S3 Buckets**:
   - Created S3 buckets for storing and accessing data.
   - Uploaded necessary files and DAGs to the S3 buckets.

## Databricks and Airflow Integration

1. **Databricks**:
   - Created Databricks notebooks for data cleaning and transformation.
   - Connected notebooks to the S3 bucket for reading and writing data.

2. **Airflow**:
   - Created an Airflow DAG to schedule the execution of Databricks notebooks.
   - Configured the DAG to run daily and uploaded it to the MWAA S3 bucket.
   - Ensured proper integration with Databricks and configured the required connections.

For any additional information or troubleshooting, please refer to the [Databricks Documentation](https://docs.databricks.com/) and [Airflow Documentation](https://airflow.apache.org/docs/).


