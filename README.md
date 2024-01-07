# Youtube-Airflow-ETL-Project

This project aims to perform ETL(Extraction-Transformation-Load) on youtube data using a combination of Apache Airflow, Apache Spark, and MongoDB, Pyspark & Python.

##Prerequisites
Before setting up and running the YOutube Data Pipeline project, make sure you have the following prerequisites in place:

1. **Environment Setup:**
   - Install and configure Apache Airflow.
   - Install and configure Apache Spark on the target machine or cluster.

2. **Youtube Developer Account:**
   - Obtain YouTube API credentials.

3. **MongoDB:**
   - Install MongoDB or you can use MongoDB Atlas

4. **Access and Permissions:**
   - Grant necessary permissions for YouTube API access and AWS S3 resources.

5. **Data Schema Understanding:**
   - Familiarize yourself with the structure of YouTube data returned by the API.

6. **Spark Job Configuration:**
   - Develop Spark jobs and ensure the correct setup of dependencies and configurations.


`youtube_airflow_etl_dag.py` -> The DAG script for performing etl job in an order.

`fetch_data.py` -> The script that gets the data from Youtube API and loads it for transformation. 

`transform_data.py` -> The script that consumes the data fetched and apply diffrent transformations.

`sink_data.py` -> This scripts stores required output in mongodb

`main.py` -> This script handles Youtube API authentication
