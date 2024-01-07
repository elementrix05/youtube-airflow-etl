from datetime import timedelta

from pyspark.sql import SparkSession

from main import get_properties
from fetch_data import fetch_top_10_most_popular_youtube_videos
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from sinkdata import save_youtube_data
from transform_data import process_youtube_data

spark = SparkSession.builder.appName("Youtube_airflow_etl").config("spark.driver.memory", "8g").getOrCreate()

config = get_properties()
# Access values
email = config.get('user_data', 'email')

default_args = {
    'owner': 'elementrix05',
    'depends_on_past': False,
    'start_date': pendulum.today("UTC").subtract(days=0),
    'email': [email],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'youtube_etl',
    default_args=default_args,
    description='Get youtube videos and comments',
    schedule=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='fetch_top_10_most_popular_youtube_videos',
    python_callable=fetch_top_10_most_popular_youtube_videos,
    provide_context=True,  # This enables passing the context to the callable function
    dag=dag,
)
t2 = PythonOperator(
    task_id='process_youtube_data',
    python_callable=process_youtube_data,
    provide_context=True,  # This enables passing the context to the callable function
    op_args=[spark],  # Pass the Spark session as an argument
    dag=dag,
)
t3 = PythonOperator(
    task_id='save_youtube_data',
    python_callable=save_youtube_data,
    provide_context=True,  # This enables passing the context to the callable function
    dag=dag,
)

t1 >> t2 >> t3