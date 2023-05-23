from datetime import datetime
import json
from airflow import DAG
from googleapiclient.discovery import build

from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'start_date':datetime(2023,1,1)
}

api_key = 'AIzaSyBPTl75JsRv0rEa9nG95vEFyCq81gen0Ps'


with DAG(
    dag_id = "youtube_api_crawl_pipeline",
    schedule_interval = "@daily",
    default_args = default_args,
    tags = ['youtube','local', 'api', 'pipeline'],
    catchup=False,) as dag:

    createing_table = SqliteOperator(
        task_id = "creating_table",
        sqlite_conn_id = 'db_sqlite',
        sql = '''
            CREATE TABLE IF NOT EXISTS youtube_crawl_result(
                title TEXT,
                description TEXT,
                viewcount INT,
                likecount INT,
                commentcount INT
            )
        '''
    )

    is_api_available = HttpSensor(
        task_id = 'is_api_available',
        http_conn_id = 'youtube_crawl_api',
        endpoint = 'youtube/v3/search',
        # headers = {
        #     'key':api_key
        # },
        request_params={
            'key':api_key,
            'part':'snippet',
            'channelId':'UC1LzvduPSKr9puUr',
            'order':'date',
            'maxResults':5,
            'publishedAfter': '2022-01-01T00:00:00Z',
            'publishedBefore': '2022-12-31T23:59:59Z',
            'type' : 'video',
        },
        response_check=lambda response : response.json()
    )

    pass