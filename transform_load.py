import json
import time
import pytz
import logging

from datetime import datetime
from dataclasses import dataclass
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account

@dataclass
class gcp():

    SERVICE_ACCOUNT_DICT = {
        "type": "service_account",
        "project_id": "<YOUR INFO HERE>",
        "private_key_id": "<YOUR INFO HERE>",
        "private_key": "<YOUR INFO HERE>",
        "client_email": "<YOUR INFO HERE>",
        "client_id": "<YOUR INFO HERE>",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "<YOUR INFO HERE>"
        }


def get_cloud_storage_connection():
    """Connect to GCP Cloud Storage"""
    
    credentials = service_account.Credentials.from_service_account_info(gcp.SERVICE_ACCOUNT_DICT)
    storage_client = storage.Client(
        project=gcp.SERVICE_ACCOUNT_DICT['project_id'], 
        credentials=credentials)

    return storage_client

def get_bigquery_connection():
    '''Connect to BigQuery'''
    
    credentials = service_account.Credentials.from_service_account_info(gcp.SERVICE_ACCOUNT_DICT)
    bigquery_client = bigquery.Client(
        project=gcp.SERVICE_ACCOUNT_DICT['project_id'], 
        credentials=credentials)
    
    return bigquery_client

def read_from_cloud_storage(storage_client):
    """Read JSON files from unprocessed folder in cloud storage bucket"""

    bucket_name = 'reddit_comments_bucket_de'
    bucket_path = 'raw-unprocessed/'
    blobs = list(storage_client.list_blobs(bucket_name, prefix = bucket_path))
    json_combined = []

    for blob in blobs:
        with blob.open('r') as f:
            if blob.name.endswith('json'):
                json_combined.extend(json.load(f))

    return json_combined

def get_last_timestamp(bigquery_client): 
    '''Gets last timestamp from BigQuery table_comments'''

    job_config_query = bigquery.QueryJobConfig()

    sql = '''
        SELECT 
            c_time
        FROM `project-reddit-364500.comments_dataset.table_comments`
        WHERE DATE(c_time) >= CURRENT_DATE()-2
        ORDER BY c_time DESC
        LIMIT 1
        '''
    
    query_job = bigquery_client.query(sql, job_config=job_config_query)
    results = query_job.result()
    for row in results:
        last_timestamp = row['c_time'].strftime('%Y-%m-%d %H:%M:%S')
    
    return last_timestamp

def transform_reddit_comments(json_combined, last_timestamp):
    """Convert timestamp format to yyyy-mm-dd HH:MM:SS"""

    json_comments = []
    json_posts = []

    for comment in json_combined:
        comment_time = datetime.fromtimestamp(comment['c_time']).astimezone(pytz.timezone("UTC")).strftime('%Y-%m-%d %H:%M:%S')
        if comment_time > last_timestamp: 
            d1 = {'c_id': comment['c_id'], 
                'c_author': comment['c_author'], 
                'c_body': comment['c_body'], 
                'c_time': comment_time
            }
            json_comments.append(d1)
            d2 = {'p_id': comment['s_id'], 
                'p_author': comment['s_author'], 
                'p_title': comment['s_title'], 
                'p_time': datetime.fromtimestamp(comment['p_time']).astimezone(pytz.timezone("UTC")).strftime('%Y-%m-%d %H:%M:%S')
            }
            json_posts.append(d2)

    return json_comments, json_posts

def load_json_comments_to_bigquery(json_comments, bigquery_client):

    table_id_comments = 'comments_dataset.table_comments'

    job_config_comments = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("c_id", "STRING"),
            bigquery.SchemaField("c_author", "STRING"),
            bigquery.SchemaField("c_body", "STRING"),
            bigquery.SchemaField("c_time", "TIMESTAMP"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    load_job = bigquery_client.load_table_from_json(
        json_comments,
        table_id_comments,
        location="us-central1",
        job_config=job_config_comments,
    )
    load_job.result()

    logging.info(f'Added {len(json_comments)} to comments table')

def get_only_new_posts(client):
    
    query_distinct_post_ids = (
            """
            SELECT DISTINCT
                p_id
            FROM comments_dataset.table_posts
            """
        )
    df = client.query(query_distinct_post_ids).to_dataframe()
    posts_in_db_set = {x[0] for x in df.values.tolist()}

    new_posts_only_json = []

    for i in json_posts:
        if list(i.values())[0] in posts_in_db_set:
            continue
        new_posts_only_json.append(i)
    
    return new_posts_only_json

def load_json_posts_to_bigquery(json_posts, bigquery_client):

    table_id_posts = 'comments_dataset.table_posts'

    job_config_posts = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("p_id", "STRING"),
            bigquery.SchemaField("p_author", "STRING"),
            bigquery.SchemaField("p_title", "STRING"),
            bigquery.SchemaField("p_time", "TIMESTAMP"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    load_job = bigquery_client.load_table_from_json(
        json_posts,
        table_id_posts,
        location="us-central1",
        job_config=job_config_posts,
    )
    load_job.result()
    logging.info(f'Added {len(json_posts)} new posts to post table')


def save_transformed_files(comments, posts, storage_client):
    """Save transformed JSON to processed folder in cloud storage bucket"""

    for file in [comments, posts]: 
        json_object = json.dumps(file, indent=4)
        filename = f'processed/{time.strftime("%Y-%m-%d--%H-%M-%S")}-{file}.json'
        
        bucket_name = 'reddit_comments_bucket_de'
        bucket = storage_client.get_bucket(bucket_name)
        
        blob = bucket.blob(filename)
        with blob.open('w') as f:
            f.write(json_object)


def transfer_raw_to_processed_folder(storage_client):
    """Move processed raw files from 'unprocessed' folder to 'processed' folder"""

    bucket_name = 'reddit_comments_bucket_de'
    bucket_path = 'raw-unprocessed/'
    blobs = list(storage_client.list_blobs(bucket_name, prefix = bucket_path))

    source_bucket = storage_client.bucket(bucket_name)
    destination_bucket = storage_client.bucket(bucket_name)

    for blob in blobs:
        if blob.name.endswith('json'):
            source_blob = source_bucket.blob(blob.name)
            destination_blob_name = f"raw-processed/{blob.name.replace('unprocessed/','')}"
            blob_copy = source_bucket.copy_blob(
                source_blob, 
                destination_bucket, 
                destination_blob_name, 
                if_generation_match=0,
            )
            source_bucket.delete_blob(blob.name)


def main():
    """Transform raw JSON files and save to 'processed' folder"""
    
    storage_client = get_cloud_storage_connection()
    bigquery_client = get_bigquery_connection()

    json_combined = read_from_cloud_storage(storage_client)
    
    last_timestamp = get_last_timestamp(bigquery_client)
    json_comments, json_posts = transform_reddit_comments(json_combined, last_timestamp)
    
    load_json_comments_to_bigquery(json_comments, bigquery_client)

    new_posts_only = get_only_new_posts(client)
    load_json_posts_to_bigquery(new_posts_only, bigquery_client)

    save_transformed_files(json_comments, new_posts_only, storage_client)
    transfer_raw_to_processed_folder(storage_client)

if __name__ == '__main__':
    main()