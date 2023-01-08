import os
import pandas as pd
import praw
import pytz
import pandas_gbq


from datetime import datetime, timedelta, time
from google.cloud import bigquery
from google.oauth2 import service_account

'''ENVIRONMENT VARIABLES'''
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.environ.get("REDDIT_USER_AGENT")
SERVICE_ACCOUNT_DICT = os.environ.get("SERVICE_ACCOUNT_DICT")


def get_bigquery_connection():
    '''Initialize and connect to the BigQuery database'''
    
    credentials = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_DICT)
    client = bigquery.Client(project=SERVICE_ACCOUNT_DICT['project_id'], credentials=credentials)
    return client

def get_reddit_connection():
    '''Initialize and connect to the PRAW Reddit API'''
    r = praw.Reddit(
        client_id = REDDIT_CLIENT_ID,
        client_secret = REDDIT_CLIENT_SECRET,
        user_agent = REDDIT_USER_AGENT
    )
    return r

def get_last_timestamp(client):
    '''Sends timestamp query to BigQuery'''

    job_config_query = bigquery.QueryJobConfig()
    sql1 = """SELECT c_id
        FROM `project-reddit-364500.comments_dataset.comments_table`
        LIMIT 1 """
    sql2 = '''SELECT c_time,
        FROM `project-reddit-364500.comments_dataset.comments_table`
        WHERE DATE(C_TIME) >= CURRENT_DATE()-2
        ORDER BY c_time DESC
        LIMIT 1'''
    
    query_job = client.query(sql1, job_config=job_config_query)
    results1 = query_job.result()
    
    last_timestamp = time(0, 0, 0)
    if results1.total_rows == 0:
        last_timestamp = (datetime.now().astimezone(pytz.timezone("UTC"))- timedelta(hours=48)).strftime('%Y-%m-%d %H:%M:%S')
    else:
        query_job = client.query(sql2, job_config=job_config_query)
        results2 = query_job.result()
        for row in results2:
            last_timestamp = row['c_time'].strftime('%Y-%m-%d %H:%M:%S')
    
    if last_timestamp == time(0, 0, 0):
        last_timestamp = (datetime.now().astimezone(pytz.timezone("UTC"))- timedelta(hours=48)).strftime('%Y-%m-%d %H:%M:%S')
    
    return last_timestamp


def get_reddit_comments(r,client):
    print(f"Starting stream at {datetime.now().astimezone(pytz.timezone('UTC')).strftime('%Y-%m-%d %H:%M:%S')}")
    
    count = 1
    last_timestamp = get_last_timestamp(client)
    dictionary_list = []
    
    for comment in r.subreddit("dataengineering").stream.comments(skip_existing=False):
        
        if comment != None and count > 0 and count <= 100:
            comment_time = datetime.fromtimestamp(comment.created_utc).astimezone(pytz.timezone("UTC")).strftime('%Y-%m-%d %H:%M:%S')
            
            if comment_time > last_timestamp:
                submission_time = datetime.fromtimestamp(comment.submission.created_utc).astimezone(pytz.timezone("UTC")).strftime('%Y-%m-%d %H:%M:%S')
                try: 
                    d = {'c_id': comment.id,
                        'c_author': comment.author.name,
                        'c_body': comment.body,
                        'c_time': comment_time,
                        's_id': comment.submission.id,
                        's_author': comment.submission.author.name,
                        's_title': comment.submission.title,
                        's_time': submission_time
                    }
                    dictionary_list.append(d)
                    print(f'Added {comment_time} at count {count}')
                except Exception as error:
                    print(error.args)
                    pass
        count += 1
        if count > 100 or comment == None: 
            break

    df = pd.DataFrame.from_dict(dictionary_list)
    return df


def load_reddit_comments(client, new_comments_df):

    schema=[
            {'name': 'c_id', 'type': "STRING"},
            {'name': 'c_author', 'type': "STRING"},
            {'name': 'c_body', 'type': "STRING"},
            {'name': 'c_time', 'type': "TIMESTAMP"},
            {'name': 's_id', 'type': "STRING"},
            {'name': 's_author', 'type': "STRING"},
            {'name': 's_title', 'type': "STRING"},
            {'name': 's_time', 'type': "TIMESTAMP"}
            ]
    
    table_id = 'comments_dataset.comments_table'
    project_id = 'project-reddit-364500'
    pandas_gbq.to_gbq(new_comments_df, table_id, project_id=project_id,if_exists='append',table_schema=schema)
    
    return True


def main():
    '''Connect to reddit API, connect to BigQuery, get new reddit comments, load all new comments into BigQuery'''
    
    r = get_reddit_connection()
    client = get_bigquery_connection()
    new_comments_df = get_reddit_comments(r,client)
    
    if not new_comments_df.empty:
        items_count = len(new_comments_df.index)
        new_comments_df['c_time'] = pd.to_datetime(new_comments_df['c_time'])
        new_comments_df['s_time'] = pd.to_datetime(new_comments_df['s_time'])
        
        final_result = load_reddit_comments(client, new_comments_df)
        print(f'Finished - {items_count} items added to BigQuery database' if final_result else "Error loading comments to BigQuery database")
    else: 
        print("Finished - no new items to add")

if __name__ == '__main__':
    main()
