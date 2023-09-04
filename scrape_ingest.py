import json
import praw
import time
import logging
from dataclasses import dataclass
from google.cloud import storage
from google.oauth2 import service_account

@dataclass
class reddit():

    CLIENT_ID = '<YOUR INFO HERE>'
    CLIENT_SECRET = '<YOUR INFO HERE>'
    USER_AGENT = '<YOUR INFO HERE>'

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


def get_reddit_handler():
    """Initialize and connect to the PRAW Reddit API"""
    
    reddit_handler = praw.Reddit(
        client_id = reddit.CLIENT_ID,
        client_secret = reddit.CLIENT_SECRET,
        user_agent = reddit.USER_AGENT
    )
    return reddit_handler

def get_cloud_storage_connection():
    """Connect to GCP Cloud Storage"""
    
    credentials = service_account.Credentials.from_service_account_info(gcp.SERVICE_ACCOUNT_DICT)
    storage_client = storage.Client(
        project=gcp.SERVICE_ACCOUNT_DICT['project_id'], 
        credentials=credentials)

    return storage_client

def get_reddit_comments(reddit_handler):
    """Retrive last 100 comments from Reddit"""
    
    count = 1
    reddit_comments = []
    
    for comment in reddit_handler.subreddit("dataengineering").stream.comments(skip_existing=False):
        
        if count > 100 or comment is None: 
            break
        try:
            d = {'c_id': comment.id,
                'c_author': comment.author.name if comment.author.name else '',
                'c_body': comment.body,
                'c_time': comment.created_utc,
                'p_id': comment.submission.id,
                'p_author': comment.submission.author.name if comment.submission.name else '',
                'p_title': comment.submission.title,
                'p_time': comment.submission.created_utc
            }
            reddit_comments.append(d)
        except (AttributeError,NameError):
            logging.warning('a comment import failed at %s'%time.strftime("%Y-%m-%d--%H-%M-%S"))

        count += 1

    return reddit_comments

def save_to_cloud_storage(reddit_comments, storage_client):
    """Save reddit comments to cloud storage bucket as JSON"""

    json_object = json.dumps(reddit_comments, indent=4)
    filename = f'raw-unprocessed/{time.strftime("%Y-%m-%d--%H-%M-%S")}.json'
    bucket_name = 'reddit_comments_bucket_de'
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(filename)

    with blob.open('w') as f:
        f.write(json_object)
    
def main():
    """Connect to reddit API and ingest comments to cloud storage bucket as JSON."""
    
    reddit_handler = get_reddit_handler()
    reddit_comments = get_reddit_comments(reddit_handler)
    storage_client = get_cloud_storage_connection()
    save_to_cloud_storage(reddit_comments, storage_client)

if __name__ == '__main__':
    main()