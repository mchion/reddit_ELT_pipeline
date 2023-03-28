import json
import praw
import boto3
import time
import json
import logging
from dataclasses import dataclass

@dataclass
class reddit():

    CLIENT_ID = <ENTER YOUR CLIENT_ID>
    CLIENT_SECRET = <ENTER YOUR CLIENT_SECRET>
    USER_AGENT = <ENTER YOUR USER_AGENT>
    
def get_reddit_handler():
    """Initialize and connect to the PRAW Reddit API"""
    
    reddit_handler = praw.Reddit(
        client_id = reddit.CLIENT_ID,
        client_secret = reddit.CLIENT_SECRET,
        user_agent = reddit.USER_AGENT
    )
    return reddit_handler

def get_S3_connection():
    """Connect to AWS S3 bucket"""
    
    s3_client = boto3.client('s3')
    return s3_client

def get_reddit_comments(reddit_handler):
    """Retrive last 100 comments from Reddit"""
    
    count = 1
    reddit_comments = []
    
    for comment in reddit_handler.subreddit("dataengineering").stream.comments(skip_existing=False):
        
        if count > 100 or comment is None: 
            break
        try:
            d = {'c_id': comment.id,
                'c_author': comment.author.name if comment else '',
                'c_body': comment.body,
                'c_time': comment.created_utc,
                's_id': comment.submission.id,
                's_author': comment.submission.author.name if comment else '',
                's_title': comment.submission.title,
                's_time': comment.submission.created_utc
            }
            reddit_comments.append(d)
        except (AttributeError,NameError):
            logging.warning('a comment import failed at %s'%time.strftime("%Y.%m.%d-%H%M%S"))

        count += 1

    return reddit_comments

def save_to_S3(reddit_comments, s3_client):
    """Save reddit comments as JSON file in S3"""

    body = json.dumps(reddit_comments, indent=4)
    bucket = 'reddit-de-comments'
    filename = f'{time.strftime("%Y%m%d-%H%M%S")}.json'
    
    s3_client.put_object(
        Body=body,
        Bucket=bucket,
        Key= filename
)
    
def lambda_handler(event, context):
    """Connect to reddit API and ingest comments as JSON file to an AWS S3 bucket."""
    
    reddit_handler = get_reddit_handler()
    reddit_comments = get_reddit_comments(reddit_handler)
    s3_client = get_S3_connection()
    save_to_S3(reddit_comments, s3_client)
