import os
import praw
import boto3
from dataclasses import dataclass


@dataclass
class reddit():

    CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
    CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
    USER_AGENT = os.environ.get("REDDIT_USER_AGENT")

@dataclass
class aws():

    ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
    SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
    SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN")


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
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id = aws.ACCESS_KEY,
        aws_secret_access_key = aws.SECRET_KEY,
        aws_session_token = aws.SESSION_TOKEN
    )
    return s3_client

def get_reddit_comments(reddit_handler):
    """Retrive last 100 comments from Reddit"""
    
    count = 1
    reddit_comments = []
    
    for comment in r.subreddit("dataengineering").stream.comments(skip_existing=False):
        
        if count > 100 or comment is None: 
            break

        d = {'c_id': comment.id,
            'c_author': comment.author.name,
            'c_body': comment.body,
            'c_time': comment.created_utc,
            's_id': comment.submission.id,
            's_author': comment.submission.author.name,
            's_title': comment.submission.title,
            's_time': comment.submission.created_utc
        }
        reddit_comments.append(d)
        count += 1

    return reddit_comments

def save_to_S3(reddit_comments, s3_client):
    """Save reddit comments as JSON file in S3"""

    filename = 
    
    s3_client.put_object(
        Body=json.dumps(reddit_comments),
        Bucket='reddit_data_engineering',
        Key= filename
)

def main():
    """Connect to reddit API and ingest comments as JSON file to an AWS S3 bucket."""

    reddit_handler = get_reddit_handler()
    reddit_comments = get_reddit_comments(reddit_handler)
    s3_client = get_S3_connection()
    save_to_S3(reddit_comments, s3_client)

if __name__ == '__main__':
    main()
