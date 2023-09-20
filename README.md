# ELT Data Pipeline using GCP

A simple ELT data pipeline that extracts posts and comments from Reddit, transforms and loads it into a data warehouse, and then displays a summary of it on a dashboard. [**Click here to view the interactive dashboard**](https://mchion-reddit-elt-pipeline-streamlit-app-wvgpbg.streamlit.app/).

<p align="center">
  <img src="https://github.com/mchion/reddit_ELT_pipeline/blob/main/images/pipeline.svg?raw=true" alt="Data Model"/>
</p>

## Data Source

- **The Data**: We're trying to collect some minimal information about comments and posts from the [dataengineering](https://www.reddit.com/r/dataengineering/) subreddit. For posts, we collect the post ID, title, author, and timestamp. Similarly, for comments, we collect the comment ID, body of comment, author, and timestamp. Because our end goal is to view posts and comments as a time stream of data, the timestamp for both posts and comments is the essential piece of information we need. 

- **Reddit API**: We can use the official Reddit API to retrieve the data that we need. Although webscraping directly from the Reddit website is possible, it is generally frowned upon and not really necessary in most cases since Reddit API is well documented and robust in terms of the granularity of data it can provide.\
\
One limitation of the API is that if we want the most recent comments posted, it can only send us 100 per request - no more, no less. This requires us then to de-duplicate the data that is sent to us to make sure that we are not adding duplicate data to our dataset.

## Data Ingestion

- **PRAW**: We used [PRAW](https://praw.readthedocs.io/en/stable/index.html), a python-based Reddit API wrapper, to make comment extraction easier. In order to prevent loading duplicate comments into the database, the app discards comments that have a timestamp older than or equal to the last timestamp in our database. Although this could potentially discard comments that have equal timestamps, because this particular subreddit the app is extracting from is not usually very active, the likelihood is small that this would happen and it's not the end of the world if it does.

- **Frequency**: We make hourly requests to the API for approximately 100 of the most recent comments from the specified subreddit. The process is run by a python app that is dockerized, placed into Artifact Registry, and then scheduled to run every hour using Cloud Run.\
\
In addition, being able to extract only 100 comments at time means that we need to extract pretty frequently because if we were to wait too long, more than 100 comments could have been posted by the time we extract, and we would be unable to retrieve those comments in the future. Because this subreddit has a relatively low, steady comment rate per hour, extracting **every hour** provides us with enough cushion to make sure there are less than 100 comments between extraction times.\
\
If we were dealing with a more popular subreddit where the rate of comments becomes almost like a constantly streaming data source, we would have to consider using a Kafka message queue before possibly loading it into an object storage like AWS S3. 

## Staging Area

- **Unprocessed**: When data is initially retrieved from the API, it is immediately saved as a JSON file in Cloud Storage. It is placed in a storage bucket designated as "unprocessed". By storing these raw files in a separate storage bucket, it makes it easier to see which files have not yet been processed in the event there is a failure. 

- **Processed**: After transforming the data to fit our needs, the JSON files are copied into a storage bucket designated for processed files and the original files are deleted. If this were a real-life production data pipeline, the original files would be moved to another storage bucket designated for processed raw files. This keeps our original data intact in case our we need to reprocess old data.

## Data Transformation and Loading

- **Timestamp conversion**: Timestamps need to be properly formatted and have the correct time zone in order before being placed in the data warehouse. Our data schema specifically 

- **Deduplication**: We query our data warehouse for the latest timestamp and use this latest timestamp to only extract comments and posts that occurred after it. Although there are more thorough deduplication methods, the low frequency of user comments to this particular subreddit makes deduplication efforts easier. 

- **Load into BigQuery**: Loading data into BigQuery is straightforward for the most part once the data has been properly transformed so that it matches the data schema. We load posts to one table and comments to another.  

## Data Warehouse

Our data model and data analyzation usage does not need a data warehouse as performant and scalable as BigQuery. It would have also sufficed to accept a slower (but more consistent) RDBMS database like Cloud SQL. However, the ease-of-use and popularity of BigQuery, combined with its generous tiered pricing for users with low usage, led us to choose it over over choices.  

- **Data Model**: Our data model consists of two tables - one for posts and another for comments - with a one-to-many relationship between posts and comments. A post can be associated with many comments, but a comment can only be associated with one post. Organizing the data this way leads to reduced processing that needs to be done by analysts and also reduced redundancy. 
  
<p align="center">
  <img src="https://github.com/mchion/reddit_ELT_pipeline/blob/main/images/schema.svg?raw=true" alt="Data Model"/>
</p>

## Data Visualization

- **Dashboard using Streamlit**: [**Streamlit**](https://streamlit.io/) is an all Python, open-source framework that is easy to get started with, free to deploy on the web, and well documented. It is used by many data science professionals who need to display their data to an wide ranging audience. 

- **Analysis # 1: Total comments per hour**:
How many comments do people write per hour to the data engineering subreddit? This analysis aggregates hourly comment counts.  
<p align="center">
  <img src="https://github.com/mchion/reddit_ELT_pipeline/blob/main/images/dashboard1.png" width="800"/>
</p>

- **Analysis #2: Percentage of posts that meet time threshold**:\
\
Let's say you're an machine learning engineer that wants to determine when a post usually completes - in other words, when the last post was made. The end goal is to try to determine when a Reddit post is "done", meaning when no new comments will be added to the post in the future.

<p align="center">
  <img src="https://github.com/mchion/reddit_ELT_pipeline/blob/main/images/dashboard3.png" width="800"/>
</p>


## Futher Directions and Considerations

- **Machine Learning**: In the second analysis, our goal was to find out the average amount of time it takes until users stop commenting on a post. Using historical data to try to predit when it was done is the realm of machine learning and data science. We can view comments as a stream of data over time (a time series).  

- **Monitoring and Observability**: Because our app is entirely run on a cloud platorm, monitoring and observability are especially important since we cannot be expected to log in and monitor the data pipeline on a regular basis. This exact situation of failure occurring while not being alerted to occurred when the app stopped after a month of running due to a change in the API wrapper. 

