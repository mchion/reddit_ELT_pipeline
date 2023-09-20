# ELT Data Pipeline using GCP

A simple ELT data pipeline that extracts posts and comments from Reddit, transforms and loads it into a data warehouse, and then displays a summary of it on a dashboard. [**Click here to view the interactive dashboard**](https://mchion-reddit-elt-pipeline-streamlit-app-wvgpbg.streamlit.app/).

<p align="center">
  <img src="https://github.com/mchion/reddit_ELT_pipeline/blob/main/images/pipeline.svg?raw=true" alt="Data Model"/>
</p>

## Data Source

- **The Data**: We're trying to collect some minimal information about comments and posts from the [dataengineering](https://www.reddit.com/r/dataengineering/) subreddit. For posts, we want to collect the post ID, title, author, and timestamp. Similarly, for comments, we want to collect the comment ID, body of comment, author, and timestamp. Because our end goal is to view posts and comments as a sort of time stream, the timestamp for both is the essential piece of information we need. 

- **Reddit API**: The Reddit API is well documented and heavily used by many other platorms. Although webscraping directly from the Reddit website is possible, it is generally frowned upon. One limitation of the API that we enountered is that is has fixed extraction amount of 100 recent comments per request. In other words, it is not possible to extract any amount less than or greater than 100.


## Data Ingestion

- **PRAW**: We used [PRAW](https://praw.readthedocs.io/en/stable/index.html), a python-based Reddit API wrapper, to make comment extraction easier. In order to prevent loading duplicate comments into the database, the app discards comments that have a timestamp older than or equal to the last timestamp in our database. Although this could potentially discard comments that have equal timestamps, because this particular subreddit the app is extracting from is not usually very active, the likelihood is small that this would happen and it's not the end of the world if it does.

- **Frequency**: We make hourly requests to the API for approximately 100 of the most recent comments from the specified subreddit. The process is run by a python app that is dockerized, placed into Artifact Registry, and then scheduled to run every hour using Cloud Run.\
\
In addition, being able to extract only 100 comments at time means that we need to extract pretty frequently because if we were to wait too long, more than 100 comments could have been posted by the time we extract, and we would be unable to retrieve those comments in the future. Because this subreddit has a relatively low, steady comment rate per hour, extracting **every hour** provides us with enough cushion to make sure there are less than 100 comments between extraction times.\
\
If we were dealing with a more popular subreddit where the rate of comments becomes almost like a constantly streaming data source, we would have to consider using a Kafka message queue before possibly loading it into an object storage like AWS S3. 

## Staging Area

- **Unprocessed**: Data is initially saved as JSON files in Cloud Storage. It is designated as "unprocessed" and stored in a storage bucket.

- **Processed**: After being transformed, the original data is marked as being "processed" and moved into a storage bucket designated for processed files. In this way, we can keep track of which files have been processed without modifying the orignal data. 

## Data Transformation and Loading

- **Get only new comments**: Because of our extraction process, we loaded exactly 100 comments without regard to whether we have requested them before. This means that our data has duplicates. We read from our data warehouse the latest timestamp and use that to
only extract from

- **Load into BigQuery**: Loading data is simple enough. We just have to make sure it matches the data schema that we set up with posts going in one table and comments into the other. 

## Data Warehouse

Our data model and data analyzation usage is simple enough to not need a data warehouse as performant and scalable as BigQuery. A RDBMS database like Cloud SQL would have also sufficed. However, the ease-of-use and popularity of BigQuery, combined with tiered pricing, led us to choose it for this simple pipeline. 

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

