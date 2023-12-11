# ELT Data Pipeline using GCP

A simple ELT data pipeline that extracts posts and comments from Reddit, transforms and loads it into a data warehouse, and then displays a summary of it on a dashboard. [**Click here to view the interactive dashboard**](https://mchion-reddit-elt-pipeline-streamlit-app-wvgpbg.streamlit.app/).

<p align="center">
  <img src="https://github.com/mchion/reddit_ELT_pipeline/blob/main/images/pipeline.svg?raw=true" alt="Data Model"/>
</p>

## Data Source

- **The Data**: The two pieces of data that we are most interested in from the [dataengineering](https://www.reddit.com/r/dataengineering/) subreddit are comments and posts. For posts, we want to grab the post ID, title, author, and timestamp. Similarly, for comments, we want to grab the comment ID, body of comment, author, and timestamp. For both comments and posts, the timestamp is what will enable us to analyze this data in a time series manner.  

## Data Ingestion

- **Reddit API**: For the purposes of this project, the official Reddit API is robust enough in terms of the granularity of data it can provide such that we can retrieve the specific data we need about posts and comments without having to web scrape directly from the Reddit website. In order to make data extraction even easier, we used [PRAW](https://praw.readthedocs.io/en/stable/index.html), a python-based Reddit API wrapper. \
\
One limitation of the Reddit API is that it only sends back a fixed amount of recent comments per request. This requires us to de-duplicate the data sent back to us in order to make sure that we are not including previously received data with each new request. De-duplication is discussed in the data transformation section below. 

- **Frequency**: We make hourly requests to the API for the data specified above. The process is run by a python app that is dockerized, placed into GCP's Artifact Registry, and then scheduled to run every hour using Cloud Run.\
\
The reason why we make hourly requests is due to the API's limitation of only being able to receive the most recent 100 comments per request. Wait to long, and more than 100 comments may be posted and our request would be unable to recapture those comments without significant de-duplication efforts. Because this particular subreddit has a relatively low, steady comment rate per hour, extracting **every hour** provides us with enough cushion to make sure there are less than 100 comments between extraction times. We also extract during an off-hour time (such as 17 minutes past the hour) in order to lessen the burden on the Reddit API during the more popular on-the-hour times.\
\
If our project dealt with a more popular subreddit where the rate of comments becomes similar to a near streaming data source, we would have to consider using a message queue like Kafka before possibly loading it into our storage. 

## Staging Area

- **Unprocessed Data**: When data is initially retrieved from the API, it is immediately saved as a JSON file in Cloud Storage. It is placed in a storage bucket designated as "unprocessed". By storing these raw files in a separate storage bucket, it makes it easier to see which files have not yet been processed in the event of a pipeline failure. 

- **Processed Data**: After transforming the data to fit our needs, the JSON files are copied into a storage bucket designated for processed files and the original files are deleted. If this were a real-life production data pipeline, the original files would be moved to another storage bucket designated for processed raw files. This keeps our original data intact in case our we need to reprocess old data.

## Data Transformation and Loading

- **Timestamp Conversion**: Timestamps need to be properly formatted and have the correct time zone before being placed in the data warehouse.

- **Deduplication**: In order to prevent loading duplicate comments into the database, the app discards comments that have a timestamp older than or equal to the last timestamp in our database. Although this could potentially discard comments that have equal timestamps, the low frequency of user comments to this particular subreddit makes the likelihood of multiple comments posted at the same time very small. Since our project aggregates comments per hour, the impact a missing comment is negligible.\ 
\
A more data intensive project that relied on ensuring that every comment was captured by our app would require more sophisticated de-duplication methods. One such way would be to relax the time requirement of the last timestamp, and thus allow duplicate data to exist in the database for a certain amount of time before a global deduplication is performed. This global deplication could be performed daily, let's say, and only involve data ingested in the past 24 hours so that its scope is limited and duplicate data can be removed in a timely way.   

- **Load into Data Warehouse**: Loading data into the data warehoue (BigQuery) is straightforward for the most part once the data has been properly transformed so that it matches the data schema. We load posts to one table and comments to another.  

## Data Model and Data Warehouse 

- **Data Model**: Our data model consists of two tables - one for posts and another for comments - with a one-to-many relationship between posts and comments. A post can be associated with many comments, but a comment can only be associated with one post. Organizing the data this way leads to reduced processing that needs to be done by analysts and reduced redundancy of having multiple comments having the same post information. 
  
<p align="center">
  <img src="https://github.com/mchion/reddit_ELT_pipeline/blob/main/images/schema.svg?raw=true" alt="Data Model"/>
</p>

- **Data Warehouse**: Although our data model and data analyzation usage does not require a data warehouse as performant and scalable as BigQuery, the ease-of-use and popularity of BigQuery, combined with its generous tiered pricing for users with low usage, led us to choose it over other options. We could have easily chosen a less performant but more consisent database such as Cloud SQL (a RDBMS database) if we had wanted without any noticable performance difference. 

## Data Visualization

- **Dashboard using Streamlit**: We used [**Streamlit**](https://streamlit.io/) as our dashboard. Streamlit is an all Python, open-source framework that is easy to get started with, free to deploy on the web, and well documented. It is used by many data science professionals who need to display their data to a wide ranging audience. 

## Possible Ways to Analyze Data

- **Analysis # 1: Visualizing total comments per hour in real-time** \
We can take advantage of the fact that our data is being updated hourly to create a "real-time" histogram with the hour on the x-axis and comment count on the y-axis. This type of visualization can be the basis for further analysis. 
<p align="center">
  <img src="https://github.com/mchion/reddit_ELT_pipeline/blob/main/images/dashboard1.png" width="800"/>
</p>

- **Analysis #2: Percentage of posts that meet time threshold** \
One question that a machine learning engineer might have is - How long does it take until a post has no more comments added to it from users?\
\
We know that posts are archived after 6 months, so there is a hard stop to the amount of time it takes until a post has no more comments. But a quick observation of posts tells us that users usually stop commenting after only a few weeks.\
\
One way to start would be to use historical data and find out the average amount of time it takes until users stop commenting on a post. Taking this a step further, we can plot the time it takes until there are no more posts on the x-axis and the **percentage** of posts that meet this threshold on the y-axis. This can be useful if we wanted to determine how long it takes until a certain percentage of posts are no longer active. 

- **Analysis #2a: Towards predicting when a post will have no more comments**:\
The real-time nature of our app gives machine learning engineers more of a playground to make predictions and test them against their model rather than using historical data as their training and test data. A real-time dashboard can also provide monitoring if the ML models start to deviate from expected predictions. 

<p align="center">
  <img src="https://github.com/mchion/reddit_ELT_pipeline/blob/main/images/dashboard3.png" width="800"/>
</p>

## Futher Directions and Considerations

- **Monitoring and Observability**: Because our app is entirely run on a cloud platorm, monitoring and observability are especially important since we cannot be expected to log in and monitor the data pipeline on a regular basis. In fact, our data pipeline did fail in the first month due to a change in the API wrapper and we thus had to retrieve historical comments and merge them with the newer data. 
