# Cloud-Based ELT Pipeline

ELT pipeline that extracts posts and comments from Reddit, loads it into BigQuery, and transforms it into a dashboard.

![Setup Overview Diagram](/images/Pipeline.svg)

Data extraction is scheduled to run hourly using Cloud Run. The end goal is to try to determine when a Reddit post is "done", meaning when no new comments will be added to the post in the future.
[Click here to view the interactive dashboard](https://mchion-reddit-elt-pipeline-streamlit-app-wvgpbg.streamlit.app/).

## Data Extraction

Extraction is done by a python app that is dockerized and placed into Artifact Registry. Cloud Run then schedules 

- **Reddit API**: [PRAW](https://praw.readthedocs.io/en/stable/index.html), a Python Reddit API wrapper, was used to make comment extraction easier. However, the API only allows a fixed extraction amount of 100 comments at a time. This means that it is not possible to extract any other amount – say 50 comments – besides 100. Thus, in order to prevent loading duplicate comments into the database, the app discards comments that have a timestamp older than or equal to the last timestamp in our database. Although this could potentially discard comments that have equal timestamps, because this particular subreddit the app is extracting from is not usually very active, the likelihood is small that this would happen and it's not the end of the world if it does.


## Data Loading

Data is mostly loaded directly into our data warehouse with only minimal transformations. 

- **Data Schema**:

  | Column Name | Value | 
  | ------------ | --------- | 
  | c_id | comment ID |
  | c_author | comment author |
  | c_body | comment message |
  | c_time | comment timestamp |
  | s_id | post ID |
  | s_author | post author |
  | s_title | post title |
  | s_time | post timestamp |

- **Scalability**: If our data batches were larger, we would want to use something like Spark Streaming



## Data Transformation

Once the data is loaded into our BigQuery data warehouse, we use SQL and pandas to transform the data into what we want. This gives us more flexibility in the end. 

## Data Visualization

With the data transformed, we were then able to build out an interactive dashboard using Streamlit. Streamlit was chosen because it is build using python and super customizable. 

![Dashboard General](/images/dashboard1.png)
![Dashboard General](/images/dashboard2.png)

## Unit Testing

Planning to incorporate some unit testing involving PyTest. 

## Futher Directions and Considerations

- **Machine Learning**: Using machine learning or time series analysis to find patterns in the rate of comments  

- **Handling Big Data**: I have chosen a subreddit with relatively low, steady comment rate per hour. However, if we were dealing with a more popular subreddit, we would have to change the streaming method. Maybe use a Kafka message queue before loading it into an S3 database. 
