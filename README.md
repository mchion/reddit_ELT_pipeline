# Cloud-Based ELT Pipeline

ELT pipeline that extracts posts and comments from Reddit, loads it into BigQuery, and transforms it into a dashboard. 
[Click here to view the interactive dashboard](https://mchion-reddit-serverless-stream-streamlit-app-axxcn6.streamlit.app/)

![Setup Overview Diagram](/images/Pipeline.svg)

The extraction is scheduled to run hourly. The end goal is to 
[Click here to view the interactive dashboard](https://mchion-reddit-serverless-stream-streamlit-app-axxcn6.streamlit.app/)

## Data Extraction

- **Reddit API**: The API has a rate limit of a fixed 100 comments per request. Thus, we have to make repeated calls and make sure we are only grabbing new posts from `last_timestamp`


## Data Loading

Data is mostly loaded directly into our data warehouse with only minimal transformations . Batches of 100 comments are first loaded into 

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
