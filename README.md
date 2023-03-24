# Cloud-Based ELT Pipeline

ELT pipeline that extracts posts and comments from Reddit, loads it into BigQuery, and transforms it into a dashboard.

![Setup Overview Diagram](/images/Pipeline.svg)

Data extraction is scheduled to run hourly using Cloud Run. The end goal is to try to determine when a Reddit post is "done", meaning when no new comments will be added to the post in the future.
[**Click here to view the interactive dashboard**](https://mchion-reddit-elt-pipeline-streamlit-app-wvgpbg.streamlit.app/).

## Data Extraction

Extraction is done by a Python app that is Dockerized and placed into Artifact Registry. Cloud Run then schedules this app to run every hour. 

- **Reddit API**: [PRAW](https://praw.readthedocs.io/en/stable/index.html), a Python Reddit API wrapper, was used to make comment extraction easier. However, the API only allows a fixed extraction amount of 100 comments at a time. This means that it is not possible to extract any other amount – say 50 comments – besides 100. Thus, in order to prevent loading duplicate comments into the database, the app discards comments that have a timestamp older than or equal to the last timestamp in our database. Although this could potentially discard comments that have equal timestamps, because this particular subreddit the app is extracting from is not usually very active, the likelihood is small that this would happen and it's not the end of the world if it does.\
\
In addition, being able to extract only 100 comments at time means that we need to extract pretty frequently because if we were to wait too long, more than 100 comments could have been posted by the time we extract, and we would be unable to retrieve those comments in the future. Because this subreddit has a relatively low, steady comment rate per hour, extracting **every hour** provides us with enough cushion to make sure there are less than 100 comments between extraction times.


## Data Loading

Data is loaded directly into our data warehouse with minimal transformations for timestamp formatting and proper time zone. 

- **Data Model**:

![Data Model](/images/schema.svg)



## Data Transformation

Once the data is loaded into our data warehouse BigQuery, we use SQL and pandas to transform the data into what we want. This transformation occurs in Python code embedded in our dashboard app. 

- **Scalability Consideration**: Because we are not working with a large amount of data, we can do things like load our entire database table into our  dashboard app without any scalability issues. However, if we were working with Big Data, we would have to use a cloud tools like dbt or move the entire ELT process into a cloud platform like Snowflake or Databricks. \
\
If we were dealing with a more popular subreddit where the rate of comments becomes almost like a constantly streaming data source, we would have to consider using a Kafka message queue before possibly loading it into an object storage like AWS S3. 

## Data Visualization

Once the data is transformed, building out an interactive dashboard is relatively straightforward for the most part. Although there are plenty of dashboard tools (Tableu, PowerBI, etc.) one can use to build a dashboard, I chose [**Streamlit**](https://streamlit.io/) because it is an all Python, open-source framework that is easy to use and super customizable. Most importantly, it's free to deploy and share your dashboard to the public.  

![Dashboard General](/images/dashboard1.png)
![Dashboard General](/images/dashboard2.png)

## Unit Testing

I plan to incorporate some unit testing in future impelmentations using a framework like pytest. 

## Monitoring and Observability

Because our app is entirely run on a cloud platorm, monitoring and observability are especially important since we cannot be expected to log in and monitor manually. I mention this because when I initially launched this app, it stopped after a month, and I was not able to detect it until a month after the stop. 

## Futher Directions and Considerations

- **Machine Learning**: Using machine learning or time series analysis to find patterns in the rate of comments would complete this project. 

