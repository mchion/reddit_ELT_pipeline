# Serverless Reddit Stream

EtLT pipeline that automatically extracts posts and comments from Reddit using 

![Setup Overview Diagram](/images/Pipeline.svg)

The extraction is scheduled to run hourly. The end goal is to 
[Click here to view the interactive dashboard](https://mchion-reddit-serverless-stream-streamlit-app-axxcn6.streamlit.app/)

## Data Extraction

- **Reddit API**: 
Reddit's API limits the amount of comments that can be retrieved to 100. Thus, we have to make repeated calls and make sure we are only grabbing new posts from `last_timestamp`



## Data Loading

Most of the transformation logic takes place using python scripting and Pandas. 

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



The schema is 



## Data Transformation

Once the data is loaded into our BigQuery data warehouse, we use SQL and pandas to transform the data into what we want

## Data Visualization

With the data transformed, we were then able to build out an interactive dashboard using Streamlit. Streamlit was chosen because it is build using python and super customizable. 

![Dashboard General](/images/dashboard1.png)
![Dashboard General](/images/dashboard2.png)

## Unit Testing

Planning to incorporate some unit testing involving PyTest. 

## Futher Directions and Considerations

-- Machine 

- **Handling Big Data**: While 
