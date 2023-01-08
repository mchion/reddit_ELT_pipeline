import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timedelta, date
import pytz
import numpy as np

import plotly.express as px

pd.options.mode.chained_assignment = None

st.set_page_config(
    page_title='r/DataEngineering Dashboard',
    layout='wide'
)

today = date.today()
time_now = (datetime.now().astimezone(pytz.timezone("UTC"))- timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

minimal_timezone_set = [
    { 'offset': '-08:00', 'label': '(GMT-08:00) Pacific Time', 'tzCode': 'America/Los_Angeles' },
    { 'offset': '-07:00', 'label': '(GMT-07:00) Mountain Time', 'tzCode': 'America/Denver' },
    { 'offset': '-06:00', 'label': '(GMT-06:00) Central Time', 'tzCode': 'America/Chicago' },
    { 'offset': '-05:00', 'label': '(GMT-05:00) Eastern Time', 'tzCode': 'America/New_York' },
    { 'offset': '-04:00', 'label': '(GMT-04:00) Atlantic Time - Halifax', 'tzCode': 'America/Halifax' },
    { 'offset': '-03:00', 'label': '(GMT-03:00) Buenos Aires', 'tzCode': 'America/Argentina/Buenos_Aires' },
    { 'offset': '-02:00', 'label': '(GMT-02:00) Sao Paulo', 'tzCode': 'America/Sao_Paulo' },
    { 'offset': '-01:00', 'label': '(GMT-01:00) Azores', 'tzCode': 'Atlantic/Azores' },
    { 'offset': '+00:00', 'label': '(GMT+00:00) London', 'tzCode': 'Europe/London' },
    { 'offset': '+01:00', 'label': '(GMT+01:00) Berlin', 'tzCode': 'Europe/Berlin' },
    { 'offset': '+02:00', 'label': '(GMT+02:00) Helsinki', 'tzCode': 'Europe/Helsinki' },
    { 'offset': '+03:00', 'label': '(GMT+03:00) Istanbul', 'tzCode': 'Europe/Istanbul' },
    { 'offset': '+04:00', 'label': '(GMT+04:00) Dubai', 'tzCode': 'Asia/Dubai' },
    { 'offset': '+04:30', 'label': '(GMT+04:30) Kabul', 'tzCode': 'Asia/Kabul' },
    { 'offset': '+05:00', 'label': '(GMT+05:00) Maldives', 'tzCode': 'Indian/Maldives' },
    { 'offset': '+05:30', 'label': '(GMT+05:30) India Standard Time', 'tzCode': 'Asia/Calcutta' },
    { 'offset': '+05:45', 'label': '(GMT+05:45) Kathmandu', 'tzCode': 'Asia/Kathmandu' },
    { 'offset': '+06:00', 'label': '(GMT+06:00) Dhaka', 'tzCode': 'Asia/Dhaka' },
    { 'offset': '+06:30', 'label': '(GMT+06:30) Cocos', 'tzCode': 'Indian/Cocos' },
    { 'offset': '+07:00', 'label': '(GMT+07:00) Bangkok', 'tzCode': 'Asia/Bangkok' },
    { 'offset': '+08:00', 'label': '(GMT+08:00) Hong Kong', 'tzCode': 'Asia/Hong_Kong' },
    { 'offset': '+08:30', 'label': '(GMT+08:30) Pyongyang', 'tzCode': 'Asia/Pyongyang' },
    { 'offset': '+09:00', 'label': '(GMT+09:00) Tokyo', 'tzCode': 'Asia/Tokyo' },
    { 'offset': '+09:30', 'label': '(GMT+09:30) Central Time - Darwin', 'tzCode': 'Australia/Darwin' },
    { 'offset': '+10:00', 'label': '(GMT+10:00) Eastern Time - Brisbane', 'tzCode': 'Australia/Brisbane' },
    { 'offset': '+10:30', 'label': '(GMT+10:30) Central Time - Adelaide', 'tzCode': 'Australia/Adelaide' },
    { 'offset': '+11:00', 'label': '(GMT+11:00) Eastern Time - Melbourne, Sydney', 'tzCode': 'Australia/Sydney' },
    { 'offset': '+12:00', 'label': '(GMT+12:00) Nauru', 'tzCode': 'Pacific/Nauru' },
    { 'offset': '+13:00', 'label': '(GMT+13:00) Auckland', 'tzCode': 'Pacific/Auckland' },
    { 'offset': '+14:00', 'label': '(GMT+14:00) Kiritimati', 'tzCode': 'Pacific/Kiritimati' },
    { 'offset': '-11:00', 'label': '(GMT-11:00) Pago Pago', 'tzCode': 'Pacific/Pago_Pago' },
    { 'offset': '-10:00', 'label': '(GMT-10:00) Hawaii Time', 'tzCode': 'Pacific/Honolulu' },
    { 'offset': '-10:00', 'label': '(GMT-10:00) Tahiti', 'tzCode': 'Pacific/Tahiti' },
    { 'offset': '-09:00', 'label': '(GMT-09:00) Alaska Time', 'tzCode': 'America/Anchorage' }
]
menu_df = pd.DataFrame(minimal_timezone_set).drop('offset',axis=1)
menu_df = menu_df[['tzCode','label']]
menu_dict = dict(menu_df.values)


# Create connection to BigQuery database
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

with st.sidebar:
    user_time_zone = st.selectbox(
        'Select your Time Zone:',
        list(menu_dict.keys()),
        format_func=lambda x:menu_dict[x])

def timeline_chart():
    query_comment_timestamps = (
        """
        SELECT datetime(format_date('%Y-%m-%d %H:00:00', c_time)) as day_hour, count(c_time) as count
        FROM `comments_dataset.comments_table`
        WHERE c_time > CURRENT_TIMESTAMP - INTERVAL '168' HOUR
        GROUP BY day_hour
        ORDER BY day_hour DESC
        """
        
    )

    query_config = bigquery.QueryJobConfig(
        # query_parameters=[
        #     bigquery.ScalarQueryParameter("timeframe", "STRING", timeframe)
        #     ]
        )
    df = client.query(query_comment_timestamps, job_config = query_config).to_dataframe()
    df['day_hour'] = df['day_hour'].dt.tz_localize('utc').dt.tz_convert(user_time_zone)
    
    fig = px.bar(df,
            x='day_hour',
            y='count',
            hover_data={"day_hour": "|%b %d, %H:00"},
            labels={
                "count": "Total Comments",
                "day_hour": "Day/Time"
                })
    fig.update_xaxes(
        dtick=4*60*60*1000,
        tickformat="%H\n%a%_d"
    )
    
    st.markdown("<h3 style='text-align: center;'> Total Comments by Hour for <a href = 'https://www.reddit.com/r/dataengineering/'><font color='#FF5700'>r/dataengineering</font></a></h3>", unsafe_allow_html=True)
    
    st.plotly_chart(fig, use_container_width=True)

def analyze_wait_time():
    
    st.markdown("<h3 style='text-align: center;'>APPROACH #1 </h3>", unsafe_allow_html=True)
    
    st.markdown("""Choose a wait time that leads to an acceptable percentage of posts that are incorrectly labeled. For instance, if you choose a wait time of 4 days, and the percentage of incorrectly labeled posts is 1%, then this percentage may be an acceptable number for you.""")
    
    st.markdown("<h4 style='text-align: center;'>Try it out:</h4>", unsafe_allow_html=True)
    st.markdown("<h5 style='text-align: center;'>How long after the last comment should we wait until we a consider a post <strong>done</strong>?</h5>", unsafe_allow_html=True)
    
    col1,col2,col3 = st.columns([1,1,1])
    with col2:
        user_option = st.selectbox(
            'Select number of hours',
            ('6 hours', '12 hours', '18 hours', '24 hours (1 day)', 
             '48 hours (2 days)', '72 hours (3 days)', '96 hours (4 days)',
             '120 hours (5 days)', '148 hours (6 days)', '172 hours (7 days)'),
            label_visibility='hidden')

    if user_option == "6 hours": 
        option = 6
    elif user_option == "12 hours":
        option = 12
    elif user_option == "24 hours (1 day)":
        option = 24
    elif user_option == "48 hours (2 days)":
        option = 48 
    elif user_option == "72 hours (3 days)":
        option = 72
    elif user_option == "96 hours (4 days)":
        option = 92 
    elif user_option == "120 hours (5 days)":
        option = 120 
    elif user_option == "148 hours (6 days)":
        option = 148
    elif user_option == "172 hours (7 days)":
        option = 172
    # Find total posts since inception
    query_number_of_posts = (
            """
            SELECT count(distinct s_id) as s_count
            FROM `comments_dataset.comments_table`
            """
        )
    total_posts = next(client.query(query_number_of_posts).result())[0]
    
    # Create df showing comments passed wait period threshold
    query = (
            """
            SELECT s_id, s_time, s_title, ARRAY_AGG(c_time) as c_time_list, count(*) as c_count
            FROM `comments_dataset.comments_table`
            GROUP BY s_id, s_time, s_title
            ORDER BY s_time DESC
            """
        )
    df = client.query(query).to_dataframe()

    def sort_list(comment_list):
        x = np.sort(comment_list)
        return x

    def find_time_diff(comment_list):
        
        x = pd.DataFrame(comment_list, columns = ['Column_A'])
        last_c_time = x.iloc[-1].item()
        time_diff = x['Column_A'].diff()
        return pd.Series([time_diff,last_c_time])

    def find_prev_threshold(time_diff_list):
        passed_threshold = np.where((time_diff_list > np.timedelta64(option,'h')).any(), "Yes","")
        return pd.Series([passed_threshold])

    # Sorts c_time_list in chronological order
    df['c_time_list'] = df['c_time_list'].apply(sort_list)

    # finds the timedelta between times in c_time_list
    df[['time_diff','last_c_time']] = df['c_time_list'].apply(find_time_diff)

    # Given a certain wait period, calculates the date/time the post passed this wait period threshold
    df['dt_cross_threshold'] = (np.where(df['last_c_time'].dt.tz_localize('UTC').dt.to_pydatetime() < datetime.now().astimezone(pytz.timezone("UTC"))- timedelta(hours=option), 
                                        df['last_c_time'].dt.to_pydatetime() + timedelta(hours=24), 
                                        "")
                            )

    # Given a certain wait period, calculates if a comment was posted after the wait period
    df[['prev_c_threshold']] = df['time_diff'].apply(find_prev_threshold)

    df_new = df[df['prev_c_threshold']=='Yes']
    posts_that_crossed_threshold = df[df['prev_c_threshold']=='Yes'].shape[0]
    
    percentage_wrong = posts_that_crossed_threshold/total_posts

    st.markdown(f"<h5 style='text-align: center;'> Given a wait time of {user_option}:</h5>", unsafe_allow_html=True)
    
    st.markdown(f"""<div style='text-align: center;'><strong style="font-size:24px;">{1 - percentage_wrong:.0%}</strong> of posts were correctly labeled as done 
                (<strong>{total_posts-posts_that_crossed_threshold}</strong> out of {total_posts} total posts since 1/7/2023)</div>""", unsafe_allow_html=True)
    
    st.markdown(f"""<div style='text-align: center;'><strong style="font-size:24px;">{percentage_wrong:.0%}</strong> of posts were incorrectly labeled as done 
                (<strong>{posts_that_crossed_threshold}</strong> out of {total_posts} total posts since 1/7/2023)</div>""", unsafe_allow_html=True)
    st.write('')
    st.write('')
    
    st.markdown(f"""Let's examine these **{posts_that_crossed_threshold}** posts that at one point we considered **done** 
            because there were no new comments after **{user_option}** but then which became **un-done** when one or more comments
            were added after this wait time.""")
    
    df_clean = df_new[['s_id','s_time','s_title','c_count', 'last_c_time']]
    df_clean.rename(columns={'s_id':'Post ID','s_time':'Post Date','s_title': 'Post Title (truncated)',
                             'c_count': 'Comments', 'last_c_time': 'Last Comment Time'}, 
                    inplace=True)
    df_clean['Post Title (truncated)'] = df_clean['Post Title (truncated)'].str[:60]
    df_clean.reset_index(drop=True, inplace=True)
    df_clean["Post Date"] = df_clean["Post Date"].dt.strftime('%Y-%m-%d')
    df_clean["Last Comment Time"] = df_clean["Last Comment Time"].dt.strftime('%Y-%m-%d %X')
    

    st.dataframe(df_clean, use_container_width = True)
    
    #st.markdown("<h4 style='text-align: center;'>Select a post index to see its time plot of comments: </h4>", unsafe_allow_html=True)
    col1,col2= st.columns([.35,1])
    with col1:
        user_input_raw = st.selectbox(
            'Select a Post ID to see more detail:',
            list(df_clean.index.astype(str) + " - " + df_clean['Post ID']),
            label_visibility='visible'
            )
    
        user_input = user_input_raw.split(" - ")[1]
        new1 = df_new[df_new['s_id']==user_input].loc[:,'time_diff'].iloc[0].to_frame()
        new1.rename(columns={'Column_A': 'time_diff'},inplace=True)
        new2 = pd.DataFrame(df_new[df_new['s_id']==user_input].loc[:,'c_time_list'].iloc[0],columns=['comment_time'])
        new3 = pd.concat([new2,new1], axis=1)
        new3['time_diff'].fillna(pd.Timedelta(seconds=0),inplace=True)
        new3['hours_diff'] = new3['time_diff']/pd.Timedelta("1 hour")

        new_view = new3[['comment_time','hours_diff']]
        new_view = new_view.rename(columns={'comment_time':'Comment Timestamp', 'hours_diff': '*Hours'})
        new_view['*Hours'] = new_view['*Hours'].apply(lambda x: '{:,.2f}'.format(x))
        new_view['Comment Timestamp'] = new_view['Comment Timestamp'].dt.strftime('%Y-%m-%d %X')
    
        st.dataframe(new_view, use_container_width = True)
        st.markdown("<em>\*Hours since previous comment</em>", unsafe_allow_html=True )
    with col2:
        fig2 = px.line(new3, 
                    x='comment_time',
                    y='hours_diff',
                    labels={
                        "comment_time": "Comment Timestamp",
                        "hours_diff": "Hours"},
                    markers=True,
                    title = f'Time plot for {user_input_raw}')
        
        fig2.update_layout(title_x=0.5)
        
        fig2.add_hline(y=option,
                    line_width=1, 
                    line_dash="dash", 
                    line_color="red", 
                    annotation_text=f"{option}hr threshold",
                    annotation_position="bottom left")

        st.plotly_chart(fig2, use_container_width=True)
    
    st.write('')
    st.write('')
    st.markdown("<h3 style='text-align: center;'>APPROACH #2 </h3>", unsafe_allow_html=True)
    st.write('')
    st.markdown(f"""Use machine learning or perform 
                a time series analysis on each individual post in order to predict when the last comment has been added. 
                A data scientist or ML Engineer would need to investigate further since the focus of this project is mainly on the ETL of the data.""")
    
def most_recent_comments():

    hide_table_row_index = """
                <style>
                thead tr th:first-child {display:none}
                tbody th {display:none}
                </style>
                """
    st.markdown(hide_table_row_index, unsafe_allow_html=True)

    query_current_comments = (
        """
        SELECT DATETIME(c_time) as Time_Submitted, c_body as Comment
        FROM `comments_dataset.comments_table`
        ORDER BY Time_Submitted DESC
        LIMIT 25
        """
    )
    df = pd.read_gbq(query_current_comments, credentials=credentials)
    df['Time_Submitted'] = df['Time_Submitted'].dt.tz_localize('utc').dt.tz_convert(user_time_zone).dt.strftime("%I:%M %p %Z")
    st.markdown('### 25 Most Recent Comments')
    st.dataframe(df)


## functions end here, title, sidebar setting and descriptions start here

st.markdown("<h1 style='text-align: center;'>Towards Predicting When A Reddit Post Is \"Done\"</h1>",unsafe_allow_html=True)
st.write("")
st.markdown("""A post on Reddit can contain zero or more comments. We can view comments as a stream of data over time (a time series).
            The plot below shows the total number of comments per hour for the entire <a href = 'https://www.reddit.com/r/dataengineering/'> <font color='#FF5700'>r/dataengineering</font></a> 
            subreddit (a subreddit is a collection of posts).""", unsafe_allow_html=True)

timeline_chart()

st.markdown("""We are trying to predict when a post is **done**. A post is considered **done** when no new comments will be added to the post in the future.
            Only archived posts are with 100% certainty **done** because after a post is archived, no new comments are allowed on that post.
            Posts are archived automatically after 6 months. 
            However, most posts are usually **done** much sooner than 6 months (usually within a week), and this sooner point in time is what we are attempting to predict.""")

analyze_wait_time()


#most_recent_comments()

with st.sidebar:
    with st.container():
        
        date_now = datetime.now().astimezone(pytz.timezone(user_time_zone)).strftime("%a %b %d %Y")
        time_now = datetime.now().astimezone(pytz.timezone(user_time_zone)).strftime("%I:%M %p %Z")
        
        st.markdown(f'''Current Date/Time: \n 
                    {date_now}  {time_now}''')
    
    #st.button("Refresh app")
    #st.markdown('*Note: New comments ingested hourly*')
    
    with st.expander("Click to learn more about this dashboard"):
        st.markdown(f"""
        This dashboard is designed as a mock user endpoint for a data engineering project.
        Details about the data engineering pipeline can be found at [here](https://github.com/mchion/reddit_serverless_stream).
        
        This is NOT a data science or machine learning project.
        
        *All data on this dashboard is active and constantly changing based on new incoming data. 
        New comments from the dataengineering subreddit are ingested to the database on an hourly basis.*

        """)
