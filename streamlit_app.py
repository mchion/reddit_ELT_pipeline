import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timedelta, date
import pandas_gbq
import pytz

import plotly.express as px

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
        SELECT DATETIME(c_time) as time
        FROM `comments_dataset.comments_table`
        WHERE c_time > @timeframe
        ORDER BY time DESC
        """
    )
    timeframe = (datetime.now().astimezone(pytz.timezone("UTC"))- timedelta(hours=168)).strftime('%Y-%m-%dT%H:%M:%SZ')
    query_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("timeframe", "STRING", timeframe)
            ]
        )
    dft1 = client.query(query_comment_timestamps, job_config = query_config).to_dataframe()
    dft1['time'] = dft1['time'].dt.tz_localize('utc').dt.tz_convert(user_time_zone)
    dft2 = dft1.resample('H',on='time').count()
    dft2.index.rename('day_hour', inplace=True)
    dft2.rename(columns={'time':'count'},inplace=True)
    dft2.reset_index(inplace=True)
    
    fig = px.bar(dft2,
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
    
    col1,col2 = st.columns([1,1])
    with col1:
        st.markdown('## Total Comments by Hour')
    with col2: 
        st.write('')
        st.write('')
        st.markdown(f'Timezone: {user_time_zone}')
    
    st.plotly_chart(fig, use_container_width=True)

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
    st.table(df)


## functions end here, title, sidebar setting and descriptions start here

st.markdown('# r/DataEngineering Dashboard')
st.write("")

timeline_chart()
most_recent_comments()

with st.sidebar:
    with st.container():
        
        date_now = datetime.now().astimezone(pytz.timezone(user_time_zone)).strftime("%a %b %d %Y")
        time_now = datetime.now().astimezone(pytz.timezone(user_time_zone)).strftime("%I:%M %p %Z")
        
        st.markdown(f'''Current Date/Time: \n 
                    {date_now}  {time_now}''')
    
    st.button("Refresh app")
    st.markdown('*Note: New comments ingested hourly*')
    
    with st.expander("Click to learn more about this dashboard"):
        st.markdown(f"""
        This dashboard is primarily focused on machine learning. 
        
        *Report updated on {str(today)}.*  
        """)
