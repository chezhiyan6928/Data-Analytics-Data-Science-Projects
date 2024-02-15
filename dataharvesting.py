# importing necessary libraries

from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st




#connection to the API Key

def Api_Connect():
    api_ID = 'AIzaSyAYBhrTlk7Pg9Eym4ZTCfUiOadxpuxSaSQ'
    api_service_name = "youtube"
    api_version = "v3"
    
    youtube = build(api_service_name, api_version, developerKey=api_ID)

    return youtube

youtube = Api_Connect()



# Getting channel credentials by connecting with an API client

def getChannelInfo(channel_id):

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    for i in response['items']:
        data = dict(channel_id = i['id'],
                    channel_name = i['snippet']['title'],
                    subscribers = i['statistics']['subscriberCount'],
                    views = i['statistics']['viewCount'],
                    total_videos = i['statistics']['videoCount'],
                    channel_description = i['snippet']['description'],
                    playlist_id = i['contentDetails']['relatedPlaylists']['uploads'])
    return data



# Getting video ids by connecting with an API client

def getVideoIds(channel_id):
    video_ids = []
    response = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id).execute()

            
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part = 'snippet',
            playlistId = playlist_id,
            maxResults = 50,
            pageToken = next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids



# Getting videos credentials by connecting with an API client

def getVideoInfo(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id)
        response = request.execute()

        for item in response['items']:
            data = dict(channel_name = item['snippet']['channelTitle'],
                        channel_id = item['snippet']['channelId'],
                        video_id = item['id'],
                        title = item['snippet']['title'],
                        tags = item['snippet'].get('tags'),
                        thumbnail = item['snippet']['thumbnails']['default']['url'],
                        description = item['snippet'].get('description'),
                        published_date = item['snippet']['publishedAt'],
                        duration = item['contentDetails']['duration'],
                        views = item['statistics'].get('viewCount'),
                        comment_count = item['statistics'].get('commentCount'),
                        likes_count = item['statistics'].get('likeCount'),
                        favorite_count = item['statistics']['favoriteCount'],
                        definition = item['contentDetails']['definition'],
                        caption_status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data



# Getting comment details by connecting with an API client

def getCommentInfo(video_ids):

    comment_data = []

    try:

        for video_id in video_ids:
            request = youtube.commentThreads().list(
                            part="snippet",
                            videoId=video_id,
                            maxResults = 50)
            response = request.execute()

            for item in response['items']:
                data = dict(comment_id = item['snippet']['topLevelComment']['id'],
                            video_id = item['snippet']['topLevelComment']['snippet']['videoId'],
                            comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_published = item['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_data.append(data)

    except:
        pass
    return comment_data



# Getting playlist details by connecting with an API client

def getPlaylistInfo(channel_id):
    next_page_token = None

    playlist_data = []

    while True:
        request = youtube.playlists().list(
                                    part="snippet, contentDetails",
                                    channelId=channel_id,
                                    maxResults = 50,
                                    pageToken = next_page_token)
        response = request.execute()

        for item in response['items']:
            data = dict(playlist_id = item['id'],
                        title = item['snippet']['title'],
                        channel_id = item['snippet']['channelId'],
                        channel_name = item['snippet']['channelTitle'],
                        published_date = item['snippet']['publishedAt'],
                        video_count = item['contentDetails']['itemCount'])
            playlist_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_data



# Connecting mongodb with python

client = pymongo.MongoClient("mongodb://localhost:27017")

mydb = client["youtube"]



# Adding all the functions into a single function.

def channelDetails(channel_id):
    ch_details = getChannelInfo(channel_id)
    pl_details = getPlaylistInfo(channel_id)
    vi_ids = getVideoIds(channel_id)
    vi_details = getVideoInfo(vi_ids)
    com_details = getCommentInfo(vi_ids)

    coll1 = mydb["channel_details"]

    coll1.insert_one({"channel_info": ch_details,
                      "playlist_info": pl_details,
                      "video_info": vi_details,
                      "comment_info": com_details})
    
    return "Details uploaded successfully!!!"



# Creating sql tables for channel details

def getChannelDetails():

    db = mysql.connector.connect(host = "localhost",
                                port = '3306',
                                user = "root",
                                password = "Dhana@2023",
                                database = "youtube_data")

    cursor = db.cursor()

    drop_query = "drop table if exists channels"
    cursor.execute(drop_query)
    db.commit()

    try:
        create_query = "create table if not exists channels(channel_id varchar(255) primary key, channel_name varchar(255), channel_subscribers bigint, channel_views bigint, total_videos bigint, channel_description text, playlist_id varchar(255))"
        cursor.execute(create_query)
        db.commit()

    except:
        print("Channel table already created!")

    ch_list = []

    mydb = client["youtube"]
    coll1 = mydb["channel_details"]

    for channel_data in coll1.find({}, {"_id": 0, "channel_info":1}):
        ch_list.append(channel_data["channel_info"])

    ch_df = pd.DataFrame(ch_list)

    for index, row in ch_df.iterrows():
        insert_query = '''insert into channels(channel_id,
                                                channel_name,
                                                channel_subscribers,
                                                channel_views,
                                                total_videos,
                                                channel_description,
                                                playlist_id)
                                                    
                                                values(%s, %s, %s, %s, %s, %s, %s)'''
            
        values = (row['channel_id'],
                row['channel_name'],
                row['subscribers'],
                row['views'],
                row['total_videos'],
                row['channel_description'],
                row['playlist_id'])
            
        try:
            cursor.execute(insert_query, values)
            db.commit()
        except:
            print("Channel values already inserted!!!")



# creating sql tables for playlists
import datetime

def getPlaylistDetails():

        db = mysql.connector.connect(host = "localhost",
                                port = '3306',
                                user = "root",
                                password = "Dhana@2023",
                                database = "youtube_data")

        cursor = db.cursor()

        drop_query = "drop table if exists playlists"
        cursor.execute(drop_query)
        db.commit()

        try:
                create_query = '''create table if not exists playlists(playlist_id varchar(255) primary key,
                                                        title varchar(255),
                                                        channel_id varchar(255),
                                                        channel_name varchar(255),
                                                        published_date datetime,
                                                        video_count int)'''
        except:
                print("playlist table already created!")

        cursor.execute(create_query)
        db.commit()

        pl_list = []
        mydb = client["youtube"]
        coll1 = mydb["channel_details"]

        for pl_data in coll1.find({}, {"_id": 0, "playlist_info":1}):
                for i in range(len(pl_data['playlist_info'])):
                        pl_list.append(pl_data['playlist_info'][i])

        pl_df = pd.DataFrame(pl_list)

        for index, row in pl_df.iterrows():
                insert_query = '''insert into playlists(playlist_id,
                                                        title,
                                                        channel_id,
                                                        channel_name,
                                                        published_date,
                                                        video_count)
                                                        
                                                        values(%s, %s, %s, %s, %s, %s)'''
                
                values = (row['playlist_id'],
                        row['title'],
                        row['channel_id'],
                        row['channel_name'],
                        datetime.datetime.strptime(row['published_date'], '%Y-%m-%dT%H:%M:%SZ'),
                        row['video_count'])
                try:
                        cursor.execute(insert_query, values)
                        db.commit()
                except:
                        print("Playlist values already inserted!!!")
        


# creating sql tables for video details

def getVideoDetails():

        db = mysql.connector.connect(host = "localhost",
                                port = '3306',
                                user = "root",
                                password = "Dhana@2023",
                                database = "youtube_data")

        cursor = db.cursor()

        drop_query = "drop table if exists videos"
        cursor.execute(drop_query)
        db.commit()

        try:
                create_query = '''create table if not exists videos(channel_name varchar(255),
                                                                channel_id varchar(255),
                                                                video_id varchar(255) primary key,
                                                                title varchar(255),
                                                                tags text,
                                                                thumbnail varchar(255),
                                                                description text,
                                                                published_date datetime,
                                                                duration time,
                                                                views int,
                                                                comment_count int,
                                                                likes_count int,
                                                                favorite_count int,
                                                                definition varchar(25),
                                                                caption_status varchar(25))'''
        except:
                print("video table already created!")

        cursor.execute(create_query)
        db.commit()

        vi_list = []
        mydb = client["youtube"]
        coll1 = mydb["channel_details"]

        for vi_data in coll1.find({}, {"_id": 0, "video_info":1}):
                for i in range(len(vi_data['video_info'])):
                        vi_list.append(vi_data['video_info'][i])

        vi_df = pd.DataFrame(vi_list)
        vi_df["tags"] = vi_df["tags"].apply(lambda x: ",".join(x) if isinstance(x, list) else x)

        def update_time_format(time_string):
            """Converts ss, mm:ss, or hh:mm:ss to hh:mm:ss format."""
            parts = time_string.split(":")
            if len(parts) == 1:  # ss format
                    return "00:00:" + parts[0].zfill(2)
            elif len(parts) == 2:  # mm:ss format
                    minutes = parts[0].zfill(2)
                    seconds = parts[1].zfill(2)
                    hours = "00"  # Default to 00 for hours
                    return f"{hours}:{minutes}:{seconds}"
            elif len(parts) == 3:  # hh:mm:ss format
                    return time_string  # No change needed
            else:
                    return time_string  # No change for other formats
        
        vi_df['duration'] = vi_df['duration'].apply(update_time_format)

        for index, row in vi_df.iterrows():
                insert_query = '''insert into videos(channel_name,
                                                channel_id,
                                                video_id,
                                                title,
                                                tags,
                                                thumbnail,
                                                description,
                                                published_date,
                                                duration,
                                                views,
                                                comment_count,
                                                likes_count,
                                                favorite_count,
                                                definition,
                                                caption_status)
                                                        
                                                values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                
                values = (row['channel_name'],
                        row['channel_id'],
                        row['video_id'],
                        row['title'],
                        row['tags'],
                        row['thumbnail'],
                        row['description'],
                        datetime.datetime.strptime(row['published_date'], '%Y-%m-%dT%H:%M:%SZ'),
                        row['duration'],
                        row['views'],
                        row['comment_count'],
                        row['likes_count'],
                        row['favorite_count'],
                        row['definition'],
                        row['caption_status'])
                try:
                        cursor.execute(insert_query, values)
                        db.commit()
                except:
                        print("videos values already inserted!!!")



# creating sql tables for comment details

def getCommentDetails():

    db = mysql.connector.connect(host = "localhost",
                        port = '3306',
                        user = "root",
                        password = "Dhana@2023",
                        database = "youtube_data")

    cursor = db.cursor()

    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    db.commit()

    try:
        create_query = '''create table if not exists comments(comment_id varchar(255) primary key,
                                                                video_id varchar(255),
                                                                comment_text text,
                                                                comment_author varchar(255),
                                                                comment_published datetime)'''
    except:
        print("comments table already created!")

    cursor.execute(create_query)
    db.commit()

    com_list = []
    mydb = client["youtube"]
    coll1 = mydb["channel_details"]

    for com_data in coll1.find({}, {"_id": 0, "comment_info":1}):
        for i in range(len(com_data['comment_info'])):
                com_list.append(com_data['comment_info'][i])

    com_df = pd.DataFrame(com_list)

    for index, row in com_df.iterrows():
        insert_query = '''insert into comments(comment_id,
                                                video_id,
                                                comment_text,
                                                comment_author,
                                                comment_published)
                                                
                                                values(%s, %s, %s, %s, %s)'''
        
        values = (row['comment_id'],
                row['video_id'],
                row['comment_text'],
                row['comment_author'],
                datetime.datetime.strptime(row['comment_published'], '%Y-%m-%dT%H:%M:%SZ'))
        try:
                cursor.execute(insert_query, values)
                db.commit()
        except:
                print("comment details already inserted!!!")



# getting all the table informations into a single function

def getTableDetails():
    getChannelDetails()
    getPlaylistDetails()
    getVideoDetails()
    getCommentDetails()

    return "Tables created successfully!!!"



# functions for streamlit application

def viewChannelTable():
    ch_list = []

    mydb = client["youtube"]
    coll1 = mydb["channel_details"]

    for channel_data in coll1.find({}, {"_id": 0, "channel_info":1}):
        ch_list.append(channel_data["channel_info"])

    ch_df = st.dataframe(ch_list)

    return ch_df


def viewPlaylistTable():
    pl_list = []
    mydb = client["youtube"]
    coll1 = mydb["channel_details"]

    for pl_data in coll1.find({}, {"_id": 0, "playlist_info":1}):
            for i in range(len(pl_data['playlist_info'])):
                    pl_list.append(pl_data['playlist_info'][i])

    pl_df = st.dataframe(pl_list)

    return pl_df


def viewVideoTable():
    vi_list = []
    mydb = client["youtube"]
    coll1 = mydb["channel_details"]

    for vi_data in coll1.find({}, {"_id": 0, "video_info":1}):
            for i in range(len(vi_data['video_info'])):
                    vi_list.append(vi_data['video_info'][i])

    vi_df = st.dataframe(vi_list)

    return vi_df


def viewCommentTable():
    com_list = []
    mydb = client["youtube"]
    coll1 = mydb["channel_details"]

    for com_data in coll1.find({}, {"_id": 0, "comment_info":1}):
        for i in range(len(com_data['comment_info'])):
                com_list.append(com_data['comment_info'][i])

    com_df = st.dataframe(com_list)

    return com_df



# connecting to the streamlit application - This will be the basic UI part of the application

with st.sidebar:
    st.title(":blue[YouTube Data Harvesting and Warehousing]")
    st.header("Technology & Skills used")
    st.caption("Python Scripting")
    st.caption("Youtube Data Harvesting")
    st.caption("API Integration")
    st.caption("Data Management using SQL & MongoDB")

channel_id = st.text_input("Enter any youtube channel id: ")

if st.button("collect data and store data in MongoDB"):
    channel_ids = []
    m_db = client["youtube"]
    coll = m_db["channel_details"]

    for ch_data in coll.find({}, {"_id": 0, "channel_info": 1}):
        channel_ids.append(ch_data["channel_info"]["channel_id"])

    if channel_id in channel_ids:
        st.success("You have entered an already existing channel id! please try with a different channel id!!")

    else:
        insert = channelDetails(channel_id)
        st.success(insert)

if st.button("Migrate data from MongoDB to MySQL"):
    table = getTableDetails()
    st.success(table)

get_table = st.radio("Select the table to view", ("Channels", "Playlists", "Videos", "Comments"))

if get_table == "Channels":
    viewChannelTable()

elif get_table == "Playlists":
    viewPlaylistTable()

elif get_table == "Videos":
    viewVideoTable()

else:
    viewCommentTable()



# sql connection and query details for streamlit application

db = mysql.connector.connect(host = "localhost",
                        port = '3306',
                        user = "root",
                        password = "Dhana@2023",
                        database = "youtube_data")

cursor = db.cursor()

queries = st.selectbox("Select your question from the below options", ("1.	What are the names of all the videos and their corresponding channels?",
                                                                       "2.	Which channels have the most number of videos, and how many videos do they have?",
                                                                       "3.	What are the top 10 most viewed videos and their respective channels?",
                                                                       "4.	How many comments were made on each video, and what are their corresponding video names?",
                                                                       "5.	Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                                       "6.	What is the total number of likes for each video, and what are their corresponding video names?",
                                                                       "7.	What is the total number of views for each channel, and what are their corresponding channel names?",
                                                                       "8.	What are the names of all the channels that have published videos in the year 2022?",
                                                                       "9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                                       "10.	Which videos have the highest number of comments, and what are their corresponding channel names?"))


if queries == "1.	What are the names of all the videos and their corresponding channels?":
    query1 = '''select title as videos, channel_name
                from videos'''
    cursor.execute(query1)
    #db.commit()

    q1 = cursor.fetchall()
    df1 = pd.DataFrame(q1, columns = ['Video Title', "Channel Name"])
    st.write(df1)


elif queries == "2.	Which channels have the most number of videos, and how many videos do they have?":
    query2 = '''select channel_name, total_videos
                from channels
                order by total_videos desc'''
    cursor.execute(query2)
    #db.commit()

    q2 = cursor.fetchall()
    df2 = pd.DataFrame(q2, columns = ["Channel Name", "Total Videos"])
    st.write(df2)


elif queries == "3.	What are the top 10 most viewed videos and their respective channels?":
    query3 = '''select views, channel_name, title
                from videos
                where views is not null
                order by views desc limit 10'''
    cursor.execute(query3)
    #db.commit()

    q3 = cursor.fetchall()
    df3 = pd.DataFrame(q3, columns = ["Views", "Channel Name", "Title of the Video"])
    st.write(df3)


elif queries == "4.	How many comments were made on each video, and what are their corresponding video names?":
    query4 = '''select comment_count, title
                from videos
                where comment_count is not null'''
    cursor.execute(query4)
    #db.commit()

    q4 = cursor.fetchall()
    df4 = pd.DataFrame(q4, columns = ["No. of Comments", "Title of the Video"])
    st.write(df4)


elif queries == "5.	Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5 = '''select likes_count, title, channel_name
                from videos
                where likes_count is not null
                order by likes_count desc'''
    cursor.execute(query5)
    #db.commit()

    q5 = cursor.fetchall()
    df5 = pd.DataFrame(q5, columns = ["No. of Likes", "Title of the video", "Channel Name"])
    st.write(df5)


elif queries == "6.	What is the total number of likes for each video, and what are their corresponding video names?":
    query6 = '''select likes_count, title, channel_name
                from videos
                where likes_count is not null'''
    cursor.execute(query6)
    #db.commit()

    q6 = cursor.fetchall()
    df6 = pd.DataFrame(q6, columns = ["No. of Likes", "Title of the video", "Channel Name"])
    st.write(df6)


elif queries == "7.	What is the total number of views for each channel, and what are their corresponding channel names?":
    query7 = '''select channel_views, channel_name
                from channels'''
    cursor.execute(query7)
    #db.commit()

    q7 = cursor.fetchall()
    df7 = pd.DataFrame(q7, columns = ["No. of Views", "Channel Name"])
    st.write(df7)


elif queries == "8.	What are the names of all the channels that have published videos in the year 2022?":
    query8 = '''select title, published_date, channel_name
            from videos
            where extract(year from published_date) = 2022'''
    cursor.execute(query8)
    #db.commit()

    q8 = cursor.fetchall()
    df8 = pd.DataFrame(q8, columns = ["Title of the video", "Published Date", "Channel Name"])
    st.write(df8)


elif queries == "9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9 = '''select channel_name, sec_to_time(avg(time_to_sec(duration))) as average_duration
            from videos group by channel_name'''
    cursor.execute(query9)
    #db.commit()

    q9 = cursor.fetchall()
    df9 = pd.DataFrame(q9, columns = ["Channel_name", "Avg_duration"])
    
    Q9 = []
    for index, row in df9.iterrows():
        channel_name = row['Channel_name']
        avg_duration = str(row['Avg_duration'])
        Q9.append(dict(Channel_Name = channel_name, Average_Duration = avg_duration))
    str_df = pd.DataFrame(Q9)
    st.write(str_df)


elif queries == "10.	Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10 = '''select comment_count, title, channel_name
            from videos
            where comment_count is not null
            order by comment_count desc'''
    cursor.execute(query10)
    #db.commit()

    q10 = cursor.fetchall()
    df10 = pd.DataFrame(q10, columns = ["No. of comments", "Title of the video", "Channel Name"])
    st.write(df10)
