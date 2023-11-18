#Required Packages
from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


#Function for API connection

def Api_Connection():
    Api_id = "AIzaSyDalbbE6DWKVCtTdxbUdfALlCepnRnYwK8"
    ApiServiceName = "YouTube"
    ApiVersion = "v3"
    
    YouTube_data = build(ApiServiceName,ApiVersion,developerKey = Api_id)
    
    return YouTube_data
YouTube =  Api_Connection()

#function to get Channel details

def YouTubeChannel_Id(Channel_id):
    request = YouTube.channels().list(
        part = "contentDetails,snippet,statistics",
        id = Channel_id

        )
    response=request.execute()
    for i in response['items']:
        needed_data=dict(Channel_Name=i['snippet']['title'],
               Channel_Id=i['id'],
               Subscription_Count=i['statistics']['subscriberCount'],
               Channel_Views=i['statistics']['viewCount'],
               Channel_Description=i['snippet']['description'],
               Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'],
               Total_Videos=i['statistics']['videoCount']
               )
    return needed_data


#Function to get videoids

def IdsofVideo(Channel_id):
    VideoIds = []

    response1 = YouTube.channels().list(id = Channel_id,
                                    part = "contentDetails").execute()

    Playlist_Id=response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    nextpagevideos = None

    while True:

        response2 = YouTube.playlistItems().list(part="snippet", 
                                                playlistId = Playlist_Id,
                                                maxResults=50,
                                                pageToken=nextpagevideos)

        Playlist_Details = response2.execute()

        for i in range(len(Playlist_Details['items'])):
            VideoIds.append(Playlist_Details['items'][i]['snippet']['resourceId']['videoId'])
            
        
        nextpagevideos = Playlist_Details.get('nextPageToken')
        
        if nextpagevideos is None:
            break
    return VideoIds


#Function to get Video Details

def Video_Details(VideoIds):
    Details_of_Videos=[]
    for video_id in VideoIds:
        request = YouTube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
            )
        response3=request.execute()

        for i in response3['items']:
            needed_Videodata = dict(Channel_Name=i['snippet']['channelTitle'],
                          Channel_Id=i['snippet']['channelId'],
                          Video_Id=i['id'],
                          Video_Title=i['snippet']['title'],
                          Video_Description = i['snippet']['description'],
                          Tags = i['snippet'].get('tags'),
                          PublishedAt = i['snippet']['publishedAt'],
                          View_Count = i['statistics'].get('viewCount'),
                          Like_Count = i['statistics'].get('likeCount'),
                          Dislike_Count =i['statistics'].get('dislikeCount'),
                          Favorite_Count = i['statistics']['favoriteCount'], 
                          Comment_Count = i['statistics'].get('commentCount'),
                          Duration = i['contentDetails']['duration'],
                          Thumbnail = i['snippet']['thumbnails']['default']['url'],
                          Caption_Status = i['contentDetails']['caption'])
            Details_of_Videos.append(needed_Videodata)
    return Details_of_Videos


#Function to get Comment details

def Comment_Details(VideoIds):
    Comment_details = []
    try:
        for k in VideoIds:
            request = YouTube.commentThreads().list(
                             part = "snippet",
                             videoId = k,
                             maxResults = 50)
            response4=request.execute()

            for i in response4['items']:
                  needed_comment_data = dict(
                         Video_id = i['snippet']['topLevelComment']['snippet']['videoId'],
                         Comment_Id = i['snippet']['topLevelComment']['id'],
                         Comment_Text = i['snippet']['topLevelComment']['snippet']['textDisplay'],
                         Comment_Author = i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                         Comment_PublishedAt = i['snippet']['topLevelComment']['snippet']['publishedAt'])

            Comment_details.append(needed_comment_data)             
    except:
        pass
    return Comment_details


#Function to get Playlist Details

def PlaylistDetails(Channel_id):
    NextPageToken = None
    Playlist_Details = []
    while True:
        request = YouTube.playlists().list(
                        part='snippet,contentDetails',
                        channelId = Channel_id,      
                        maxResults = 50 ,
                        pageToken = NextPageToken
        )

        response5 = request.execute()

        for i in response5['items']:
                  needed_playlist_details = dict (Channel_Name = i['snippet']['channelTitle'],
                               Channel_Id = i['snippet']['channelId'],
                               Playlist_Id = i['id'],
                               Playlist_Name = i['snippet']['title'],
                               VideoCount = i['contentDetails']['itemCount'])
                  Playlist_Details.append(needed_playlist_details) 
        NextPageToken = response5.get('nextPageToken') 
        if NextPageToken is None:
                break     
    return Playlist_Details            


#MongoDB Connection

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["Youtube_Project"]


#Function to store details in MongoDB

def TotalDetails(Channel_id):
    Channel = YouTubeChannel_Id(Channel_id)
    Playlist = PlaylistDetails(Channel_id)
    VideoId = IdsofVideo(Channel_id)
    Videodetails = Video_Details(VideoId)
    Comment = Comment_Details(VideoId)
    
    coll1 = db["ChannelDetails"]
    coll1.insert_one({"Channel_Info":Channel,"Playlist_Info":Playlist,"Video_Info":Videodetails,"Comment_Info":Comment})
    
    return "uploaded successfully in MongoDB"


#For Channel Table in SQL

def channeltable():
    sqldb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="kavin",
                            database="YouTube_Data",
                            port="5432")
    cursor=sqldb.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    sqldb.commit()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(255),
                                                            Channel_Id varchar(255) primary key,
                                                            Subscription_Count bigint,
                                                            Channel_Views bigint,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(255),
                                                            Total_Videos bigint)'''
        cursor.execute(create_query)
        sqldb.commit()
    except:
        print("channel table created")


    channel_list = []
    db = client['Youtube_Project']
    coll1 = db["ChannelDetails"]
    for i in coll1.find({},{"_id":0,"Channel_Info":1}):
        channel_list.append(i["Channel_Info"])
    DF_channel_list = pd.DataFrame(channel_list)

    for index,row in DF_channel_list.iterrows():
        insert_query = '''insert into channels(Channel_Name, 
                                            Channel_Id, 
                                            Subscription_Count, 
                                            Channel_Views,
                                            Channel_Description,
                                            Playlist_Id,
                                            Total_Videos)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Channel_Views'],
                row['Channel_Description'],
                row['Playlist_Id'],
                row['Total_Videos'])

        try:
            cursor.execute(insert_query,values)
            sqldb.commit()

        except:
            print("channel details inserted already")                 
    


#For Playlist Table in SQL 

def playlisttable():
    sqldb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="kavin",
                            database="YouTube_Data",
                            port="5432")
    cursor=sqldb.cursor()

    drop_query = '''drop table if exists playlist'''
    cursor.execute(drop_query)
    sqldb.commit()

    try:
        create_query='''create table if not exists playlist(Channel_Name varchar(255),
                                                            Channel_Id varchar(255),
                                                            Playlist_Id varchar(255)primary key,
                                                            Playlist_Name varchar(255),
                                                            VideoCount int)'''
                                                            
                                                            
        cursor.execute(create_query)
        sqldb.commit()


    except:
        print("playlist table created")



    playlist_list = []
    db = client['Youtube_Project']
    coll1 = db["ChannelDetails"]
    for pl in coll1.find({},{"_id":0,"Playlist_Info":1}):
        for i in range(len(pl["Playlist_Info"])):
            playlist_list.append(pl["Playlist_Info"][i])
    DF_playlist_list = pd.DataFrame(playlist_list)

    for index,row in DF_playlist_list.iterrows():
            insert_query = '''insert into playlist(Channel_Name,
                                                Channel_Id,
                                                Playlist_Id,
                                                Playlist_Name,
                                                VideoCount)
                                            
                                                values(%s,%s,%s,%s,%s)'''
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Playlist_Id'],
                    row['Playlist_Name'],
                    row['VideoCount'])

            
            cursor.execute(insert_query,values)
            sqldb.commit()
              


#For Video Table in SQL

def videotable():
    sqldb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="kavin",
                            database="YouTube_Data",
                            port="5432")
    cursor=sqldb.cursor()

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    sqldb.commit()
    
    try:
        create_query='''create table if not exists videos(Channel_Name varchar(255),
                                                          Channel_Id varchar(255),
                                                          Video_Id varchar(255),
                                                          Video_Title varchar(255),
                                                          Video_Description text,
                                                          Tags text,
                                                          PublishedAt timestamp,
                                                          View_Count bigint,
                                                          Like_Count bigint,
                                                          Dislike_Count bigint,
                                                          Favorite_Count bigint, 
                                                          Comment_Count bigint,
                                                          Duration interval,
                                                          Thumbnail varchar(255),
                                                          Caption_Status varchar(255))'''



        cursor.execute(create_query)
        sqldb.commit()
      
    except:
        print("video table created")  
    

    


    videodetails_list = []
    db = client['Youtube_Project']
    coll1 = db["ChannelDetails"]    
    for vid in coll1.find({},{"_id":0,"Video_Info":1}):
        for i in range(len(vid["Video_Info"])):
            videodetails_list.append(vid["Video_Info"][i])
    DF_videodetails_list = pd.DataFrame(videodetails_list)

    for index,row in DF_videodetails_list.iterrows():
            insert_query = '''insert into videos(Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    Video_Title,
                                                    Video_Description,
                                                    Tags,
                                                    PublishedAt,
                                                    View_Count,
                                                    Like_Count,
                                                    Dislike_Count,
                                                    Favorite_Count, 
                                                    Comment_Count,
                                                    Duration,
                                                    Thumbnail,
                                                    Caption_Status)

                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Video_Title'],
                    row['Video_Description'],
                    row['Tags'],
                    row['PublishedAt'],
                    row['View_Count'],
                    row['Like_Count'],
                    row['Dislike_Count'],
                    row['Favorite_Count'],
                    row['Comment_Count'],
                    row['Duration'],
                    row['Thumbnail'],
                    row['Caption_Status'])


            cursor.execute(insert_query,values)
            sqldb.commit()



#For Comment Table in SQL

def commenttable():
    sqldb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="kavin",
                            database="YouTube_Data",
                            port="5432")
    cursor=sqldb.cursor()

    drop_query = '''drop table if exists comment'''
    cursor.execute(drop_query)
    sqldb.commit()
    
    try:
        create_query='''create table if not exists comment(Video_id varchar(255),
                                                         Comment_Id varchar(255),
                                                         Comment_Text text,
                                                         Comment_Author varchar(255),
                                                         Comment_PublishedAt timestamp)'''
                                                            
                                                            
        cursor.execute(create_query)
        sqldb.commit()


    except:
        print("comment table created")
        
        
    comment_list = []
    db = client['Youtube_Project']
    coll1 = db["ChannelDetails"]
    for com in coll1.find({},{"_id":0,"Comment_Info":1}):
        for i in range(len(com["Comment_Info"])):
            comment_list.append(com["Comment_Info"][i])
    DF_comment_list = pd.DataFrame(comment_list)

    for index,row in DF_comment_list.iterrows():
            insert_query = '''insert into comment(Video_id,
                                                 Comment_Id,
                                                 Comment_Text,
                                                 Comment_Author,
                                                 Comment_PublishedAt)
                                            
                                                values(%s,%s,%s,%s,%s)'''
            
            values=(row['Video_id'],
                    row['Comment_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_PublishedAt'])

            
            cursor.execute(insert_query,values)
            sqldb.commit()  



#Function for all Tables

def sqltables():
    channeltable()
    playlisttable()
    videotable()
    commenttable()

    return "sqltables created"

SQLTables=sqltables()


#st channel

def viewchanneltable():
    channel_list = []
    db = client['Youtube_Project']
    coll1 = db["ChannelDetails"]
    for i in coll1.find({},{"_id":0,"Channel_Info":1}):
        channel_list.append(i["Channel_Info"])
    df_channel_list=st.dataframe(channel_list)
    return df_channel_list
       

#st playlist

def viewplaylisttable():       
    playlist_list = []
    db = client['Youtube_Project']
    coll1 = db["ChannelDetails"]
    for pl in coll1.find({},{"_id":0,"Playlist_Info":1}):
            for i in range(len(pl["Playlist_Info"])):
                playlist_list.append(pl["Playlist_Info"][i])
    df_playlist = st.dataframe(playlist_list)
    return df_playlist


#st video

def viewvideotable():
    videodetails_list = []
    db = client['Youtube_Project']
    coll1 = db["ChannelDetails"]    
    for vid in coll1.find({},{"_id":0,"Video_Info":1}):
        for i in range(len(vid["Video_Info"])):
            videodetails_list.append(vid["Video_Info"][i])
    df_videodetails = st.dataframe(videodetails_list)
    return df_videodetails


#st Comment

def viewcommenttable():
    comment_list = []
    db = client['Youtube_Project']
    coll1 = db["ChannelDetails"]
    for com in coll1.find({},{"_id":0,"Comment_Info":1}):
        for i in range(len(com["Comment_Info"])):
            comment_list.append(com["Comment_Info"][i])
    df_comment = st.dataframe(comment_list)
    return df_comment


#Streamlit code


st.title(":red[YouTube Data Harvesting and Warehousing ]")

with st.sidebar:
     st.title(":red[Project Module]")
     st.header(":blue[KEY FRAMEWORK]")
     st.caption('Python ScriptWork')
     st.caption('Data Extraction by API')
     st.caption('DB in MongoDB')
     st.caption('DBM using SQL')
     st.caption('Streamlit Show')


Channel_ID = st.text_input("Enter the Channel ID")

if st.button("Get and Store in DataBase"):
    channel_ids=[]
    db = client['Youtube_Project']
    coll1 = db["ChannelDetails"]
    for i in coll1.find({},{"_id":0,"Channel_Info":1}):
        channel_ids.append(i["Channel_Info"]["Channel_Id"])

    if Channel_ID in channel_ids:
        st.success("Channel Data already exists")
    else:
        insert=TotalDetails(Channel_ID)
        st.success(insert) 

if st.button("Fetch in SQL"):
    SQLTables=sqltables()
    st.success(SQLTables) 

show_table=st.radio("SELECT TO VIEW A TABLE",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    viewchanneltable()

elif show_table=="PLAYLISTS":
    viewplaylisttable()

elif show_table=="VIDEOS":
    viewvideotable() 
    
elif show_table=="COMMENTS":
    viewcommenttable()



#10 Queries

sqldb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="kavin",
                        database="YouTube_Data",
                        port="5432")
cursor=sqldb.cursor()

question=st.selectbox("Select Your Question",("1.Names of all the videos and their corresponding channels?",
                                               "2.Channels have the most number of videos, and how many videos do they have?",
                                               "3.Ten most viewed videos and their respective channels?",
                                               "4.Number of comments on each video and their corresponding video names?",
                                               "5.Videos having highest number of likes and their corresponding channel names?",
                                               "6.Total number of likes and favourite count for each video and their corresponding video names?",
                                               "7.Total number of views for each channel and their corresponding channel names?",
                                               "8.Names of all the channels that have published videos in the year 2022?",
                                               "9.Average duration of all videos in each channel their corresponding channel names?",
                                               "10.Videos have the highest number of comments and their corresponding channel names?"))


if question=="1.Names of all the videos and their corresponding channels?":
    query1='''select video_title as videoname,channel_name as channelname from videos'''
    cursor.execute(query1)
    sqldb.commit()
    a1=cursor.fetchall()
    df1=pd.DataFrame(a1,columns=["Video Name","Channel Name"])
    st.write(df1)


elif question=="2.Channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_name as channelname,total_videos as no_of_videos from channels
                    order by total_videos desc'''
    cursor.execute(query2)
    sqldb.commit()
    a2=cursor.fetchall()
    df2=pd.DataFrame(a2,columns=["Channel Name","No of Videos"])
    st.write(df2)


elif question=="3.Ten most viewed videos and their respective channels?":
    query3='''select channel_name as channelname,video_title as videoname,view_count as totalviews from videos
                    where view_count is not null order by view_count desc limit 10'''
    cursor.execute(query3)
    sqldb.commit()
    a3=cursor.fetchall()
    df3=pd.DataFrame(a3,columns=["Channel Name","Video Name","No of Views"])
    st.write(df3)   


elif question=="4.Number of comments on each video and their corresponding video names?":
    query4='''select channel_name as channelname,video_title as videoname,comment_count as no_of_comments from videos
                    where comment_count is not null'''
    cursor.execute(query4)
    sqldb.commit()
    a4=cursor.fetchall()
    df4=pd.DataFrame(a4,columns=["Channel Name","Video Name","No of Comments"])
    st.write(df4) 


elif question=="5.Videos having highest number of likes and their corresponding channel names?":
    query5='''select channel_name as channelname,video_title as videoname,like_count as likecounts from videos
                    where like_count is not null order by like_count desc'''
    cursor.execute(query5)
    sqldb.commit()
    a5=cursor.fetchall()
    df5=pd.DataFrame(a5,columns=["Channel Name","Video Name","No of Likes"])
    st.write(df5)


elif question=="6.Total number of likes and favourite count for each video and their corresponding video names?":
    query6='''select video_title as videoname,like_count as likecounts,favorite_count as favoritecount from videos
                    where like_count is not null'''
    cursor.execute(query6)
    sqldb.commit()
    a6=cursor.fetchall()
    df6=pd.DataFrame(a6,columns=["Video Name","No of Likes","No of FavouriteCount"])
    st.write(df6)


elif question=="7.Total number of views for each channel and their corresponding channel names?":
    query7='''select channel_name as channelname,channel_views as totalviews from channels
                    where channel_views is not null'''
    cursor.execute(query7)
    sqldb.commit()
    a7=cursor.fetchall()
    df7=pd.DataFrame(a7,columns=["Channel Name","Total Views"])
    st.write(df7)


elif question=="8.Names of all the channels that have published videos in the year 2022?":
    query8='''select channel_name as channelname,video_title as videoname,publishedat as publisheddate from videos 
                    where extract(year from publishedat)=2022'''
    cursor.execute(query8)
    sqldb.commit()
    a8=cursor.fetchall()
    df8=pd.DataFrame(a8,columns=["Channel Name","Video Name","Date of Publication"])
    st.write(df8) 


elif question=="9.Average duration of all videos in each channel their corresponding channel names?":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    sqldb.commit()
    a9=cursor.fetchall()
    df9=pd.DataFrame(a9,columns=["Channel Name","Average Duration of Videos"])
        
    t9=[]
    for index,row in df9.iterrows():
       channel_title=row["Channel Name"]
       avg_duration=row["Average Duration of Videos"]
       avg_duration_str=str(avg_duration)
       t9.append(dict(channel_title=channel_title,avg_duration=avg_duration_str))
    df_avg=pd.DataFrame(t9)
    st.write(df_avg)


elif question=="10.Videos have the highest number of comments and their corresponding channel names?":
    query10='''select channel_name as channelname,video_title as videoname,comment_count as commentcount from videos 
                    where comment_count is not null order by comment_count desc'''
    cursor.execute(query10)
    sqldb.commit()
    a10=cursor.fetchall()
    df10=pd.DataFrame(a10,columns=["Channel Name","Video Name","No of Comments"])
    st.write(df10)



    
            
       