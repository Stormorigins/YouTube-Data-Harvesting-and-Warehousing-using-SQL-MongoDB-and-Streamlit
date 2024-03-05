                                                               #import Module

import googleapiclient.discovery
import pymongo
import pandas as pd
import pymysql
import streamlit as st

                                                               #API Connection

api_key = "AIzaSyCBkthBQKDj1yS91deimYwNp_6fHtAcWKA"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey= api_key)

                                                               #Channel_info

def channel_id(ID):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=ID
            )
    response = request.execute()    
    for i in response["items"]:
                    data=dict(channel_Name=i["snippet"]['title'],channel_ID=i["id"],subs_Count=i["statistics"]["subscriberCount"],Views_Count=i["statistics"]["viewCount"],Total_Video=i["statistics"]["videoCount"],channel_desc=i["snippet"]['description'],Upload_ID=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

                                                               #VideosID_info

def videos1(enterid):
    video_ID=[]
    request2 = youtube.channels().list(
            part="contentDetails",
            id=(enterid)
            )
    reponse=request2.execute()
    playlist_ID= reponse["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    number_of_result=None
    while True:
        request3 = youtube.playlistItems().list(part="snippet",playlistId=playlist_ID,maxResults=50,pageToken=number_of_result).execute()
        for i in range(len(request3["items"])):
            video_ID.append(request3["items"][i]["snippet"]["resourceId"]["videoId"])
        number_of_result=request3.get("nextPageToken")

        if number_of_result is None:
            break
    return video_ID

                                                               #Video_info

def getvideodata(video_data):
    videodata=[]
    for VID in video_data:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=VID
        )
        response = request.execute()
        for i in response["items"]:
            data= dict(Channel_Name =i['snippet']['channelTitle'], Channel_Id = i['snippet']['channelId'],Video_Id = i['id'],
                            Title = i['snippet']['title'],Thumbnail = i['snippet']['thumbnails']['default']['url'],
                            Description = i['snippet']['description'],Published_Date = i['snippet']['publishedAt'],Duration = i['contentDetails']['duration'],
                            Views = i['statistics'].get('viewCount'), Likes = i['statistics'].get('likeCount'),Comments = i['statistics'].get('commentCount'),
                            Favorite_Count = i['statistics']['favoriteCount'],Definition = i['contentDetails']['definition'],Caption_Status= i['contentDetails']['caption'])
            videodata.append(data)
    return videodata


                                                               #Comment_info

def comment(x):
    videodata1=[]
    try:
        for VID in x:
            request= youtube.commentThreads().list(part="snippet",
                    videoId=VID,
                    maxResults=50
                )
            response =request.execute()

            for i in response["items"]:
                data= {"comment_ID":i["snippet"]["topLevelComment"]["id"],"VideoID":i["snippet"]["videoId"],"comment_txt":i["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                      "comment_author":i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],"comment_published":i["snippet"]["topLevelComment"]["snippet"]["publishedAt"]}
                videodata1.append(data)
    
    except:
        pass
       
    return videodata1


                                                               #Playlist_info

def play(x):
    playlist=[]
    nextpage=None
    while True:
        request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=x,
                maxResults=100,pageToken=nextpage
            )
        response = request.execute()
        for i in response["items"]:
            data= dict(Playlist_ID =i["id"],Playlist_Title=i["snippet"]["title"],Channel_ID=i["snippet"]["channelId"],Channel_name=i["snippet"]["channelTitle"],
                  Published=i['snippet']['publishedAt'],Video_count=i["contentDetails"]["itemCount"])
            playlist.append(data)
        nextpage=response.get("nextPageToken")
        
        if nextpage is None:
            break
    return playlist


                                                                   #MONGODP 

connection=pymongo.MongoClient("mongodb://localhost:27017")
db=connection["Youtube"]

                                                               #Channel Details

def channel_data(copy_ID):
    Channel_info=channel_id(copy_ID)
    Playlist_data=play(copy_ID)
    VideoIDs=videos1(copy_ID)
    LV_Count=getvideodata(VideoIDs)
    Comment_data=comment(VideoIDs)
    
    col=db["Harvest"]
    col.insert_one({"Channel_info":Channel_info,"Playlist_details":Playlist_data, "Video_IDs":VideoIDs,"Likes_Views":LV_Count,
                   "Comments": Comment_data})
    return "Upload done"


                                                                  #MYSQL WORKBENCH

                                                 #creating table and insert data for collection channels
def tables():
    myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='root',database = "Youtube")
    cur = myconnection.cursor()
    
    drop_table='''drop table if exists Channels'''
    cur.execute(drop_table)
    myconnection.commit()
    
    try:
        Create_Query='''create table Channels(channel_Name char(100),
                                            channel_ID char(100) primary key,
                                            subs_Count bigint, Views_Count bigint, 
                                            Total_Video bigint, 
                                            channel_desc text, 
                                            Upload_ID char(100))'''
        cur.execute(Create_Query)
        myconnection.commit()
    except:
        print("already exists")
        
    List=[]
    db=connection["Youtube"]
    col=db["Harvest"]
    for i in col.find({},{"_id":0,"Channel_info":1}):
        List.append(i["Channel_info"])
        Data= pd.DataFrame(List)

        for index,row in Data.iterrows():
            DataQuery='''insert into Channels(channel_Name,channel_ID,subs_Count,Views_Count,Total_Video,channel_desc,Upload_ID)
                        values(%s,%s,%s,%s,%s,%s,%s)'''

            values=(row['channel_Name'], row['channel_ID'],row['subs_Count'],row['Views_Count'],row['Total_Video'],row['channel_desc'],row['Upload_ID'])

        try:
            cur.execute(DataQuery,values)
            myconnection.commit()
        except:
            print("done")

                                                 #creating table and insert data for collection playlists


def playlist():
    myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='root',database = "Youtube",port=3306)
    cur = myconnection.cursor()

    drop_table='''drop table if exists playlists'''
    cur.execute(drop_table)
    myconnection.commit()

    try:
        Create_playlistQuery='''create table playlists(Playlist_ID char(100) primary key,
                                            Playlist_Title char(100),
                                            Channel_ID char(100), Channel_name char(100), 
                                            Published char(100),
                                            Video_count bigint)'''
        cur.execute(Create_playlistQuery)
        myconnection.commit()
    except:
        print("already exists")

    playList=[]
    db=connection["Youtube"]
    col=db["Harvest"]
    for Playid in col.find({},{"_id":0,"Playlist_details":1}):
        for i in range(len(Playid["Playlist_details"])):
            playList.append(Playid["Playlist_details"][i])
            Data2= pd.DataFrame(playList)

            for index,row in Data2.iterrows():
                DataQuery='''insert into playlists(Playlist_ID,Playlist_Title,Channel_ID,Channel_name,Published,Video_count)
                            values(%s,%s,%s,%s,%s,%s)'''

                values=(row['Playlist_ID'], row['Playlist_Title'],row['Channel_ID'],row['Channel_name'],row['Published'],row['Video_count'])


            cur.execute(DataQuery,values)
            myconnection.commit()  


                                                 #creating table and insert data for collection videos


def videos():
    myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='root',database = "Youtube",port=3306)
    cur = myconnection.cursor()

    drop_video='''drop table if exists videos'''
    cur.execute(drop_video)
    myconnection.commit()


    Create_videoQuery='''create table videos(Channel_Name char(255),Channel_Id char(255),Video_Id char(50) primary key,Title char(255),
                         Thumbnail char(255),Description text,Published_Date char(255), Duration char(100),
                           Views bigint,Likes bigint,Comments bigint, Favorite_Count bigint, Definition char(255), Caption_Status char(255))'''
    cur.execute(Create_videoQuery)
    myconnection.commit()

    video=[]
    db=connection["Youtube"]
    col=db["Harvest"]
    for VID in col.find({},{"_id":0,"Likes_Views":1}):
        for i in range(len(VID["Likes_Views"])):
            video.append(VID["Likes_Views"][i])
    Data3= pd.DataFrame(video)

    for index,row in Data3.iterrows():
        DataQuery='''insert into videos(Channel_Name, Channel_Id, Video_Id, Title, Thumbnail, Description, Published_Date,
                    Duration, Views, Likes, Comments, Favorite_Count, Definition, Caption_Status)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['Channel_Name'],row["Channel_Id"],row["Video_Id"],row["Title"],row["Thumbnail"],row["Description"],
                row["Published_Date"],row["Duration"],row["Views"],row["Likes"],row["Comments"],row["Favorite_Count"],row["Definition"],row["Caption_Status"])

        cur.execute(DataQuery,values)
        myconnection.commit()


                                                 #creating table and insert data for collection Comments


def comments():    
    myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='root',database = "Youtube",port=3306)
    cur = myconnection.cursor()

    drop_comments='''drop table if exists Comments'''
    cur.execute(drop_comments)
    myconnection.commit()


    Create_commentQuery='''create table Comments(comment_ID char(100) primary key,VideoID char(100), comment_txt text, comment_author char(150), comment_published char(150))'''
    cur.execute(Create_commentQuery)
    myconnection.commit()

    comment=[]
    db=connection["Youtube"]
    col=db["Harvest"]
    for cmt in col.find({},{"_id":0,"Comments":1}):
        for i in range(len(cmt["Comments"])):
            comment.append(cmt["Comments"][i])
    Data4= pd.DataFrame(comment)

    for index,row in Data4.iterrows():
        DataQuery='''insert into Comments(comment_ID,VideoID,comment_txt,comment_author,comment_published)
                    values(%s,%s,%s,%s,%s)'''

        values=(row['comment_ID'], row['VideoID'],row['comment_txt'],row['comment_author'],row['comment_published'])
        cur.execute(DataQuery,values)
        myconnection.commit()  


                                                              #TABLE COLECTIONS

def Youtube_data():
    tables()
    playlist()
    videos()
    comments()

    return "Tables created succcessfully"

                                                              #STREAMLIT

                                                              #RADIO FUNCTIONS
def show_channel():
    List=[]
    db=connection["Youtube"]
    col=db["Harvest"]
    for i in col.find({},{"_id":0,"Channel_info":1}):
        List.append(i["Channel_info"])
    Data= st.dataframe(List)
    return Data


def show_playlist():
    playList=[]
    db=connection["Youtube"]
    col=db["Harvest"]
    for Playid in col.find({},{"_id":0,"Playlist_details":1}):
        for i in range(len(Playid["Playlist_details"])):
            playList.append(Playid["Playlist_details"][i])
    Data2= st.dataframe(playList)
    return Data2


def show_video():
    video=[]
    db=connection["Youtube"]
    col=db["Harvest"]
    for VID in col.find({},{"_id":0,"Likes_Views":1}):
        for i in range(len(VID["Likes_Views"])):
            video.append(VID["Likes_Views"][i])
    Data3= st.dataframe(video)
    return Data3


def show_comment():
    comment=[]
    db=connection["Youtube"]
    col=db["Harvest"]
    for cmt in col.find({},{"_id":0,"Comments":1}):
        for i in range(len(cmt["Comments"])):
            comment.append(cmt["Comments"][i])
    Data4= st.dataframe(comment)
    return Data4

                                                              #Side Bar Creation
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL TAKE AWAY")
    st.caption('API Integration')
    st.caption("PYTHON SCRIPTING")
    st.caption("MONGODP")
    st.caption("MYSQL WORKBENCH")
    st.caption(" DATA MANAGEMENT USING MONGODP AND MYSQL WORKBENCH")
    
                                                               #USER INPUT                                                           
channel_ID = st.text_input("Enter channel ID:")

                                                               #Button 1

if st.button("Mongodp data"):
    cha_IDs=[]
    db=connection["Youtube"]
    col=db["Harvest"]
    for i in col.find({},{"_id":0,"Channel_info":1}):
        cha_IDs.append(i["Channel_info"]["channel_ID"])


    if channel_ID in cha_IDs:
        st.success("Channel ID already Existes")
    else:
        output=channel_data(channel_ID)
        st.success(output)

                                                               #Button 2

if st.button("Insert into MYSQL"):
    Tables=Youtube_data()
    st.success(Tables)

                                                               #RADIO Buttons

RADIO=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

if RADIO == "CHANNELS":
    show_channel()

elif RADIO == "PLAYLISTS":
    show_playlist()

elif RADIO == "VIDEOS":
    show_video()
    
elif RADIO == "COMMENTS":
    show_comment()


myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='root',database = "Youtube",port=3306)
cur = myconnection.cursor()

                                                               #QUESTIONS

Questions = st.selectbox('Please Select Your Question',
    ('1. What are the names of all the videos and their corresponding channels?',
     '2. Which channels have the most number of videos, and how many videos do they have?',
     '3. What are the top 10 most viewed videos and their respective channels',
     '4. How many comments were made on each video, and what are their corresponding video names?',
     '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
     '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
     '7. What is the total number of views for each channel, and what are their corresponding channel names?',
     '8. What are the names of all the channels that have published videos in the year 2022?',
     '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
     '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))

                                                               #QUESTION1

if Questions =='1. What are the names of all the videos and their corresponding channels?':
    Query1 = '''select Channel_Name as Channelname, Title as Videos  from videos'''
    cur.execute(Query1)
    myconnection.commit()
    F1=cur.fetchall()
    DF1 = pd.DataFrame(F1,columns = ["Channels", "Videos"])
    st.write(DF1)
                                                               #QUESTION2

elif Questions =='2. Which channels have the most number of videos, and how many videos do they have?':
        Query2 = '''select channel_Name, Total_Video from channels order by Total_Video desc'''
        cur.execute(Query2)
        myconnection.commit()
        F2=cur.fetchall()
        DF2 = pd.DataFrame(F2,columns = ["Channel_Name", "Most#ofvideos"])
        st.write(DF2)
                                                               #QUESTION3

elif Questions =='3. What are the top 10 most viewed videos and their respective channels':
        Query3 = '''select Channel_Name, Title,views from videos order by views desc'''
        cur.execute(Query3)
        myconnection.commit()
        F3=cur.fetchall()
        DF3 = pd.DataFrame(F3,columns = ["Channel_Name", "Top_10_ most_viewed_videos", "Views"])
        st.write(DF3.head(10))
                                                               #QUESTION4

elif Questions =='4. How many comments were made on each video, and what are their corresponding video names?':
        Query4 = '''select Title, Comments from videos'''
        cur.execute(Query4)
        myconnection.commit()
        F4=cur.fetchall()
        DF4 = pd.DataFrame(F4,columns = ["Videos", "Comments"])
        st.write(DF4)
                                                               #QUESTION5

elif Questions =='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        Query5 = '''select Channel_Name, Title, likes from videos order by likes desc'''
        cur.execute(Query5)
        myconnection.commit()
        F5=cur.fetchall()
        DF5 = pd.DataFrame(F5,columns = ["Channel_Name", "Title", "Most#of likes"])
        st.write(DF5)
                                                               #QUESTION6

elif Questions =='6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        Query6 = '''select Title, likes from videos'''
        cur.execute(Query6)
        myconnection.commit()
        F6=cur.fetchall()
        DF6 = pd.DataFrame(F6,columns = ["Videos", "Total#oflikes"])
        st.write(DF6)
                                                               #QUESTION7

elif Questions =='7. What is the total number of views for each channel, and what are their corresponding channel names?':
        Query7 = '''select channel_Name, Views_Count from channels'''
        cur.execute(Query7)
        myconnection.commit()
        F7=cur.fetchall()
        DF7 = pd.DataFrame(F7,columns = ["Channel_Name", "Total#ofviews"])
        st.write(DF7)
                                                               #QUESTION8

elif Questions =='8. What are the names of all the channels that have published videos in the year 2022?':
        Query8 = '''select Channel_Name, Title, Published_Date from videos where Published_Date like "2022%"'''
        cur.execute(Query8)
        myconnection.commit()
        F8=cur.fetchall()
        DF8 = pd.DataFrame(F8,columns = ["Channel_Name", "Videos", "Published_on"])
        st.write(DF8)
                                                               #QUESTION9

elif Questions =='9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        Query9 = '''select Channel_Name, Title, Duration from videos group by Channel_Name'''
        cur.execute(Query9)
        myconnection.commit()
        F9=cur.fetchall()
        DF9 = pd.DataFrame(F9,columns = ["Channel_Name", "Videos", "Average_Duration"])
        st.write(DF9)
                                                               #QUESTION10
                                                               
elif Questions =='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        Query10 = '''select Channel_Name, Title, Comments from videos order by Comments desc'''
        cur.execute(Query10)
        myconnection.commit()
        F10=cur.fetchall()
        DF10 = pd.DataFrame(F10,columns = ["Channel_Name", "Videos", "Comments"])
        st.write(DF10)