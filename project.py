import streamlit as st
import pymongo
import mysql.connector
import pandas as pd
import googleapiclient.discovery
from pymongo.mongo_client import MongoClient
from flatten_json import flatten
from streamlit_option_menu import option_menu
from PIL import Image
import plotly.express as px

#push_to_mysql = False
#global push_to_mysql

#api_key ="AIzaSyDI3TKadEMd2ewZd2J8wUQisbMPVc8iJJI"  #OCT 07 2023 Never used but still getting quto exceeeded
    #api_key = "AIzaSyDxDq4OHLz7vqqM3-LfW3Ilzanq-fKlHMc" # Oct 07 2023
    #api_key = "AIzaSyA3jJEAPiSKuocsZTdqtEWQjXCXc7sBfvA" #jazzoria_id 
api_key = "AIzaSyAju57hvhz5T42vtpwUoVkpFuwbVlzRhyM" #mr.gsanthosh
    #AIzaSyAju57hvhz5T42vtpwUoVkpFuwbVlzRhyM
    #api_key = "AIzaSyBlugsdjEdym36NqTI2dzlEntwKoJFbuYc"
youtube = googleapiclient.discovery.build("youtube","v3", developerKey = api_key)

# Define MongoDB and MySQL connection details
client = MongoClient( "mongodb+srv://jazzoria:root@cluster0.4eyuj8w.mongodb.net/?retryWrites=true&w=majority") #.dtt.youtube  #creating file for the youtube
db = client.dtt
youtube_collection = db.youtube

#MySql Connection
mysql_connect = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="barath",
)
mycursor = mysql_connect.cursor()

# Create a Streamlit app title
st.title("YouTube Data Retrieval")

# Add a text input box for channel name
channel_name = st.text_input("Enter Channel Name")

# Create a button to retrieve and push data
push_button = st.button("Get Details and Collect Data")
#enable_push_to_mysql = False
#push_to_mysql = st.button("MySQL")

questions =  ["1. What are the names of all the videos and their corresponding channels?",
    "2. Which channels have the most number of videos, and how many videos do they have?",
    "3. What are the top 10 most viewed videos and their respective channels?",
    "4. How many comments were made on each video, and what are their corresponding video names?",
    "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
    "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "7. What is the total number of views for each channel, and what are their corresponding channel names?",
    "8. What are the names of all the channels that have published videos in the year 2022?",
    "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "10. Which videos have the highest number of comments, and what are their corresponding channel names?"]
    
selected_question = st.selectbox("Select a Question", questions)

def sql_connect(sqlData) : #mysql_connect,
# Perform a MongoDB query and get a cursor
  result = youtube_collection.find(sqlData)
  doc_push = None
  for doc in result:
     doc_push = doc
   # doc_result = doc
   # doc_push.append(doc_result)
# Establish a MySQL connection
  mycursor = mysql_connect.cursor(buffered=True)
  print("chn after mycursor")
  get_channel_det = doc_push["channel_id"] #doc_result
  #for channel in get_channel_det:
  channel_id = get_channel_det['channel_id']
  channel_name = get_channel_det['channel_name']
  channel_views = get_channel_det['channel_views']  # Convert to int if it's a string
  channel_description = get_channel_det['channel_des']
  channel_status = get_channel_det['channel_status']
  print("chn after chn details")
  mycursor.execute("SHOW TABLES LIKE 'channel12'") # LIKE channel mycursor.execute("SHOW TABLES LIKE 'channel12'")
  print("chn before try")
  table_exist = mycursor.fetchmany()
  try :
        if not table_exist:
           print("inside if chn")
           #create_channel = "create table if not exists channel12(channel_id varchar(255),channel_name varchar(255),channel_views int,channel_description text,channel_status varchar(255),primary key (channel_id))"
           create_channel = "CREATE TABLE IF NOT EXISTS channel12 (channel_id VARCHAR(255), channel_name VARCHAR(255), channel_views INT, channel_description TEXT,channel_status VARCHAR(255),PRIMARY KEY (channel_id))"
           mycursor.execute(create_channel)
           mycursor.execute(
                "INSERT INTO channel12 (channel_id, channel_name, channel_views, channel_description, channel_status)"
                "VALUES (%s, %s, %s, %s, %s)",
                (channel_id, channel_name, channel_views, channel_description, channel_status)
            )
           print("If :Data saved to CHN TAB")  
        else :
            print("inside else chn")
            mycursor.execute(
                "INSERT INTO channel12 (channel_id, channel_name, channel_views, channel_description, channel_status)"
                "VALUES (%s, %s, %s, %s, %s)",
                (channel_id, channel_name, channel_views, channel_description, channel_status)
            )
            print("else :Data saved to CHN TAB")  
  except Exception as e:
       print("Inside Error")
       print(e)
  print("Data saved to CHN TAB")  
  mysql_connect.commit() 
  mycursor.close()   
   
  # Extract playlist information
 
  # Iterate over playlists
  get_playlist_det = doc_push["playlist_id"] #doc_result
#  print(get_playlist_det)
  print("Inside the Playlist")
  channel_id = get_playlist_det['channel_id']
  playlist_id = get_playlist_det['playlist_id']
  playlist_names = get_playlist_det['playlist_name']
  #mycursor.execute("CREATE TABLE IF NOT EXISTS playlist12 (playlist_id VARCHAR(255),channel_id VARCHAR(255), playlist_name Text , PRIMARY KEY(playlist_id),FOREIGN KEY(channel_id) REFERENCES channel12(channel_id))")
 # print(channel_id)
 # print(playlist_id)
 # print(playlist_names)
  mycursor = mysql_connect.cursor(buffered=True)
  mycursor.execute("SHOW TABLES LIKE 'playlist12'") # LIKE channel mycursor.execute("SHOW TABLES LIKE 'channel12'")
  print("plytb before try")
  table_exist = mycursor.fetchmany()
  try :
    if not table_exist:
      mycursor.execute("CREATE TABLE IF NOT EXISTS playlist12 (playlist_id VARCHAR(255),channel_id VARCHAR(255), playlist_name Text , FOREIGN KEY(channel_id) REFERENCES channel12(channel_id))")
      #UNIQUE KEY (playlist_id, channel_id)
      #mycursor.execute("CREATE TABLE IF NOT EXISTS playlist12 (playlist_id VARCHAR(255),channel_id VARCHAR(255), playlist_name Text , UNIQUE KEY(playlist_id,channel_id),FOREIGN KEY(channel_id) REFERENCES channel12(channel_id))")
    # Iterate through the list of playlist names and insert each one
      for i in range(len(playlist_names)):
     #print(playlist_names[i])
     #print(channel_id,playlist_id,playlist_names[i])
          mycursor.execute(
            "INSERT INTO playlist12 (playlist_id, channel_id, playlist_name) "
            "VALUES (%s, %s, %s)",
            (playlist_id, channel_id, playlist_names[i])
          )
    else :
      print("inside else playlist")
      for i in range(len(playlist_names)):
     #print(playlist_names[i])
     #print(channel_id,playlist_id,playlist_names[i])
          mycursor.execute(
            "INSERT INTO playlist12 (playlist_id, channel_id, playlist_name) "
            "VALUES (%s, %s, %s)",
            (playlist_id, channel_id, playlist_names[i])
          )
      print("else :Data saved to playlist TAB")  
  except Exception as e:
       print("Inside playlist Error")
       print(e)        
  print("Data into playlist")
  mysql_connect.commit()
  mycursor.close()
  video_call(doc_push) #doc_push

def video_call(x):
# Details of the Video 
  print(x)
  #mycursor = mysql_connect.cursor(buffered=True)
  #mycursor.execute("SHOW TABLES LIKE 'playlist12'") # LIKE channel mycursor.execute("SHOW TABLES LIKE 'channel12'")
  print(" before try")
  #table_exist = mycursor.fetchmany()
  videos = x['video_id']
  print(videos)
 # print ("videos :",videos)
  print("Inside video TAB")
  for video in videos:
        # Extract video information
        video_id = video['video_id']
        #channel_id = video['channelId']
        play_id = video['playlist_id'] #playlist_id
        video_name = video['video_name']
        video_description = video['video_descrp']
        publish_date = video['publish_date']
        view_count = int(video['view_count'])  # Convert to int if it's a string
        like_count = int(video['like_count'])  # Convert to int if it's a string
        fav_comment = video['fav_comnt']  # Convert to int if it's a string
        duration = video['Duration']
        thumbnail = video['Thumbanil']
        caption_status = video['caption_status']
        #playlist_id = playlist['playlist_id'] 
        get_comments = video['comments']
        #print(get_comments)
        # Insert video data into the 'video' table in MySQL
        mycursor.execute(
            "INSERT INTO video (video_id, video_name, video_description, publish_date, view_count, "
            "like_count, fav_comment, duration, thumbnail, caption_status,playlist_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (video_id, video_name, video_description, publish_date, view_count, like_count,
             fav_comment, duration, thumbnail, caption_status,play_id)
        )
        filtered_comment_details = [item for item in get_comments if item is not None]

        if filtered_comment_details:
           for i in range(len(filtered_comment_details)):
              comment_detail = filtered_comment_details[i]
              comment_id = comment_detail['comment_id']
              comment_text = comment_detail['comment_text']
              comment_author = comment_detail['comment_author']
              publishAt = comment_detail['publishAt']

              mycursor.execute(
                    "INSERT INTO comment (comment_id, video_id, comment_text, comment_author, publish_date)"
                    "VALUES (%s, %s, %s, %s, %s)",
                    (comment_id, video_id, comment_text, comment_author, publishAt)
                )
        
  print("before the comment call")
  mysql_connect.commit()
  mycursor.close()
  st.success("Data pushed to MySQL successfully!")


# Define a function to push data to MongoDB
def push_to_mongodb(details):
   # mongodb_collection.insert_one({"name": channel_name, **channel_data})
  try:
    global mylist
    mylist = details
        #youtube_collection.create_index([("channel_id.channel_name", pymongo.ASCENDING)], unique=True)
    existing_channel = youtube_collection.find_one({"channel_id.channel_id": mylist["channel_id"]["channel_id"]})
    if existing_channel:
        print("Error: Channel with the same channel_id already exists.")
    else:
       print("Pinged your deployment. You successfully connected to MongoDB!")
      #client.collection.insert_one(mylist)
      #sql_connect(table_data)
  except Exception as e:
      print("Error :", e)
  
  st.success("Data pushed to MongoDB successfully!") 
  youtube_collection.insert_one(mylist)
  #client.close()
  st.subheader("Channel Details")
  df_channel = pd.DataFrame([channel_table])
  st.dataframe(df_channel)
  sql_connect(mylist)
  #push_to_mysql = st.button("MySQL")
   
       

def channelgetId(channel_name):
    request = youtube.search().list(q=channel_name, type= 'channel', part = 'id', maxResults=1)
    response =request.execute()
    #channel_id
    flatdata = flatten(response) #['items'][0]['id']['channelId']
    #print(flatdata)
    channel_id = flatdata['items_0_id_channelId']
    response = youtube.channels().list(
        id = channel_id,
        part = 'snippet,status,statistics,contentDetails' #localizations'
    )
    channel_overview = response.execute()
    channel_flat = flatten(channel_overview)
    #print(channel_flat)
    global channel_table
    #print("\n")
    channel_id = channel_flat['items_0_id']    #channel[items_0_id]
    channel_name = channel_flat['items_0_snippet_title']       #channel_overview['items'][0]['snippet']['title']
    #channel_type =channel_id_2['items'][0]['status']['madeForKids']
    channel_views = channel_flat['items_0_statistics_viewCount']   #channel_overview['items'][0]['statistics']['viewCount']
    channel_des = channel_flat['items_0_snippet_description']      #channel_overview['items'][0]['snippet']['description']
    channel_status = channel_flat['items_0_status_privacyStatus']  #channel_overview['items'][0]['status']['privacyStatus']#['status']['privacystatus'] #
    channel_table = {
        'channel_id' : channel_id,
        'channel_name':channel_name,
      #  'channel_type':channel_type,
        'channel_views':channel_views,
        'channel_des':channel_des,
        'channel_status':channel_status
    }
    playlist_tabledb(channel_flat)


def playlist_tabledb(y):
    #y=channel_flat
    passChan_id = y
    #print("\n")
    channel_id = passChan_id['items_0_id']
    #print('channel_id :',channel_id)
    #print("\n")
    response = youtube.channels().list(
        id = channel_id,
        part = 'snippet,status,statistics,contentDetails' #localizations'
    )
    channel_playlist = flatten(response.execute())
    # 'items_0_id': 'UCiEmtpFVJjpvdhsQ2QAhxVA'
   # print("\n")
    playid = channel_playlist['items_0_contentDetails_relatedPlaylists_uploads'] #UUbCmjCuTUZos6Inko4u57UQ
    #print('playlist_id :',playid) #playlist_id)
    #print("\n")
    request = youtube.playlists().list( #playlistsItems
        part = "id,contentDetails,localizations,player,snippet",
        #"contentDetails,id,snippet,localization,player", #id,contentDetails,localizations,player,snippet
        channelId = channel_id,
        # playlistId = playid, #playlistId wont come under playlists
        maxResults = 50
        #nextPageToken
        #prevPageToken
    )
    response = request.execute()
   # print(response)
    total = response['pageInfo']['totalResults']
   # print(total)
    title = []
    global playlist_table
    play_id = []
    if 'items' in response:
      for i in range(min(response['pageInfo']['resultsPerPage']-1, len(response['items']))): #'pageInfo_totalResults': 9  / total
          title.append(response['items'][i]['snippet']['title'])
    else :
      return None
    playlist_table = {
        'channel_id' : channel_id, #channelId ,
        'playlist_id' :playid, #playlistid, #play_id,
        'playlist_name': title
    }
    play_flat = flatten(playlist_table)
    #UCiEmtpFVJjpvdhsQ2QAhxVA guvi
    video_tabledata(passChan_id, play_flat)
    #get only the multiple playlist_id of the channel

def video_tabledata(z,c):
    channelInfo = z #channelId
    v_det = c #playlist_id
    playlt = v_det['playlist_id']
    request = youtube.playlistItems().list(
        part="id,snippet,contentDetails,status",
        playlistId=playlt,
        maxResults = 50
    )
    videodeets = request.execute()
    #print(flatten(videodeets))
    # After gettting the video ID for the channel playlist ,using videos() to get the info. related to the videos. And iterate them thro for loop.

    vidId =[]
    for i in range(videodeets['pageInfo']['resultsPerPage']):
        vidId.append(videodeets['items'][i]['contentDetails']['videoId']) #items_2_contentDetails_videoId

    #sampleVid =['4MPneA3QHsE', 'TMhrSf6RjhY']
    global total_video
    total_video = []
    for x in range(len(vidId)): #vidId  #use sampleVid to check the results
        #
        videoReq = youtube.videos().list(
            part = "contentDetails,id,localizations,player,recordingDetails, snippet, statistics,status,topicDetails",
            id = vidId[x]  # "DEIDOw_sFI4"

        )
     
        videoExe = videoReq.execute()
        #print(videoExe)
        #print("playlistId :",videodeets['items'][0]['snippet']['playlistId'] )
        video_deed = {
            'video_id' : videoExe['items'][0]['id'],
            'channel_id': videoExe['items'][0]['snippet']['channelId'],
            'playlist_id' :videodeets['items'][0]['snippet']['playlistId'], #videoExe['items'][0]['snippet']['playlistId'] , #items_47_snippet_playlistId
            'video_name' : videoExe['items'][0]['snippet']['title'], #items_0_snippet_title
            'video_descrp':videoExe['items'][0]['snippet']['description'] ,
            'publish_date' : videoExe['items'][0]['snippet']['publishedAt'],
            'view_count' : videoExe['items'][0]['statistics']['viewCount'],
            'like_count' : videoExe['items'][0]['statistics']['likeCount'],
            #dislike_count : videoExe[''],
            'fav_comnt' : videoExe['items'][0]['statistics']['favoriteCount'],
            #'comt_count' : videoExe['items'][0]['statistics']['commentCount'],
            'Duration' : videoExe['items'][0]['contentDetails']['duration'],
            'Thumbanil' : videoExe['items'][0]['snippet']['thumbnails']['default']['url'],
            'caption_status' : videoExe['items'][0]['contentDetails']['caption'],
            'comments' :[
                video_comments(vidId[x])
            ]
            }
        #print("video_deed :",video_deed)
        total_video.append(video_deed)
   # print(total_video)
    #video_comments(total_video,vidId)

def video_comments(a): # q,r
    videoNo = a
    #print('videoNo:',videoNo)
    for x in range(len(videoNo)):
      results = youtube.commentThreads().list(
      part='snippet',
      videoId = videoNo # used for commentThreads  videoId = videoNo[i]
    ).execute()
    comment = results.get('items',[])
    #print('comment : ',comment)
    if comment:

      for item in comment:
        comment_coll = {
          'comment_id' : item['snippet']['topLevelComment']['id'],
          'comment_text' : item['snippet']['topLevelComment']['snippet']['textDisplay'],
          'comment_author' : item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
          'publishAt' : item['snippet']['topLevelComment']['snippet']['publishedAt'],
          'video_id' : item['snippet']['videoId'] #videoId
        }
        return(comment_coll)
    else :
      return None

# Handle button click and data retrieval
if push_button:
  if channel_name:
    channelgetId(channel_name)
    atlast_info = {
        'channel_id' : channel_table,
        'playlist_id' : playlist_table,
        'video_id': total_video
      }
    push_to_mongodb(atlast_info)


if selected_question :
    if selected_question  == "1.What are the names of all the videos and their corresponding channels?" :
      mycursor.execute("""SELECT v.video_name, c.channel_name
                          FROM video AS v
                          JOIN playlist AS p ON v.playlist_id = p.playlist_id
                          JOIN channel AS c ON p.channel_id = c.channel_id""")
      df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
      st.write(df)

    elif selected_question  == "2. Which channels have the most number of videos, and how many videos do they have?":
       mycursor.execute(
          """SELECT c.channel_name, COUNT(v.video_id) AS video_count
              FROM channel AS c
              JOIN playlist AS p ON c.channel_id = p.channel_id
              JOIN video AS v ON p.playlist_id = v.playlist_id
              GROUP BY c.channel_name
              ORDER BY video_count DESC
              LIMIT 10; """
        )
       df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
       st.write(df)
       st.write("### :green[Number of videos in each channel :]")
        #st.bar_chart(df,x= mycursor.column_names[0],y= mycursor.column_names[1])
       fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
       st.plotly_chart(fig,use_container_width=True)

    elif selected_question  == "3. What are the top 10 most viewed videos and their respective channels?":
        mycursor.execute("""SELECT v.video_name, v.view_count, c.channel_name
                            FROM video AS v
                            JOIN playlist AS p ON v.playlist_id = p.playlist_id
                            JOIN channel AS c ON p.channel_id = c.channel_id
                            ORDER BY v.view_count DESC
                            LIMIT 10; """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif selected_question  == "4. How many comments were made on each video, and what are their corresponding video names?":
        mycursor.execute("""SELECT v.video_name, COUNT(c.comment_id) AS comment_count
                          FROM video AS v LEFT JOIN comment AS c ON v.video_id = c.video_id
                          GROUP BY v.video_id, v.video_name
                          ORDER BY comment_count DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
          
    elif selected_question  == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
        mycursor.execute("""SELECT v.video_name, COUNT(c.comment_id) AS comment_count
                            FROM video AS v LEFT JOIN comment AS c ON v.video_id = c.video_id
                            GROUP BY v.video_id, v.video_name ORDER BY comment_count DESC """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif selected_question  == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        mycursor.execute("""SELECT v.video_name, v.like_count, c.channel_name
                            FROM video AS v JOIN channel12 AS c ON v.playlist_id = c.channel_id
                            ORDER BY v.like_count DESC
                            LIMIT 10 """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
         
    elif selected_question  == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
        mycursor.execute("""SELECT channel_name , channel_views from channel12""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif selected_question  == "8. What are the names of all the channels that have published videos in the year 2022?":
        mycursor.execute("""SELECT DISTINCT c.channel_name
                            FROM channel12 AS c JOIN video AS v ON c.channel_id = v.playlist_id
                            WHERE YEAR(v.publish_date) = 2023
                         """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif selected_question  == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        mycursor.execute("""SELECT c.channel_name, 
           AVG(TIME_TO_SEC(TIME(CONVERT(SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'PT', -1), 'S', 1), 'SIGNED INTEGER'))) +
           TIME_TO_SEC(TIME(CONVERT(SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'PT', -1), 'M', 1), 'SIGNED INTEGER')) * 60)
           ) AS average_duration FROM channel12 AS c JOIN video AS v ON c.channel_id = v.playlist_id
           GROUP BY c.channel_name """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Avg video duration for channels :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif selected_question  == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
        mycursor.execute(""" SELECT v.video_id, v.video_name, v.view_count, v.like_count, v.fav_comment, v.duration, v.thumbnail, 
                            c.channel_name, COUNT(cm.comment_id) AS num_comments
                            FROM video AS v
                            JOIN channel12 AS c ON v.playlist_id = c.channel_id
                            LEFT JOIN comment AS cm ON v.video_id = cm.video_id
                            GROUP BY v.video_id, v.video_name, v.view_count, v.like_count, v.fav_comment, v.duration, v.thumbnail, c.channel_name
                            ORDER BY num_comments DESC
                            LIMIT 10 """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                     x=mycursor.column_names[1],
                     y=mycursor.column_names[2],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
# Close MongoDB and MySQL connectionsmongodb_client.close()
mycursor.close()
mysql_connect.close()
