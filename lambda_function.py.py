import pandas as pd
import numpy as np
from dateutil import parser
import isodate
from googleapiclient.discovery import build
from datetime import date, timedelta
import boto3
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def lambda_handler(event, context):
    
    #########################--GOOGLE SHEETS API ACCESS--########################
    #############################################################################
    
    # The credentials for accessing google sheets api was stored in an S3 bucket
    s3 = boto3.client('s3')
    bucket_name = 'bucket_name'     # Replace with your bucket name 
    file_key = 'file_key'           # Replace with your file name   
    
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    
    file_content = response['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(json_content)
    client = gspread.authorize(credentials)
    
    spreadsheet = client.open_by_key("workbook_id")     # Replace with your workbook id
    yt_data = spreadsheet.worksheet("worksheet_name")   # Replace with worksheet name
    
    print("Google Sheets API Access Successful")
    
    ###########################--Youtube API ACCESS--############################
    #############################################################################
    
    # ## Data Extraction from Youtube API
    channel_ids = ['UCUvvj5lwue7PspotMDjk5UA', # MeetKevin
                'UCV6KDgJskWaEckne5aPA0aQ',  # Graham Stephan
                'UCGy7SkBjcIAgTiwkXEtPnYg',  # Andrei Jikh
                'UCnMn36GT_H0X-w5_ckLtlgQ',  # Financial Education (Jeremy)
                'UCJwKCyEIFHwUOPQQ-4kC1Zw'   # Tom Nash
                ]

    # Credentials
    api_key = 'youtube_api_key'     # Replace with api key
    api_service_name = "youtube"
    api_version = "v3"

    # Get credentials and create an API client
    youtube = build(api_service_name, api_version, developerKey=api_key)

    # Extract data using Youtube API
    class youtube_api:
        def __init__(self, api_key, api_service_name, api_version):
            # Create an API Client
            self.youtube = build(api_service_name, api_version, developerKey=api_key)

        def get_channel_data(self, channel_ids):
            all_channel_data = []

            request = self.youtube.channels().list(
                part = "snippet,contentDetails,statistics",
                id = ",".join(channel_ids)
                )
            response = request.execute()

            # loop through each channel details
            for item in response['items']:
                channel_data = {'channelName': item['snippet']['title'],
                                'createdAt' : item['snippet']['publishedAt'],
                               'subscribers': item['statistics']['subscriberCount'],
                               'views': item['statistics']['viewCount'],
                               'totalVideos': item['statistics']['videoCount'],
                               'playlistId': item['contentDetails']['relatedPlaylists']['uploads']}

                all_channel_data.append(channel_data)

            return pd.DataFrame(all_channel_data)

        def get_video_ids(self, channel_ids):
            # For each channel, there is a playlist ID we need to extract
            playlist_ids = self.get_channel_data(channel_ids)['playlistId'].tolist()

            video_ids = {}

            # For each playlist ID, we need to extract all the associated video IDs
            for playlist_id in playlist_ids:
                video_ids[playlist_id] = self._get_video_ids_(playlist_id)

            return video_ids

        # This function extracts the associated video IDs for each playlist ID
        def _get_video_ids_(self, playlist_id):
            video_ids = []
            request = self.youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults = 30
            )
            response = request.execute()

            for item in response['items']:
                video_ids.append(item['contentDetails']['videoId'])   
            return video_ids

        # This function gets the data associated with each video id
        def get_video_data(self, channel_ids):
            video_ids_per_channel = self.get_video_ids(channel_ids)
            all_video_df = pd.DataFrame()

            for key in video_ids_per_channel.keys():
                video_data = self._get_video_data_per_channel_(video_ids_per_channel[key])
                video_df = pd.DataFrame(video_data)
                all_video_df = pd.concat([all_video_df, video_df], ignore_index=True)

            return all_video_df

        def _get_video_data_per_channel_(self, video_ids):
            all_video_data = []

            data_to_get = {
                'snippet': ['channelTitle', 'title', 'publishedAt'],
                'statistics': ['viewCount', 'likeCount', 'commentCount'],
                'contentDetails': ['duration']
            }

            for i in range(0, len(video_ids), 50):
                request = self.youtube.videos().list(
                    part = "snippet,contentDetails,statistics",
                    id = ','.join(video_ids[i:i+50])
                )
                response = request.execute()

                for video in response['items']:
                    video_data = {}
                    video_data['video_id'] = video['id']

                    for key in data_to_get.keys():
                        for value in data_to_get[key]:
                            try:
                                video_data[value] = video[key][value]
                            except:
                                video_data[value] = None
                    all_video_data.append(video_data)

            return pd.DataFrame(all_video_data)

    # Create a youtube_api object and extract data from API
    youtube_api_access = youtube_api(api_key, api_service_name, api_version)
    df = youtube_api_access.get_video_data(channel_ids)
    
    ###########################--DATA TRANSFORMATION--###########################
    #############################################################################

    # Convert date Column and Create a new Column 'publishDayOfWeek'
    df['publishedAt'] = df['publishedAt'].apply(lambda x: parser.parse(x))
    df['publishDayOfWeek'] = df['publishedAt'].apply(lambda x: x.strftime("%A"))

    # Convert duration to Seconds
    df['durationSec'] = df['duration'].apply(lambda x: isodate.parse_duration(x))
    df['durationSec'] = df['durationSec'].astype('timedelta64[s]')
    df.drop('duration', axis=1, inplace=True)
    
    # Extract Data for the last 7 days
    df = df[(df.publishedAt.dt.date>=(date.today()-timedelta(days=7))) & (df.publishedAt.dt.date<date.today())]
    df['publishedAt'] = df['publishedAt'].astype(str)
    df['commentCount'] = df['commentCount'].fillna(0)
    df['likeCount'] = df['likeCount'].fillna(0)
    df['viewCount'] = df['viewCount'].fillna(0)
    df.fillna('', inplace=True)
    df[['viewCount', 'commentCount', 'likeCount', 'durationSec']] = df[['viewCount', 'commentCount', 'likeCount', 'durationSec']].astype('int64')

    #######################--LOAD DATA TO GOOGLE SHEET--#########################
    #############################################################################
    
    if df.empty:
        print("No Youtube Data to be Uploaded")
    else:
        yt_data.append_rows(df.values.tolist())
        print("Latest Youtube Data Uploaded")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Youtube ETL Process Completed')
    }
