import logging

from main import authenticate_youtube


def fetch_top_10_most_popular_youtube_videos(**kwargs):
    try:
        # authenticate using api-key
        youtube = authenticate_youtube()

        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            chart="mostPopular",
            regionCode="GB",
            maxResults=2,
            videoCategoryId=25
        )
        response = request.execute()
        videos = response['items']
        for video in videos:
            video_id = video['id']
            comments = fetch_video_comments(youtube, video_id)
            video['comments'] = comments

        kwargs['ti'].xcom_push(key='t1_output', value=videos)

        # return videos
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


def fetch_video_comments(youtube, video_id):
    try:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            maxResults=200,
            order="time",
        )
        response = request.execute()
        comments = response['items']

        return comments
    except Exception as e:
        logging.error(f"An error occurred while fetching comments: {e}")
        return []
