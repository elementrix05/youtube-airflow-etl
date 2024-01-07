import logging

from pymongo import MongoClient, UpdateOne


def save_youtube_data(**kwargs):
    ti = kwargs['ti']
    videos = ti.xcom_pull(task_ids='process_youtube_data', key='t2_video_output')
    comments = ti.xcom_pull(task_ids='process_youtube_data', key='t2_comment_output')
    # logging.info(f'Transformed videos: {videos}')

    uri = MONGO_URI

    # Create a MongoClient
    client = MongoClient(uri)

    # Access the database and collection
    database = client.get_database("youtube")
    video_collection = database.get_collection("videos")
    comment_collection = database.get_collection("comments")

    video_update_operations = [
        UpdateOne({"_id": video["_id"]}, {"$set": video}, upsert=True) for video in videos
    ]
    comment_update_operations = [
        UpdateOne({"_id": comment["_id"]}, {"$set": comment}, upsert=True) for comment in comments
    ]

    # Execute the bulk write operation
    video_collection.bulk_write(video_update_operations)

    # video_collection.insert_many(videos)
    # comment_collection.insert_many(comments)
    comment_collection.bulk_write(comment_update_operations)