import logging

from matplotlib import pyplot as plt
from nltk import word_tokenize, WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.sentiment import SentimentIntensityAnalyzer
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, when, col
from pyspark.sql.types import StringType, StructType, StructField, FloatType, DateType, IntegerType
from textblob import TextBlob
from wordcloud import WordCloud

# nltk.download()
import sys
from pathlib import Path

current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

STOPWORDS = set(stopwords.words('english'))


def process_youtube_data(spark, **kwargs):
    ti = kwargs['ti']
    videos = ti.xcom_pull(task_ids='fetch_top_10_most_popular_youtube_videos', key='t1_output')
    # logging.info(f'Transformed videos: {videos}')

    # videos = fetch_top_10_most_popular_youtube_videos()
    logging.info(f"SparkSession Configuration: {spark.sparkContext.getConf().getAll()}")

    # Define the schema explicitly
    comments_schema = StructType([
        StructField("_id", StringType(), True),
        StructField("original_text", StringType(), True),
        StructField("author_name", StringType(), True),
        StructField("video_id", StringType(), True),
        StructField("like_count", IntegerType(), True),
        StructField("published_at", StringType(), True),
        StructField("total_reply_count", IntegerType(), True)
    ])

    def perform_sentiment_analysis(df):
        # Tokenization using PySpark
        tokenize_udf = udf(lambda text: text.split(), StringType())
        df = df.withColumn("tokens", tokenize_udf("formatted_text"))

        # Sentiment analysis using NLTK
        sid = SentimentIntensityAnalyzer()
        sentiment_udf = udf(lambda tokens: sid.polarity_scores(" ".join(tokens))["compound"], FloatType())
        df = df.withColumn("sentiment_score", sentiment_udf("tokens"))
        df = df.withColumn("sentiment", when(col("sentiment_score") > 0, "Positive")
                           .when(col("sentiment_score") < 0, "Negative")
                           .otherwise("Neutral"))
        return df

    def getSubjectivity(text):
        return TextBlob(text).sentiment.subjectivity

    def getPolarity(text):
        return TextBlob(text).sentiment.polarity

    def generate_word_cloud(comments_list):
        # Concatenate all comments into a single string
        text = " ".join(comments_list)

        # Generate a word cloud
        wordcloud = WordCloud(width=800, height=400, background_color="white").generate(text)

        # Display the word cloud using matplotlib
        plt.figure(figsize=(10, 5))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        plt.show()

    def preprocess_text(text):
        # Tokenize the text
        tokens = word_tokenize(text.lower())
        # Remove stop words
        filtered_tokens = [token for token in tokens if token not in STOPWORDS]
        # Lemmatize the tokens
        lemmatizer = WordNetLemmatizer()
        lemmatized_tokens = [lemmatizer.lemmatize(token) for token in filtered_tokens]
        # Join the tokens back into a string
        processed_text = ' '.join(lemmatized_tokens)

        return processed_text

    comments_array = []
    videos_array = []
    for video in videos:
        # print(videos)
        extracted_video_fields = {
            "_id": video.get("id"),
            "published_at": video.get("snippet", {}).get('publishedAt'),
            "channel_id": video.get("snippet", {}).get('channelId'),
            "title": video.get("snippet", {}).get('title'),
            "description": video.get("snippet", {}).get('description'),
            "channel_title": video.get("snippet", {}).get('channelTitle'),
            "tags": video.get("snippet", {}).get('tags'),
            "category_id": video.get("snippet", {}).get('categoryId'),
            "view_count": video.get("statistics", {}).get('viewCount'),
            "like_count": video.get("statistics", {}).get('likeCount'),
            "comment_count": video.get("statistics", {}).get('commentCount'),
            # Add more fields to extract as needed
        }
        videos_array.append(extracted_video_fields)
        print(videos_array)
        print(extracted_video_fields)
        comments = [(comment.get('id', ''),
                     comment.get('snippet', {}).get('topLevelComment', {}).get('snippet', {}).get('textOriginal', ''),
                     comment.get('snippet', {}).get('topLevelComment', {}).get('snippet', {}).get('authorDisplayName',
                                                                                                  ''),
                     comment.get('snippet', {}).get('videoId', ''),
                     comment.get('snippet', {}).get('topLevelComment', {}).get('snippet', {}).get('likeCount', 0),
                     comment.get('snippet', {}).get('topLevelComment', {}).get('snippet', {}).get('publishedAt', ''),
                     comment.get('snippet', {}).get('totalReplyCount', 0),
                     )
                    for comment in video['comments']]

        columns = ["text"]
        logging.info(f'Comments : {comments}')

        df = spark.createDataFrame(comments, schema=comments_schema)
        preprocess_text_udf = udf(preprocess_text, StringType())
        df = df.withColumn("formatted_text", preprocess_text_udf("original_text"))
        df = perform_sentiment_analysis(df)
        subjectivity_udf = udf(getSubjectivity, FloatType())
        polarity_udf = udf(getPolarity, FloatType())
        df = df.withColumn("subjectivity", subjectivity_udf("formatted_text"))
        df = df.withColumn("polarity", polarity_udf("formatted_text"))
        # Show the result
        # df.show(10, truncate=False)
        # Collect the comments to a local list
        comments_list = [row["formatted_text"] for row in df.select("formatted_text").collect()]

        # Create a word cloud
        # generate_word_cloud(comments_list)

        # video['comments'] = [row.asDict() for row in df.collect()]
        comments_array.extend([row.asDict() for row in df.collect()])
        # Create a new array to store only comments

    kwargs['ti'].xcom_push(key='t2_video_output', value=videos_array)
    kwargs['ti'].xcom_push(key='t2_comment_output', value=comments_array)