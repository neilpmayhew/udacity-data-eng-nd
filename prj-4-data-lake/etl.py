import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,from_unixtime,monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CONFIG']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CONFIG']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):

    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json")

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table

    song_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.select(song_columns)

    # write songs table to parquet files partitioned by year and artist
    song_output_dir = os.path.join(output_data,'songs')
    songs_table.write.partitionBy("year", "artist_id").parquet(song_output_dir)

    # extract columns to create artists table
    artists_columns = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_columns)

    # write artists table to parquet files
    artists_output_dir = os.path.join(output_data,'artists')
    artists_table.write.parquet(artists_output_dir)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data,'log_data/*/*/*.json')
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # # extract columns for users table
    users_columns = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(users_columns).dropDuplicates()

    # # write users table to parquet files
    users_output_dir = os.path.join(output_data,'users')
    users_table.write.parquet(users_output_dir)

    # # create timestamp column from original timestamp column
    df = df.withColumn('start_time',from_unixtime(col('ts')/1000))

    # # extract columns to create time table
    time_table = df.select("start_time")\
        .dropDuplicates()\
        .withColumn("hour", hour(col("start_time")))\
        .withColumn("day", dayofmonth(col("start_time")))\
        .withColumn("week", weekofyear(col("start_time")))\
        .withColumn("month", month(col("start_time")))\
        .withColumn("year", year(col("start_time")))\
        .withColumn("weekday", date_format(col("start_time"),'E'))

    # write time table to parquet files partitioned by year and month
    time_output_dir = os.path.join(output_data,'time')
    time_table.write.parquet(time_output_dir)

    # # read in song data to use for songplays table
    song_output_dir = os.path.join(output_data,'songs')
    song_df = spark.read.parquet(song_output_dir)

    # load artists from parquet file written by previous function
    artists_output_dir = os.path.join(output_data,'artists')
    artists_df = spark.read.parquet(artists_output_dir)

    # join song and artist data together dropping artist_id from the song data frame in order to avoid duplicate column
    song_artists_columns = ['title','name','artist_id','song_id']
    song_artists_df = song_df.join(artists_df,song_df.artist_id == artists_df.artist_id)\
                             .drop(song_df.artist_id)\
                             .select(song_artists_columns)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_columns = ['songplay_id','start_time','userId as user_id','level','song_id','artist_id','sessionId as session_id','location','userAgent as user_agent','year(start_time) as year','month(start_time) as month']
    songplays_table = df.join(song_artists_df,(df.song == song_artists_df.title) & (df.artist == song_artists_df.name))\
                        .withColumn('songplay_id',monotonically_increasing_id())\
                        .selectExpr(songplays_columns)

    # write songplays table to parquet files partitioned by year and month
    song_plays_output_dir = os.path.join(output_data,'songplays')
    songplays_table.write.partitionBy("year", "month").parquet(song_plays_output_dir)

def main():
    spark = create_spark_session()
    # input_data = "s3a://udacity-dend/"
    # output_data = ""

    input_data = '/home/neil/src/udacity-data-eng-nd/prj-4-data-lake/data'
    output_data='/home/neil/src/udacity-data-eng-nd/prj-4-data-lake/output'
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
