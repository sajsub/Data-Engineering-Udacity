import configparser
from datetime import datetime
import calendar
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
"""
      Read a dl.cfg for AWS credential details
"""
config.read_file(open('dl.cfg'))
"""
       Retreving the AWS connection details
"""
os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
      Creating a spark session for data processing:
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
     Function for processsing song data: Inputs are as below.
     input data: Path where files are placed.
     output data: Path where output will be written
    """
    # Reterive the file path for processing the song data.
    print("Starting the file processing!")
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    print("Fetched the files for songs:")
    # creating a data frame for reading song data.
    df = spark.read.json(song_data)
    # Extracting the details for songs table!
    print("Extracting the columns for songs:")
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']

    # write songs table to parquet files partitioned by year and artist
    print("Writing the song table:")
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')
    print("Processing for songs.parquet is completed:")
    # creating a data frame for creating artists table
    print("Starting artists_table:")
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']

    # Create parquet file for artists table:
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')
    print("Data processing for artists.parquet completed:")
    print("Processing for song data completed!")

def process_log_data(spark, input_data, output_data):
    """
     Function for processsing log data: Inputs are as below.
     input data: Path where files are placed.
     output data: Path where output will be written
    """
    # Extract the log path for processing:
    # log_data =os.path.join(input_data,"log_data/*/*/*.json")
    print("Starting processing for log data !")
    log_data = os.path.join(input_data, "log_data/*.json")

    # Processing the Log file data
    df1 = spark.read.json(log_data)

    # Filter by Nextsongs for data processing
    print("Filtering by Nextsong:")
    df = df1.filter(df1.page == 'NextSong')

    # creating a songsplays table using the fieldssongplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

    songplays_table = df['ts', 'userId', 'level', 'sessionId', 'location', 'userAgent']

    # Extract columns for users table
    # Fields used will be user_id, first_name, last_name, gender, level
    #users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table_Temp = df['userId', 'firstName', 'lastName', 'gender', 'level','userAgent']
    users_table = users_table_Temp.drop_duplicates(subset=['userId'])
    # Create users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')
    print("users.parquet completed!")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x) / 1000)))
    get_week = udf(lambda x: calendar.day_name[x.weekday()])
    get_weekday = udf(lambda x: x.isocalendar()[1])
    get_hour = udf(lambda x: x.hour)
    get_day = udf(lambda x: x.day)
    get_year = udf(lambda x: x.year)
    get_month = udf(lambda x: x.month)

    df = df.withColumn('start_time', get_datetime(df.ts))
    df = df.withColumn('hour', get_hour(df.start_time))
    df = df.withColumn('day', get_day(df.start_time))
    df = df.withColumn('week', get_week(df.start_time))
    df = df.withColumn('month', get_month(df.start_time))
    df = df.withColumn('year', get_year(df.start_time))
    df = df.withColumn('weekday', get_weekday(df.start_time))
    # Extract columns to create time table
    time_table = df['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']

    # Creating time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')
    print("Processing of Time parquet completed")
    # Read in song data to use for songplays table
    song_df = spark.read.parquet("s3a://sparkify-dend/songs.parquet")

    # Creation of songplay table:
    df = df.join(song_df, song_df.title == df.song)
    songplays_table = df['start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent','year','month']
    songplays_table.select(monotonically_increasing_id().alias('songplay_id')).collect()
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')
    print("Songplay processing completed:")
    print("Processing for log data is completed ***\n\n END")

def main():
    """
     Function for invoking main()
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    #input_data = "/home/workspace/data/"
    output_data = "s3a://sparkify-dend/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()