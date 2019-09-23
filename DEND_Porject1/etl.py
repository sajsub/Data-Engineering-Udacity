import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):

    """ Function process_song_file is used to process the song files;
	Tables songs and artists will be loaded using this function
    """
    # open song file
    # Read json is used to read the files
    df = pd.read_json(filepath, lines=True)
   
    #insert song record
    #Song data is used to store the columns and the respective data
    
    song_data = list(df.loc[:,["song_id","title", "artist_id", "year","duration"]].values[0])
    cur.execute(song_table_insert, song_data)
    
    
    #insert artist record
    #artist_data variable is used for processing the artist data
    
    artist_data = list(df.loc[:,["artist_id","artist_name","artist_location", \
	"artist_latitude","artist_longitude"]].values[0])
    cur.execute(artist_table_insert, artist_data)
   
    print(process_song_file.__doc__) 

def process_log_file(cur, filepath):

    """Function process_log_file is used for processing Log files
   Loads the tables user, time and songsplay data.
    """
	
    #open log file
    #This portion reads the log files.
    
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[(df['page']=='NextSong')]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit='ms')
    
    # insert time data records
    time_data = list((t,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.dayofweek))
    column_labels =('start_time', 'hour', 'day', 'week', 'month', 'year','weekday')
    time_df = pd.DataFrame(dict(zip(column_labels,time_data)),columns=column_labels).reset_index(drop=True)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    #sngs1 = df[(df['userId']!='')]
    user_df = df[['userId','firstName','lastName','gender','level']]
    #user_df = user_df1.drop_duplicates()
    

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'),row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        print(process_log_file.__doc__) 

def process_data(cur, conn, filepath, func):

    """Function process_data is used for finding the Json files  """
	
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
    print(process_data.__doc__) 

def main():

    """Main fucntion connects to the DB and invokes the functions for processing song and log files"""
	
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()
    print(main.__doc__)

if __name__ == "__main__":
    main()
