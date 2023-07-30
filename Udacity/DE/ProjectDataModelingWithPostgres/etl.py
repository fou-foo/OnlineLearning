import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Process the `data/song_data/*` files that contain the 
    song information.

    INPUTS: 
    cur (cursor): Cursor to insert rows to tables
    filepath (string): Filepath to folder 'data/song_data/' where songs data is recorded
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Process the `data/log_data/*` files that contain the 
     information about the songs played by users.

    INPUTS: 
    cur (cursor): Cursor to insert rows to tables
    filepath (string): Filepath to folder 'data/log_data/' where songs data is recorded
    '''

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.where( df.page == 'NextSong').dropna()

    # convert timestamp column to datetime
    t = pd.to_datetime( df['ts'],  unit='ms')
    
    # insert time data records
    time_data = [ pd.to_datetime( t.dt.microsecond ,  unit='ms'), t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year,  t.dt.weekday  ]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'] 
    time_df =  { column_labels[x]:time_data[x] for x in range(len(column_labels)) }
    time_df = pd.DataFrame(time_df)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']] 

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
        songplay_data = [ pd.to_datetime( df['ts'][index] ,  unit='ms'), df['userId'][index], df['level'][index], songid, artistid, df['sessionId'][index], df['location'][index], df['userAgent'][index] ]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Function to count and list the content of folders inside 
    'data/log_data/' and 'data/song_data /'
    
    INPUTS: 
         cur (cursor): Cursor to insert rows
         conn (conection): Connection to Postgres DB
         filepath (string): Path to folder to process
         func (function): Function to apply 
    '''

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


def main():
    '''
    Main function to perform the ETL process
        - First create the connection to the DB
        - Initialize the cursor
        - Process the songs
        - Process the logs
    '''
    #conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    # LOCAL TESTING
    conn = psycopg2.connect(database = "studentdb", user = "student2", password = "student2", host = "localhost", port = "5432")
   
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()