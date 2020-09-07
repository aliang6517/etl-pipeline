#!/usr/bin/env python
# coding: utf-8

# # ETL Processes


import os
import config
import glob
import psycopg2
import pandas as pd
import numpy
from psycopg2.extensions import register_adapter, AsIs
from sql_queries import *


import os
import glob
import psycopg2
import pandas as pd
import numpy
from sql_queries import *
from psycopg2.extensions import register_adapter, AsIs


def process_song_file(cur, filepath):
    '''
    Loads song files using pandas and inserts the appropraite records into the song and artist tables
        cur - psycopg2 cursor object
        filepath - filepath of the file to be read
    '''
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].iloc[0].fillna('').values.tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[['artist_id', 'artist_latitude', 'artist_longitude', 'artist_location', 'artist_name']].iloc[0].fillna('').values.tolist()
    cur.execute(artist_table_insert, artist_data)



def process_log_file(cur, filepath):
    # open log file
    '''
    Loads log files using pandas and inserts time, user, and songplay records
        cur - psycopg2 cursor object
        filepath - filepath of the file to be read
    '''
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit = 'ms')

    # insert time data records
    time_data = list(zip(t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday))
    column_labels = ['start_time', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['firstName', 'lastName', 'gender', 'level', 'userId']]

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
            # insert songplay record
            try:
                songplay_data = row[['ts', 'userId', 'level', 'sessionId', 'location', 'userAgent']].fillna('').values.tolist()
                songplay_data.extend([songid, artistid, index])
                cur.execute(songplay_table_insert, songplay_data)
                conn.commit()
            except:
                print("Error at inserting")

        else:
            songid, artistid = None, None



def process_data(cur, conn, filepath, func):
    '''
    Gets files matching the given filepath and iterates over the files found
        cur - psycopg2 cursor object
        conn - database session
        filepath - directory in which files are contained
        func - the function to be applied on each file
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


def addapt_numpy_float64(numpy_float64):
    '''Adapts numpy float64 objects'''
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    '''Adapts numpy int64 objects'''
    return AsIs(numpy_int64)

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    register_adapter(numpy.float64, addapt_numpy_float64)
    register_adapter(numpy.int64, addapt_numpy_int64)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
