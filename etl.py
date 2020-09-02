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


#Connect to database


conn = psycopg2.connect("host={} dbname=sparkifydb user={} password={}".format(config.host_port, config.user, config.password))
cur = conn.cursor()

#Define function to get all files in a directory

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    return all_files

# songs table
# - Select the first song in this list
# - Read the song file and view the data

filepath = 'data/song_data'

song_files = get_files('data/song_data')

df = pd.read_json(song_files[1], lines = True)
df.head()

# - Index to select the first (only) record in the dataframe
# - Convert the array to a list and set it to `song_data`

song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].iloc[0].values.tolist()
song_data

for song in song_data:
    print(type(song))


# Handle strings that defaulted to numpy objects

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)


# Insert Record into songs Table

cur.execute(song_table_insert, song_data)
conn.commit()


# artists table
# - Select columns for artist ID, name, location, latitude, and longitude
# - Use `df.values` to select just the values from the dataframe
# - Index to select the first (only) record in the dataframe
# - Convert the array to a list and set it to `artist_data`

artist_data = df[['artist_id', 'artist_latitude', 'artist_longitude', 'artist_location', 'artist_name']].fillna('').iloc[0].values.tolist()
artist_data


# - Insert Record into Artist Table

cur.execute(artist_table_insert, artist_data)
conn.commit()


# Process log_data

# - Select the first log file in this list
# - Read the log file and view the data

log_files = get_files('data/log_data')

filepath = 'data/log_data'

df = pd.read_json(log_files[1], lines = True)
df.head()


# time table
# - Filter records by `NextSong` action
# - Convert the `ts` timestamp column to datetime
# - Extract the timestamp, hour, day, week of year, month, year, and weekday from the `ts` column and set `time_data` to a list containing these values in order
# - Specify labels for these columns and set to `column_labels`
# - Create a dataframe time_df

df = df[df.page == 'NextSong']
df.head()


t = pd.to_datetime(df.ts, unit = 'ms')
t.head()


time_data = list(zip(t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday))
column_labels = ['start_time', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']


time_df = pd.DataFrame(time_data, columns = column_labels)
time_df.head()

for time in time_df.head():
    print(type(time))


# - Insert Records into time table

for i, row in time_df.iterrows():
    cur.execute(time_table_insert, list(row))
    conn.commit()

# users table
# - Select columns for user ID, first name, last name, gender and level and set to `user_df`

user_df = df[['firstName', 'lastName', 'gender', 'level', 'userId']]


# - Insert Records into Users Table

for i, row in user_df.iterrows():
    cur.execute(user_table_insert, row)
    conn.commit()

# songplays table
# - Select the appropriate appropriate records
# - Insert Records into Songplays Table

for index, row in df.iterrows():

    # get songid and artistid from song and artist tables
    cur.execute(song_select, (row.song, row.artist, row.length))
    results = cur.fetchone()

    if results:
        songid, artistid = results
    else:
        songid, artistid = None, None

    # insert songplay record
    songplay_data = df[['ts', 'userId', 'level', 'sessionId', 'location', 'userAgent']].fillna('').iloc[0].values.tolist()
    songplay_data.extend([songid, artistid])
    cur.execute(songplay_table_insert, songplay_data)
    conn.commit()


# Close Connection to database

conn.close()
