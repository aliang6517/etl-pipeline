#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS USERS"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("CREATE TABLE songplays(songplay_id varchar PRIMARY KEY, start_time decimal, user_id int, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar)")

user_table_create = ("CREATE TABLE users(firstname varchar, lastname varchar, gender char, level varchar, user_id int)")

song_table_create = ("CREATE TABLE songs(song_id varchar, song_title varchar, artist_id varchar, year int, duration numeric)")

artist_table_create = ("CREATE TABLE artists(artist_id varchar, artist_latitude varchar, artist_longitude varchar, artist_location varchar, artist_name varchar)")

time_table_create = ("CREATE TABLE time(start_time varchar, hour int, day text, week_of_year int, month text, year int, weekday int)")

# INSERT RECORDS

songplay_table_insert = ("INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")

user_table_insert = ("INSERT INTO users (firstname, lastname, gender, level, user_id) VALUES (%s, %s, %s, %s, %s)")

song_table_insert = ("INSERT INTO songs (song_id, song_title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s)")

artist_table_insert = ("INSERT INTO artists (artist_id, artist_latitude, artist_longitude, artist_location, artist_name) VALUES (%s, %s, %s, %s, %s)")

time_table_insert = ("INSERT INTO time (start_time, hour, day, week_of_year, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)")

# FIND SONGS

song_select = ("SELECT songs.song_id, artists.artist_id FROM artists JOIN songs ON songs.song_title = %s AND artists.artist_name = %s AND songs.duration = %s")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]