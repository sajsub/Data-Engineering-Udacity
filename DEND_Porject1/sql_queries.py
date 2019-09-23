# DROP TABLES

songplay_table_drop = "DROP TABLE songplays"
user_table_drop =  "DROP TABLE users"
song_table_drop = "DROP TABLE songs"
artist_table_drop = "DROP TABLE artists"
time_table_drop = "DROP TABLE time"



# CREATE TABLES


songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays (songplay_id serial PRIMARY KEY, \
                                                                start_time timestamp not null,\
                                                                user_id int not null,\
                                                                level text,\
                                                                song_id varchar,\
                                                                artist_id varchar,\
                                                                session_id int,\
                                                                location varchar,\
                                                                user_agent varchar);")

user_table_create = ("CREATE TABLE users (user_id int primary key, \
                                                       first_name varchar,\
													    last_name varchar, \
                                                        gender varchar,\
														level text);")		


song_table_create = ("CREATE TABLE IF NOT EXISTS songs (song_id varchar primary key, \
                                                       title varchar,\
													    artist_id varchar, \
                                                        year int,\
														duration NUMERIC(5, 2));")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists (artist_id varchar primary key, \
                                                       name varchar,\
													    location varchar, \
                                                        lattitude NUMERIC (5, 2),\
														longitude NUMERIC (5, 2));")

time_table_create = ("CREATE TABLE IF NOT EXISTS time (start_time timestamp, \
                                                       hour int,\
													    day int, \
														month int,\
                                                        week int,\
														year int,\
														weekday int);")



song_table_insert = ("""
INSERT INTO songs (song_id ,
                title ,
                artist_id ,
                year ,
                duration )
             VALUES(%s, %s, %s, %s, %s) 
             ON CONFLICT (song_id) DO NOTHING
""")
artist_table_insert = (""" 
INSERT INTO ARTISTS (artist_id ,
                    name ,
                    location,
                    lattitude,
                    longitude) 
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (artist_id) DO NOTHING
""")

time_table_insert = (""" 
INSERT INTO time (start_time,
                  hour,
                  day,
                  week,
                  month,
                  year,
                  weekday)
                  VALUES (%s, %s, %s, %s, %s, %s, %s)
""")


user_table_insert = ("""
insert into users(user_id,
                first_name, 
                last_name,
                gender,
                level)
                values (%s, %s, %s, %s, %s)  
                ON CONFLICT(user_id) DO UPDATE SET level=EXCLUDED.level 
""")

songplay_table_insert = ("""
insert into songplays(start_time,
                    user_id,
                    level,
                    song_id,
                    artist_id,
                    session_id,
                    location,
                    user_agent)
                    values (%s,%s,%s,%s,%s,%s,%s,%s)
""")

song_select = (""" SELECT s.song_id as song_id , a.artist_id as artist_id
                   FROM songs s 
                   LEFT JOIN artists a on
                   a.artist_id =  s.artist_id where s.title = %s and a.name = %s and s.duration = %s
                   """)

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]