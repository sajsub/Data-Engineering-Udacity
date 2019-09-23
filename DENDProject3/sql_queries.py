import configparser

""" BELOW PORTION CONTAINS THE CONFIGURATION DETAILS """
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

""" BELOW VARIABLES ARE USED FOR DROPING ANY STAGING TABLES OR THE MAIN TABLES WHICH ARE USED """

STG_EVENTS_TABLE_DROP = "DROP TABLE IF EXISTS STG_EVENTS"
STG_SONGS_TABLE_DROP = "DROP TABLE IF EXISTS STG_SONGS"
SONGPLAY_TABLE_DROP = "DROP TABLE IF EXISTS SONGPLAY"
USER_TABLE_DROP = "DROP TABLE IF EXISTS USERS"
SONG_TABLE_DROP = "DROP TABLE IF EXISTS SONG"
ARTIST_TABLE_DROP = "DROP TABLE IF EXISTS ARTIST"
TIME_TABLE_DROP = "DROP TABLE IF EXISTS TIME"

""" TABLE CREATION QUERIES ARE MENTIONED INT THE BELOW PORTION """
""" TABLE CREATION FOR STG_EVENTS """

STG_EVENTS_TABLE_CREATE = ("""CREATE TABLE IF NOT EXISTS STG_EVENTS(
                                ARTIST TEXT,
                                AUTH TEXT,
                                FIRST_NAME TEXT,
                                GENDER CHAR(1),
                                ITEM_SESSION INTEGER,
                                LAST_NAME TEXT,
                                LENGTH NUMERIC,
                                LEVEL TEXT,
                                LOCATION TEXT,
                                METHOD TEXT,
                                PAGE TEXT,
                                REGISTRATION NUMERIC,
                                SESSION_ID INTEGER,
                                SONG TEXT,
                                STATUS INTEGER,
                                TS BIGINT,
                                USER_AGENT TEXT,
                                USER_ID INTEGER
                            )
""")

""" TABLE CREATION FOR STAGING SONGS TABLE: """
STG_SONGS_TABLE_CREATE = ("""CREATE  TABLE IF NOT EXISTS STG_SONGS(
                                NUM_SONGS INTEGER,
                                ARTIST_ID TEXT,
                                ARTIST_LATITUDE NUMERIC,
                                ARTIST_LONGITUDE NUMERIC,
                                ARTIST_LOCATION TEXT,
                                ARTIST_NAME TEXT,
                                SONG_ID TEXT,
                                TITLE TEXT,
                                DURATION NUMERIC,
                                YEAR INTEGER
                            )
""")

""" TABLE CREATION FOR SONGPLAY TABLE """

SONGPLAY_TABLE_CREATE = ("""CREATE TABLE IF NOT EXISTS SONGPLAY(
                            SONGPLAY_ID INT IDENTITY(1,1) PRIMARY KEY,
                            START_TIME TIMESTAMP,
                            USER_ID INTEGER NOT NULL,
                            LEVEL TEXT,
                            SONG_ID TEXT,
                            ARTIST_ID TEXT,
                            SESSION_ID INTEGER,
                            LOCATION TEXT,
                            USER_AGENT TEXT
                        )
""")

##TABLE CREATION FOR STORING THE USER RELATED INFORMATION##
USER_TABLE_CREATE = ("""CREATE TABLE IF NOT EXISTS USERS(
                        USER_ID INTEGER PRIMARY KEY,
                        FIRST_NAME TEXT NOT NULL,
                        LAST_NAME TEXT NOT NULL,
                        GENDER CHAR(1),
                        LEVEL TEXT
                    )
""")

""" TABLE CREATION FOR SONG """

SONG_TABLE_CREATE = ("""CREATE TABLE IF NOT EXISTS SONG(
                        SONG_ID TEXT PRIMARY KEY,
                        TITLE TEXT,
                        ARTIST_ID TEXT,
                        YEAR INTEGER,
                        DURATION NUMERIC
                    )
""")
##TABLE CREATION FOR ARTIST
ARTIST_TABLE_CREATE = ("""CREATE TABLE IF NOT EXISTS ARTIST(
                          ARTIST_ID TEXT PRIMARY KEY,
                          NAME TEXT,
                          LOCATION TEXT,
                          LATITUDE NUMERIC,
                          LONGITUDE NUMERIC
                       )
""")

""" TABLE CREATION FOR TIME """

TIME_TABLE_CREATE = ("""CREATE TABLE IF NOT EXISTS TIME(
                        START_TIME TIMESTAMP PRIMARY KEY,
                        HOUR INTEGER,
                        DAY INTEGER,
                        WEEK INTEGER,
                        MONTH INTEGER,
                        YEAR INTEGER,
                        WEEKDAY INTEGER
                    )
""")

""" THIS PORTION IS USED FOR LOADING THE DATA FROM S3 INTO STAGING TABLES """
""" LOG DATA IS PROCESSED """

STG_EVENTS_COPY = ("""copy stg_events 
                          from {}
                          IAM_ROLE {}
                          json {};
                       """).format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

""" SONG DATA IS PROCESSED """

STG_SONGS_COPY = ("""copy stg_songs 
                          from {} 
                          IAM_ROLE {}
                          json 'auto';
                      """).format(SONG_DATA, IAM_ROLE)

"""FACT TABLE: SONGPLAYS: DATA LOAD INTO THE FACT TABLE """

SONGPLAY_TABLE_INSERT = ("""INSERT INTO SONGPLAY
                                (START_TIME, 
                                USER_ID, 
                                LEVEL, 
                                SONG_ID, 
                                ARTIST_ID, 
                                SESSION_ID,
                                LOCATION, 
                                USER_AGENT)
                            SELECT  
                                TIMESTAMP 'EPOCH' + SE.TS/1000 * INTERVAL '1 SECOND' AS START_TIME, 
                                SE.USER_ID, 
                                SE.LEVEL, 
                                SS.SONG_ID,
                                SS.ARTIST_ID, 
                                SE.SESSION_ID,
                                SE.LOCATION,
                                SE.USER_AGENT
                            FROM 
                                STG_EVENTS SE, 
                                STG_SONGS SS
                            WHERE
                                UPPER(SE.PAGE) = 'NEXTSONG' AND
                                SE.SONG =SS.TITLE AND
                                SE.ARTIST = SS.ARTIST_NAME AND
                                SE.LENGTH = SS.DURATION
""")

"""Dimension TABLE: USERS: DATA LOAD INTO THE Dimension TABLE """

USER_TABLE_INSERT = ("""INSERT INTO USERS(USER_ID, FIRST_NAME, LAST_NAME, GENDER, LEVEL)
                        SELECT DISTINCT  USER_ID, FIRST_NAME, LAST_NAME, GENDER, LEVEL
                        FROM STG_EVENTS
                        WHERE UPPER(PAGE) = 'NEXTSONG'
""")

"""Dimension TABLE: SONG: DATA LOAD INTO THE Dimension TABLE """

SONG_TABLE_INSERT = ("""INSERT INTO SONG
                        (SONG_ID, 
                        TITLE, 
                        ARTIST_ID,
                        YEAR, 
                        DURATION
                        )
                        SELECT
                            SONG_ID, 
                            TITLE, 
                            ARTIST_ID,
                            YEAR,
                            DURATION
                        FROM 
                            STG_SONGS
                        WHERE 
                            SONG_ID IS NOT NULL
""")

"""Dimension TABLE: ARTIST: DATA LOAD INTO THE Dimension TABLE """

ARTIST_TABLE_INSERT = ("""INSERT INTO ARTIST
                            (
                                ARTIST_ID,
                                NAME, 
                                LOCATION,
                                LATITUDE,
                                LONGITUDE)
                          SELECT 
                                DISTINCT ARTIST_ID, 
                                ARTIST_NAME, 
                                ARTIST_LOCATION,
                                ARTIST_LATITUDE, 
                                ARTIST_LONGITUDE 
                          FROM 
                                STG_SONGS
                          WHERE
                                ARTIST_ID IS NOT NULL
""")

"""Dimension TABLE: TIME: DATA LOAD INTO THE Dimension TABLE """

TIME_TABLE_INSERT = ("""INSERT INTO TIME
                            (START_TIME,
                                HOUR, 
                                DAY, 
                                WEEK, 
                                MONTH,
                                YEAR, 
                                WEEKDAY)
                        SELECT 
                            START_TIME,
                            EXTRACT(HOUR FROM START_TIME), 
                            EXTRACT(DAY FROM START_TIME),
                            EXTRACT(WEEK FROM START_TIME), 
                            EXTRACT(MONTH FROM START_TIME),
                            EXTRACT(YEAR FROM START_TIME),
                            EXTRACT(DAYOFWEEK FROM START_TIME)
                        FROM 
                            SONGPLAY
""")

""" Below are the Table query list that will be used for creating dropping and loading the data."""

create_table_queries = [STG_EVENTS_TABLE_CREATE, STG_SONGS_TABLE_CREATE, SONGPLAY_TABLE_CREATE, USER_TABLE_CREATE,
                        SONG_TABLE_CREATE, ARTIST_TABLE_CREATE, TIME_TABLE_CREATE]
drop_table_queries = [STG_EVENTS_TABLE_DROP, STG_SONGS_TABLE_DROP, SONGPLAY_TABLE_DROP, USER_TABLE_DROP,
                      SONG_TABLE_DROP, ARTIST_TABLE_DROP, TIME_TABLE_DROP]
copy_table_queries = [STG_EVENTS_COPY, STG_SONGS_COPY]
insert_table_queries = [SONGPLAY_TABLE_INSERT, USER_TABLE_INSERT, SONG_TABLE_INSERT, ARTIST_TABLE_INSERT,
                        TIME_TABLE_INSERT]
