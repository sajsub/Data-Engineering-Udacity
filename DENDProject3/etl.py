import configparser
import psycopg2
import datetime
from sql_queries import copy_table_queries, insert_table_queries

currentDT = datetime.datetime.now()

""" Block for loading the data from S3 to staging tables"""


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print (currentDT.strftime("%Y-%m-%d %H:%M:%S"))
        print('copying tables: ' + query)
        cur.execute(query)
        conn.commit()


""" Block for loading the data loading the data"""


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print (currentDT.strftime("%Y-%m-%d %H:%M:%S"))
        print('insert tables: ' + query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    print('Reading the configuration file:')
    config.read('dwh.cfg')
    print('Connecting to the database:')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Table load:')
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print('Closing all connections')
    conn.close()
    print (currentDT.strftime("%Y-%m-%d %H:%M:%S"))
    print('Data processing is completed:')


if __name__ == "__main__":
    main()