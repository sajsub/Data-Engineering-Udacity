import configparser
import psycopg2
import datetime
from sql_queries import create_table_queries, drop_table_queries

currentDT = datetime.datetime.now()

""" Executing create_table.py ; this process creates the tables and loads the information """

def drop_tables(cur, conn):
    for query in drop_table_queries:
        print (currentDT.strftime("%Y-%m-%d %H:%M:%S"))
        print('Dropping tables: '+query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        print (currentDT.strftime("%Y-%m-%d %H:%M:%S"))
        print('Creating tables: '+query)
        cur.execute(query)
        conn.commit()

""" Executing main block  """
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print (currentDT.strftime("%Y-%m-%d %H:%M:%S"))
    print('Connecting to the database:')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print (currentDT.strftime("%Y-%m-%d %H:%M:%S"))
    print('Closing connection:')

if __name__ == "__main__":
    main()