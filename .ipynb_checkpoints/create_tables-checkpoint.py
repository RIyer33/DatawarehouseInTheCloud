import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    ''' Drops tables in the Redshift DB if already present.'''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    ''' Create tables if they do not exist.'''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    ''' Main function that connects to AWS Redshift cluster and connects to database.
    
        also calls the functions to drop and create the tables in the database.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print ('connectin to db')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()