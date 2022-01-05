import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    ''' Performs load into the staging table.'''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    ''' Extract, transform and load into the dimension and fact tables.'''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    ''' Main function that connects to the Redshift cluster DB and calls the above 2 functions.'''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    

    conn.close()


if __name__ == "__main__":
    main()