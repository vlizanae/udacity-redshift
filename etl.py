import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
        Executes the queries in charge of staging the data onto
        Redshift from the json files in the S3 buckets.
    '''
    for query in copy_table_queries:
        print('> Executing:')
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
        Executes the queries in charge of restructuring data from
        the staging tables into the analytical tables on star schema.
    '''
    for query in insert_table_queries:
        print('> Executing:')
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print(' -> Connecting to cluster...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print(' -> Copying to staging tables...')
    load_staging_tables(cur, conn)
    print(' -> Inserting into analytical tables...')
    insert_tables(cur, conn)

    print(' -> Done.')
    conn.close()


if __name__ == "__main__":
    main()