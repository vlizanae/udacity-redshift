import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
        Drops all tables if present (staging and analytical).
    '''
    for query in drop_table_queries:
        print('> Executing:')
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
        Creates all the table for the project (staging and analytical).
    '''
    for query in create_table_queries:
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

    print(' -> Dropping tables...')
    drop_tables(cur, conn)
    print(' -> Creating tables...')
    create_tables(cur, conn)

    print(' -> Done.')
    conn.close()


if __name__ == "__main__":
    main()