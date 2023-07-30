import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Process the `song_data/*` files that contain the song information 
    and `log_data/*` files that contain the information about the songs played by users.

    INPUTS: 
    cur (cursor): Cursor to insert rows to tables
    filepath (string): Filepath to folder 'data/song_data/' where songs data is recorded
    '''
  
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Process to copy tables from stagging to final schema
    
    INPUTS: 
    cur (cursor): Cursor to insert rows to tables
    filepath (string): Filepath to folder 'data/song_data/' where songs data is recorded
    '''

    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    '''
        Main function to perform the ETL process
        - First create the connection to the DB
        - Initialize the cursor
        - Process the songs
        - Process the logs

    '''
 
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    print('finish load')
    insert_tables(cur, conn)
    print('finish copy')
    conn.close()


if __name__ == "__main__":
    main()