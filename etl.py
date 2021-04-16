import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        This function loads utilizises the copy operation for the staging stage to copy data from s3 to the datawarehouse  
        
        Args:
               cur: database cursor for executing queries
               conn: connection for database

        Returns:
              None
        
    """
    for query in copy_table_queries:
        print('executing query', query)
        cur.execute(query)
        conn.commit()
        print('executed query.')


def insert_tables(cur, conn):
    """
        This function intserts data points into the tables of the redshift datawarehouse  
        
        Args:
               cur: database cursor for executing queries
               conn: connection for database

        Returns:
              None
        
    """
    for query in insert_table_queries:
        print('executing query', query)
        cur.execute(query)
        conn.commit()
        print('executed query.')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()