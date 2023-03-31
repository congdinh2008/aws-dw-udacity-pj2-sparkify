import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_schema_queries, drop_schema_queries


def drop_schemas(cur, conn):
    for query in drop_schema_queries:
        cur.execute(query)
        conn.commit()


def create_schemas(cur, conn):
    for query in create_schema_queries:
        cur.execute(query)
        conn.commit()

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        config.get('CLUSTER', "DWH_HOST"),
        config.get('CLUSTER', "DWH_DB"),
        config.get('CLUSTER', "DWH_DB_USER"),
        config.get('CLUSTER', "DWH_DB_PASSWORD"),
        config.get('CLUSTER', "DWH_PORT")
    ))
    cur = conn.cursor()

    drop_schemas(cur, conn)
    create_schemas(cur, conn)

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()