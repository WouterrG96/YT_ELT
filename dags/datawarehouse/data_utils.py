from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

# Base table name used across schemas (e.g., staging.yt_api, prod.yt_api)
table = "yt_api"


def get_conn_cursor():
    """
    Create and return a Postgres connection + cursor using an Airflow Postgres connection.
    Cursor uses RealDictCursor so rows come back as dict-like objects: row["col_name"].
    """
    # Grab connection details from Airflow's connection registry by ID
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")

    # Create a live psycopg2 connection object
    conn = hook.get_conn()

    # Create a cursor that returns rows as dictionaries (instead of tuples)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    return conn, cur


def close_conn_cursor(conn, cur):
    """Close cursor and connection to avoid leaking DB resources."""
    cur.close()
    conn.close()


def create_schema(schema):
    """
    Create a schema if it doesn't already exist.
    Example: create_schema("staging") -> ensures staging schema exists.
    """
    conn, cur = get_conn_cursor()

    # IF NOT EXISTS makes this operation idempotent (safe to run repeatedly)
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    # Execute DDL statement
    cur.execute(schema_sql)

    # Commit the transaction so the schema creation is persisted
    conn.commit()

    close_conn_cursor(conn, cur)


def create_table(schema):
    """
    Create the yt_api table in the given schema if it doesn't already exist.

    Note: staging schema uses slightly different types (Duration stored as VARCHAR).
    Non-staging schemas store Duration as TIME and add a Video_Type column.
    """
    conn, cur = get_conn_cursor()

    # Staging mirrors raw-ish ingestion (less strict typing, no derived fields)
    if schema == "staging":
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT   
                );
            """
    # Non-staging is a cleaned/curated model (stronger types + additional attribute)
    else:
        table_sql = f"""
                  CREATE TABLE IF NOT EXISTS {schema}.{table} (
                      "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                      "Video_Title" TEXT NOT NULL,
                      "Upload_Date" TIMESTAMP NOT NULL,
                      "Duration" TIME NOT NULL,
                      "Video_Type" VARCHAR(10) NOT NULL,
                      "Video_Views" INT,
                      "Likes_Count" INT,
                      "Comments_Count" INT    
                  ); 
              """

    # Create table (idempotent due to IF NOT EXISTS)
    cur.execute(table_sql)

    # Commit the transaction so the table (and its definition) is persisted
    conn.commit()

    close_conn_cursor(conn, cur)


def get_video_ids(cur, schema):
    """
    Fetch all Video_ID values from schema.yt_api using an existing cursor.
    Returns a plain Python list of strings.
    """
    # Select only the primary key column to keep data transfer minimal
    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")

    # With RealDictCursor, each row is a dict like {"Video_ID": "..."}
    ids = cur.fetchall()

    # Flatten dict rows into a simple list
    video_ids = [row["Video_ID"] for row in ids]

    return video_ids
