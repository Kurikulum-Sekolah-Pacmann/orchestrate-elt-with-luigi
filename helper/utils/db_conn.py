import psycopg2
import warnings
warnings.filterwarnings('ignore')
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

def db_connection():
    """
    Establishes connections to source and data warehouse (DWH) databases.

    Returns:
        tuple: A tuple containing the following elements:
            - psycopg2.extensions.connection: Connection object for the source database.
            - psycopg2.extensions.cursor: Cursor object for the source database.
            - psycopg2.extensions.connection: Connection object for the data warehouse database.
            - psycopg2.extensions.cursor: Cursor object for the data warehouse database.

    Note:
        Make sure to handle exceptions properly in the calling code.
        This function assumes that PostgreSQL is running locally on default ports.
        Update the connection parameters (database, host, user, password, port)
        according to your PostgreSQL server configuration.
    """
    try:
        conn_src = psycopg2.connect(database=os.getenv("SRC_POSTGRES_DB"),
                                    host=os.getenv("SRC_POSTGRES_HOST"),
                                    user=os.getenv("SRC_POSTGRES_USER"),
                                    password=os.getenv("SRC_POSTGRES_PASSWORD"),
                                    port=os.getenv("SRC_POSTGRES_PORT"))
        cur_src = conn_src.cursor()

        conn_dwh = psycopg2.connect(database=os.getenv("DWH_POSTGRES_DB"),
                                    host=os.getenv("DWH_POSTGRES_HOST"),
                                    user=os.getenv("DWH_POSTGRES_USER"),
                                    password=os.getenv("DWH_POSTGRES_PASSWORD"),
                                    port=os.getenv("DWH_POSTGRES_PORT"))
        cur_dwh = conn_dwh.cursor()
        
        return conn_src, cur_src, conn_dwh, cur_dwh

    except Exception as e:
        print(f"Error: {e}")
        return None
