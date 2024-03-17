import luigi
import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
from db_conn import db_connection

load_dotenv()
EXTRACT_OUTPUT_DIR = os.getenv("EXTRACT_OUTPUT_DIR")

class Extract(luigi.Task):
    table_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f'{EXTRACT_OUTPUT_DIR}{self.table_name}.csv')

    def run(self):
        try:
            # Connect to PostgreSQL database
            conn_src, cur_src, conn_dwh, cur_dwh = db_connection()

            # Query to select data from the table
            query = f"SELECT * FROM {self.table_name}"

            # Read data into DataFrame
            df = pd.read_sql_query(query, conn_src)

            # Write DataFrame to CSV
            df.to_csv(f"{EXTRACT_OUTPUT_DIR}{self.table_name}.csv", index = False)

            print(f"Data from table '{self.table_name}' exported to '{self.table_name}.csv' successfully.")

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL:", error)

        finally:
            # Close database conn_src
            if conn_src:
                conn_src.close()

if __name__ == '__main__':
    # Run Extract task
    luigi.build([Extract(table_name = 'category'),
                 Extract(table_name = 'subcategory'),
                 Extract(table_name = 'customer'),
                 Extract(table_name = 'orders'),
                 Extract(table_name = 'order_detail'),
                 Extract(table_name = 'product'),
                 Extract(table_name = 'subcategory')], local_scheduler = True)