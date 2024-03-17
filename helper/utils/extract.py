import luigi
import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
from db_conn import db_connection

load_dotenv()
EXTRACT_OUTPUT_DIR = os.getenv("EXTRACT_OUTPUT_DIR")

class Extract(luigi.Task):
    tables_to_extract = ['category', 'subcategory', 'customer', 'orders', 'order_detail', 'product']  # Define tables to extract

    def output(self):
        outputs = []
        for table_name in self.tables_to_extract:
            outputs.append(luigi.LocalTarget(f'{EXTRACT_OUTPUT_DIR}/{table_name}.csv'))
        return outputs

    def run(self):
        try:
            # Connect to PostgreSQL database
            conn_src, cur_src, conn_dwh, cur_dwh = db_connection()

            for table_name in self.tables_to_extract:
                # Query to select data from the table
                query = f"SELECT * FROM {table_name}"

                # Read data into DataFrame
                df = pd.read_sql_query(query, conn_src)

                # Write DataFrame to CSV
                df.to_csv(f"{EXTRACT_OUTPUT_DIR}/{table_name}.csv", index=False)

                print(f"Data from table '{table_name}' exported to '{table_name}.csv' successfully.")

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL:", error)

        finally:
            # Close database conn_src
            if conn_src:
                conn_src.close()

if __name__ == '__main__':
    # Run Extract
    luigi.build([Extract()], local_scheduler=True)
