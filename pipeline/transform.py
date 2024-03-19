import luigi
import logging
import pandas as pd
from datetime import datetime
from pipeline.load import Load
from pipeline.extract import Extract
from pipeline.utils.db_conn import db_connection
from pipeline.utils.read_sql import read_sql_file
from pipeline.utils.concat_dataframe import concat_dataframes
from pipeline.utils.copy_log import copy_log
from pipeline.utils.delete_files_in_directory import delete_files_in_directory
import time

class Transform(luigi.Task):
    current_local_time = datetime.now()
    
    def requires(self):
        return Load()
    
    def run(self):
        # Create summary for extract task
        timestamp_data = [datetime.now()]
        task_data = ['Transform']
        status_data = []
        execution_time_data = []
        
        try:
            
            # Configure logging
            logging.basicConfig(filename = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/log/logs.log', 
                                level = logging.INFO, 
                                format = '%(asctime)s - %(levelname)s - %(message)s')
               
            # Establish connections to source and DWH databases
            _, _, conn_dwh, cur_dwh = db_connection()
            
            # Define the query of each tables
            upsert_dim_customer_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/src_query/transform/prod-dim_customer.sql'
            )
            upsert_dim_product_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/src_query/transform/prod-dim_product.sql'
            )
            upsert_fct_order_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/src_query/transform/prod-fct_order.sql'
            )
            
            # Store all query in list
            all_query = [upsert_dim_customer_query, upsert_dim_product_query, upsert_fct_order_query]
            
            # Table name
            table_name = ['dim_customer', 'dim_product', 'fct_order']
            
            start_time = time.time()  # Record start time
            
            try:
                # Lopp throug each query
                for list_index in range(3):
                    try:
                        start_time = time.time()  # Record start time
                        
                        # Execute the upsert query
                        cur_dwh.execute(all_query[list_index].format(
                            current_local_time = self.current_local_time
                        ))
                        
                        # Commit the transaction
                        conn_dwh.commit()
                        
                        # Log success message
                        logging.info(f"Transform {table_name[list_index]} - SUCCESS")
                        
                    except Exception:
                        logging.info(f"Transform {table_name[list_index]} - FAILED")
                        print(f"Error Transforming data: {e}")
                    
                end_time = time.time()  # Record end time
                execution_time = end_time - start_time  # Calculate execution time
                
                # Get summary
                status_data.append('Success')
                execution_time_data.append(execution_time)
                
                # Get summary dict
                summary_data = {
                    'timestamp': timestamp_data,
                    'task': task_data,
                    'status' : status_data,
                    'execution_time': execution_time_data
                }
                
                # Get summary dataframes
                summary = pd.DataFrame(summary_data)
                
                # Write DataFrame to CSV
                summary.to_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data/transform-summary.csv', index = False)
        
            except Exception as e:
                start_time = time.time() # Record start time
                end_time = time.time()  # Record end time
                execution_time = end_time - start_time  # Calculate execution time
                
                # Get summary
                status_data.append('Success')
                execution_time_data.append(execution_time)
                
                # Get summary dict
                summary_data = {
                    'timestamp': timestamp_data,
                    'task': task_data,
                    'status' : status_data,
                    'execution_time': execution_time_data
                }
                
                # Get summary dataframes
                summary = pd.DataFrame(summary_data)
                
                # Write DataFrame to CSV
                summary.to_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data/transform-summary.csv', index = False)
        
                logging.info(f"Transform  - FAILED")
                    
            # Close the cursor and connection
            conn_dwh.close()
            cur_dwh.close()

        except Exception as e:
            print(f"Error during data transformation: {e}")
            
    def output(self):
        return [luigi.LocalTarget('/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/log/logs.log'),
                luigi.LocalTarget('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data/transform-summary.csv')]
            
# Execute the functions when the script is run
if __name__ == "__main__":
    luigi.build([Extract(),
                 Load(),
                 Transform()])
    
    concat_dataframes(
        df1 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline_summary.csv'),
        df2 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data/extract-summary.csv')
    )
    
    concat_dataframes(
        df1 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline_summary.csv'),
        df2 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data/load-summary.csv')
    )
    
    concat_dataframes(
        df1 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline_summary.csv'),
        df2 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data/transform-summary.csv')
    )
    
    copy_log(
        source_file = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/log/logs.log',
        destination_file = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/logs/logs.log'
    )
    
    delete_files_in_directory(
        directory = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data'
    )
    
    delete_files_in_directory(
        directory = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/log'
    )