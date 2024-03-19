import luigi
import logging
import pandas as pd
import time
from datetime import datetime
from pipeline.extract import Extract
from pipeline.utils.db_conn import db_connection
from pipeline.utils.read_sql import read_sql_file
from pipeline.utils.concat_dataframe import concat_dataframes
from pipeline.utils.copy_log import copy_log

class Load(luigi.Task):
    tables_to_extract = ['category', 'subcategory', 'customer', 'orders', 'product', 'order_detail']
    current_local_time = datetime.now()
    
    def requires(self):
        return Extract()
    
    def run(self):
        # Create summary for extract task
        timestamp_data = [datetime.now()]
        task_data = ['Load']
        status_data = []
        execution_time_data = []
    
        # Establish connections to source and DWH databases
        try:
            _, _, conn_dwh, cur_dwh = db_connection()
            
        except Exception:
            raise Exception("Failed to connect to Data Warehouse")
        
        # Configure logging
        logging.basicConfig(filename = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/log/logs.log', 
                            level = logging.INFO, 
                            format = '%(asctime)s - %(levelname)s - %(message)s')
        
        # Data to be loaded
        try:
            category = pd.read_csv(self.input()[0].path)
            subcategory = pd.read_csv(self.input()[1].path)
            customer = pd.read_csv(self.input()[2].path)
            orders = pd.read_csv(self.input()[3].path)
            product = pd.read_csv(self.input()[4].path)
            order_detail = pd.read_csv(self.input()[5].path)

        except Exception:
            raise Exception("Failed to Read Extracted CSV")
        
        
        # Define the query of each tables
        try:
            upsert_category_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/src_query/load/stg-category.sql'
            )
            upsert_subcategory_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/src_query/load/stg-subcategory.sql'
            )
            upsert_customer_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/src_query/load/stg-customer.sql'
            )
            upsert_orders_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/src_query/load/stg-orders.sql'
            )
            upsert_product_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/src_query/load/stg-product.sql'
            )
            upsert_order_detail_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/src_query/load/stg-order_detail.sql'
            )
            
        except Exception:
            raise Exception("Failed to read SQL Query")
        
        start_time = time.time()  # Record start time
        
        # Load to Database
        try:
            # Load to 'category' Table
            for index, row in category.iterrows():
                # Extract values from the DataFrame row
                category_id = row['category_id']
                name = row['name']
                description = row['description']
                created_at = row['created_at']
                updated_at = row['updated_at']
            
                # Execute the upsert query
                cur_dwh.execute(upsert_category_query.format(
                    category_id = category_id,
                    name = name,
                    description = description,
                    created_at = created_at,
                    updated_at = updated_at,
                    current_local_time = self.current_local_time
                ))

            # Commit the transaction
            conn_dwh.commit()
            
            # Log success message
            logging.info(f"LOAD category - SUCCESS")

            # Load to 'subcategory' table
            for index, row in subcategory.iterrows():
                # Extract values from the DataFrame row
                subcategory_id = row['subcategory_id']
                name = row['name']
                category_id = row['category_id']
                description = row['description']
                created_at = row['created_at']
                updated_at = row['updated_at']

                # Execute the upsert query
                cur_dwh.execute(upsert_subcategory_query.format(
                    subcategory_id = subcategory_id,
                    name = name,
                    category_id = category_id,
                    description = description,
                    created_at = created_at,
                    updated_at = updated_at,
                    current_local_time = self.current_local_time
                ))

            # Commit the transaction
            conn_dwh.commit()
            
            # Log success message
            logging.info(f"LOAD subcategory - SUCCESS")
            
            # Load to 'customer' table
            for index, row in customer.iterrows():
                # Extract values from the DataFrame row
                customer_id = row['customer_id']
                first_name = row['first_name']
                last_name = row['last_name']
                email = row['email']
                phone = row['phone']
                address = row['address']
                created_at = row['created_at']
                updated_at = row['updated_at']

                # Execute the upsert query
                cur_dwh.execute(upsert_customer_query.format(
                    customer_id = customer_id,
                    first_name = first_name,
                    last_name = last_name,
                    email = email,
                    phone = phone,
                    address = address,
                    created_at = created_at,
                    updated_at = updated_at,
                    current_local_time = self.current_local_time
                ))

            # Commit the transaction
            conn_dwh.commit()
            
            # Log success message
            logging.info(f"LOAD customer - SUCCESS")
            
            # Load to 'orders' table
            for index, row in orders.iterrows():
                # Extract values from the DataFrame row
                order_id = row['order_id']
                customer_id = row['customer_id']
                order_date = row['order_date']
                status = row['status']
                created_at = row['created_at']
                updated_at = row['updated_at']

                # Execute the upsert query
                cur_dwh.execute(upsert_orders_query.format(
                    order_id = order_id,
                    customer_id = customer_id,
                    order_date = order_date,
                    status = status,
                    created_at = created_at,
                    updated_at = updated_at,
                    current_local_time = self.current_local_time
                ))

            # Commit the transaction
            conn_dwh.commit()
            
            # Log success message
            logging.info(f"LOAD orders - SUCCESS")
            
            # Load to 'product' table
            for index, row in product.iterrows():
                # Extract values from the DataFrame row
                product_id = row['product_id']
                name = row['name']
                subcategory_id = row['subcategory_id']
                price = row['price']
                stock = row['stock']
                created_at = row['created_at']
                updated_at = row['updated_at']

                # Execute the upsert query
                cur_dwh.execute(upsert_product_query.format(
                    product_id = product_id,
                    name = name,
                    subcategory_id = subcategory_id,
                    price = price,
                    stock = stock,
                    created_at = created_at,
                    updated_at = updated_at,
                    current_local_time = self.current_local_time
                ))

            # Commit the transaction
            conn_dwh.commit()
            
            # Log success message
            logging.info(f"LOAD product - SUCCESS")
            
            # Load to 'order_detail' table
            for index, row in order_detail.iterrows():
                # Extract values from the DataFrame row
                order_detail_id = row['order_detail_id']
                order_id = row['order_id']
                product_id = row['product_id']
                quantity = row['quantity']
                price = row['price']
                created_at = row['created_at']
                updated_at = row['updated_at']

                # Execute the upsert query
                cur_dwh.execute(upsert_order_detail_query.format(
                    order_detail_id = order_detail_id,
                    order_id = order_id,
                    product_id = product_id,
                    quantity = quantity,
                    price = price,
                    created_at = created_at,
                    updated_at = updated_at,
                    current_local_time = self.current_local_time
                ))

            # Commit the transaction
            conn_dwh.commit()
            
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
            summary.to_csv(f"/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data/load-summary.csv", index = False)
            
            # Log success message
            logging.info(f"LOAD order_detail - SUCCESS")
            
            # Close the cursor and connection
            conn_dwh.close()
            cur_dwh.close()
        
        except Exception:
            start_time = time.time() # Record start time
            end_time = time.time()  # Record end time
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            status_data.append('Failed')
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
            summary.to_csv(f"/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data/load-summary.csv", index = False)
            
            raise Exception('Failed to Load Tables')

    def output(self):
        return [luigi.LocalTarget(f'/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/log/logs.log'),
                luigi.LocalTarget(f'/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data/load-summary.csv')]
  
# Execute the functions when the script is run
if __name__ == "__main__":
    luigi.build([Extract(),
                 Load()])
    
    concat_dataframes(
        df1 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline_summary.csv'),
        df2 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data/extract-summary.csv')
    )
    
    concat_dataframes(
        df1 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline_summary.csv'),
        df2 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline/temp/data/load-summary.csv')
    )
    
    copy_log(
        source_file = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/log/logs.log',
        destination_file = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/logs/logs.log'
    )