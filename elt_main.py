import luigi
import psycopg2
import os
import pandas as pd
from helper.utils.db_conn import db_connection
from helper.utils.read_sql import read_sql_file
from helper.utils.delete_files_in_directory import delete_files_in_directory
from helper.utils.copy_log import copy_log
from helper.utils.concat_dataframe import concat_dataframes
from datetime import datetime
import logging
import time


class Extract(luigi.Task):
    tables_to_extract = ['category', 'subcategory', 'customer', 'orders', 'product', 'order_detail']
    
    def requires(self):
        pass

    def run(self):
        try:
            # Configure logging
            logging.basicConfig(filename = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/log/logs.log', 
                                level = logging.INFO, 
                                format = '%(asctime)s - %(levelname)s - %(message)s')
            
            # Connect to PostgreSQL database
            conn_src, _, _, _ = db_connection()
            
            # Define the query using the SQL content
            extract_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/src_query/extract/all-tables.sql'
            )
            
            # Create summary for extract task
            timestamp_data = []
            tables_name_data = []
            task_data = []
            status_data = []
            execution_time_data = []
            
            for table_name in self.tables_to_extract:
                try:
                    start_time = time.time()  # Record start time
                    
                    # Read data into DataFrame
                    df = pd.read_sql_query(extract_query.format(table_name = table_name), conn_src)

                    # Write DataFrame to CSV
                    df.to_csv(f"/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/data/{table_name}.csv", index=False)
                    
                    end_time = time.time()  # Record end time
                    execution_time = end_time - start_time  # Calculate execution time
                    
                    # Get summary
                    timestamp_data.append(datetime.now())
                    tables_name_data.append(table_name)
                    task_data.append('Extract')
                    status_data.append('Success')
                    execution_time_data.append(execution_time)
                    
                    # Log success message
                    logging.info(f"EXTRACT '{table_name}' - SUCCESS.")
                    
                except (Exception, psycopg2.Error) as error:
                    print("Error while connecting to PostgreSQL:", error)
            
            # Get summary dict
            summary_data = {
                'timestamp': timestamp_data,
                'tables_name': tables_name_data,
                'task': task_data,
                'status' : status_data,
                'execution_time': execution_time_data
            }
            
            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write DataFrame to CSV
            summary.to_csv(f"/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/data/extract-summary.csv", index = False)
                    
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL:", error)

        finally:
            # Close database conn_src
            if conn_src:
                conn_src.close()
                
    def output(self):
        outputs = []
        for table_name in self.tables_to_extract:
            outputs.append(luigi.LocalTarget(f'/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/data/{table_name}.csv'))
            
        outputs.append(luigi.LocalTarget(f'/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/data/extract-summary.csv'))
            
        outputs.append(luigi.LocalTarget(f'/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/log/logs.log'))
        return outputs


# --------------------------------------------------------------------------------------------------------------------------------------------------------                
class Load(luigi.Task):
    tables_to_extract = ['category', 'subcategory', 'customer', 'orders', 'product', 'order_detail']
    current_local_time = datetime.now()
    
    def requires(self):
        return Extract()
    
    def run(self):
        try:
            
            # Configure logging
            logging.basicConfig(filename = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/log/logs.log', 
                                level = logging.INFO, 
                                format = '%(asctime)s - %(levelname)s - %(message)s')
            
            # Data to be loaded
            category = pd.read_csv(self.input()[0].path)
            subcategory = pd.read_csv(self.input()[1].path)
            customer = pd.read_csv(self.input()[2].path)
            orders = pd.read_csv(self.input()[3].path)
            product = pd.read_csv(self.input()[4].path)
            order_detail = pd.read_csv(self.input()[5].path)
            
            # Establish connections to source and DWH databases
            _, _, conn_dwh, cur_dwh = db_connection()
            
            # Define the query of each tables
            upsert_category_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/src_query/load/stg-category.sql'
            )
            upsert_subcategory_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/src_query/load/stg-subcategory.sql'
            )
            upsert_customer_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/src_query/load/stg-customer.sql'
            )
            upsert_orders_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/src_query/load/stg-orders.sql'
            )
            upsert_product_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/src_query/load/stg-product.sql'
            )
            upsert_order_detail_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/src_query/load/stg-order_detail.sql'
            )
            
            # Create summary for extract task
            timestamp_data = []
            tables_name_data = []
            task_data = []
            status_data = []
            execution_time_data = []
            
            
            # Load to 'category' Table
            start_time = time.time()  # Record start time
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
            
            end_time = time.time()  # Record end time
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            timestamp_data.append(datetime.now())
            tables_name_data.append('category')
            task_data.append('Load')
            status_data.append('Success')
            execution_time_data.append(execution_time)
            
            # Log success message
            logging.info(f"LOAD category - SUCCESS")


            # Load to 'subcategory' table
            start_time = time.time()  # Record start time
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
            
            end_time = time.time()  # Record end time
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            timestamp_data.append(datetime.now())
            tables_name_data.append('subcategory')
            task_data.append('Load')
            status_data.append('Success')
            execution_time_data.append(execution_time)
            
             # Log success message
            logging.info(f"LOAD subcategory - SUCCESS")
            
            
            # Load to 'customer' table
            start_time = time.time()  # Record start time
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
            
            end_time = time.time()  # Record end time
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            timestamp_data.append(datetime.now())
            tables_name_data.append('customer')
            task_data.append('Load')
            status_data.append('Success')
            execution_time_data.append(execution_time)
            
             # Log success message
            logging.info(f"LOAD customer - SUCCESS")
            
            
            # Load to 'orders' table
            start_time = time.time()  # Record start time
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
            
            end_time = time.time()  # Record end time
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            timestamp_data.append(datetime.now())
            tables_name_data.append('orders')
            task_data.append('Load')
            status_data.append('Success')
            execution_time_data.append(execution_time)
            
             # Log success message
            logging.info(f"LOAD orders - SUCCESS")
            
            
            # Load to 'product' table
            start_time = time.time()  # Record start time
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
            
            end_time = time.time()  # Record end time
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            timestamp_data.append(datetime.now())
            tables_name_data.append('product')
            task_data.append('Load')
            status_data.append('Success')
            execution_time_data.append(execution_time)
            
             # Log success message
            logging.info(f"LOAD product - SUCCESS")
            
            
            # Load to 'order_detail' table
            start_time = time.time()  # Record start time
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
            timestamp_data.append(datetime.now())
            tables_name_data.append('order_detail')
            task_data.append('Load')
            status_data.append('Success')
            execution_time_data.append(execution_time)
            
            # Get summary dict
            summary_data = {
                'timestamp': timestamp_data,
                'tables_name': tables_name_data,
                'task': task_data,
                'status' : status_data,
                'execution_time': execution_time_data
            }
            
            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write DataFrame to CSV
            summary.to_csv(f"/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/data/load-summary.csv", index = False)
            
             # Log success message
            logging.info(f"LOAD order_detail - SUCCESS")
            
            # Close the cursor and connection
            conn_dwh.close()
            cur_dwh.close()
            
        except Exception as e:
            print(f"Error loading data: {e}")
            
    def output(self):
        return [luigi.LocalTarget(f'/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/log/logs.log'),
                luigi.LocalTarget(f'/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/data/load-summary.csv')]
            
            
class Transform(luigi.Task):
    current_local_time = datetime.now()
    
    def requires(self):
        return Load()
    
    def run(self):
        try:
            
            # Configure logging
            logging.basicConfig(filename = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/log/logs.log', 
                                level = logging.INFO, 
                                format = '%(asctime)s - %(levelname)s - %(message)s')
               
            # Establish connections to source and DWH databases
            _, _, conn_dwh, cur_dwh = db_connection()
            
            # Define the query of each tables
            upsert_dim_customer_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/src_query/transform/prod-dim_customer.sql'
            )
            upsert_dim_product_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/src_query/transform/prod-dim_product.sql'
            )
            upsert_fct_order_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/src_query/transform/prod-fct_order.sql'
            )
            
            # Store all query in list
            all_query = [upsert_dim_customer_query, upsert_dim_product_query, upsert_fct_order_query]
            
            # Table name
            table_name = ['dim_customer', 'dim_product', 'fct_order']
            
            # Create summary for extract task
            timestamp_data = []
            tables_name_data = []
            task_data = []
            status_data = []
            execution_time_data = []
            
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
                    
                    end_time = time.time()  # Record end time
                    execution_time = end_time - start_time  # Calculate execution time
                    
                    # Get summary
                    timestamp_data.append(datetime.now())
                    tables_name_data.append(table_name[list_index])
                    task_data.append('Extract')
                    status_data.append('Success')
                    execution_time_data.append(execution_time)
                    
                    # Log success message
                    logging.info(f"Transform {table_name[list_index]} - SUCCESS")
                    
                except Exception as e:
                    print(f"Error Transforming data: {e}")
                    
            # Get summary dict
            summary_data = {
                'timestamp': timestamp_data,
                'tables_name': tables_name_data,
                'task': task_data,
                'status' : status_data,
                'execution_time': execution_time_data
            }
            
            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write DataFrame to CSV
            summary.to_csv(f"/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/data/transform-summary.csv", index = False)      
            
            # Close the cursor and connection
            conn_dwh.close()
            cur_dwh.close()

        except Exception as e:
            print(f"Error during data transformation: {e}")
            
    def output(self):
        return [luigi.LocalTarget(f'/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/log/logs.log'),
                luigi.LocalTarget(f'/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/data/transform-summary.csv')]
            
# Execute the functions when the script is run
if __name__ == "__main__":
    luigi.build([Extract(),
                 Load(),
                 Transform()])
    
    concat_dataframes(
        df1 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline_summary.csv'),
        df2 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/data/extract-summary.csv')
    )
    
    concat_dataframes(
        df1 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline_summary.csv'),
        df2 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/data/load-summary.csv')
    )
    
    concat_dataframes(
        df1 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/pipeline_summary.csv'),
        df2 = pd.read_csv('/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/data/transform-summary.csv')
    )
    
    copy_log(
        source_file = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/log/logs.log',
        destination_file = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/logs/logs.log'
    )
    
    delete_files_in_directory(
        directory = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/data/'
    )
    
    delete_files_in_directory(
        directory = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/temp/log/'
    )