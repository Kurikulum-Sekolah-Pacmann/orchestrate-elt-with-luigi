import luigi
import psycopg2
import os
import pandas as pd
from helper.utils.db_conn import db_connection
from helper.utils.read_sql import read_sql_file
from datetime import datetime

class Extract(luigi.Task):
    tables_to_extract = ['category', 'subcategory', 'customer', 'orders', 'product', 'order_detail']
    
    def requires(self):
        pass

    def run(self):
        try:
            # Connect to PostgreSQL database
            conn_src, _, _, _ = db_connection()
            
            extract_query = read_sql_file(
                file_path = '/home/laode/pacmann/project/orchestrate-elt-with-luigi/src_query/extract/all-tables.sql'
            )

            for table_name in self.tables_to_extract:
                # Read data into DataFrame
                df = pd.read_sql_query(extract_query.format(table_name = table_name), conn_src)

                # Write DataFrame to CSV
                df.to_csv(f"/home/laode/pacmann/project/orchestrate-elt-with-luigi/helper/utils/temp_data/{table_name}.csv", index=False)

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL:", error)

        finally:
            # Close database conn_src
            if conn_src:
                conn_src.close()
                
    def output(self):
        outputs = []
        for table_name in self.tables_to_extract:
            outputs.append(luigi.LocalTarget(f'/home/laode/pacmann/project/orchestrate-elt-with-luigi/helper/utils/temp_data/{table_name}.csv'))
        return outputs


# --------------------------------------------------------------------------------------------------------------------------------------------------------                
class Load(luigi.Task):
    tables_to_extract = ['category', 'subcategory', 'customer', 'orders', 'product', 'order_detail']
    current_local_time = datetime.now()
    
    def requires(self):
        return Extract()
    
    def run(self):
        try:
            
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
            
            # Close the cursor and connection
            conn_dwh.close()
            cur_dwh.close()
            
        except Exception as e:
            print(f"Error loading data: {e}")
            
    def output(self):
        outputs = []
        for table_name in self.tables_to_extract:
            outputs.append(luigi.LocalTarget(f'/home/laode/pacmann/project/orchestrate-elt-with-luigi/helper/utils/temp_data/{table_name}.csv'))
        return outputs
            
class Transform(luigi.Task):
    current_local_time = datetime.now()
    
    def requires(self):
        return Load()
    
    def run(self):
        try:
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
            
            # Lopp throug each query
            for query in all_query:
                # Execute the upsert query
                cur_dwh.execute(query.format(
                    current_local_time = self.current_local_time
                ))
                
                # Commit the transaction
                conn_dwh.commit()
            
            # Close the cursor and connection
            conn_dwh.close()
            cur_dwh.close()

        except Exception as e:
            print(f"Error during data transformation: {e}")
            
# Execute the functions when the script is run
if __name__ == "__main__":
    luigi.build([Transform()])