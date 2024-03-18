import luigi
import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
from helper.utils.db_conn import db_connection
from datetime import datetime

load_dotenv()
EXTRACT_OUTPUT_DIR = os.getenv("EXTRACT_OUTPUT_DIR")

class Extract(luigi.Task):
    tables_to_extract = ['category', 'subcategory', 'customer', 'orders', 'product', 'order_detail']
    
    def requires(self):
        pass

    def run(self):
        try:
            
            # Connect to PostgreSQL database
            conn_src, _, _, _ = db_connection()

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
                
    def output(self):
        outputs = []
        for table_name in self.tables_to_extract:
            outputs.append(luigi.LocalTarget(f'{EXTRACT_OUTPUT_DIR}/{table_name}.csv'))
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
            
            # Load to 'category' Table
            for index, row in category.iterrows():
                # Extract values from the DataFrame row
                category_id = row['category_id']
                name = row['name']
                description = row['description']
                created_at = row['created_at']
                updated_at = row['updated_at']

                # Construct the SQL INSERT query
                insert_query = f"""
                INSERT INTO stg.category 
                    (category_id, name, description, created_at, updated_at) 
                VALUES 
                    ('{category_id}', '{name}', '{description}', '{created_at}', '{updated_at}')
                ON CONFLICT(category_id) 
                DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    updated_at = CASE WHEN 
                                        stg.category.name <> EXCLUDED.name 
                                        OR stg.category.description <> EXCLUDED.description 
                                THEN 
                                        '{self.current_local_time}'
                                ELSE
                                        stg.category.updated_at
                                END;
                """

                # Execute the INSERT query
                cur_dwh.execute(insert_query)

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
                
                
                # Construct the SQL INSERT query
                insert_query = f"""
                INSERT INTO stg.subcategory 
                    (subcategory_id, name, category_id, description, created_at, updated_at) 
                VALUES 
                    ('{subcategory_id}', '{name}', '{category_id}', '{description}', '{created_at}', '{updated_at}')
                ON CONFLICT(subcategory_id) 
                DO UPDATE SET
                    name = EXCLUDED.name,
                    category_id = EXCLUDED.category_id,
                    description = EXCLUDED.description,
                    updated_at = CASE WHEN 
                                        stg.subcategory.name <> EXCLUDED.name 
                                        OR stg.subcategory.category_id <> EXCLUDED.category_id 
                                        OR stg.subcategory.description <> EXCLUDED.description 
                                THEN 
                                        '{self.current_local_time}'
                                ELSE
                                        stg.subcategory.updated_at
                                END;
                """

                # Execute the INSERT query
                cur_dwh.execute(insert_query)

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

                # Construct the SQL INSERT query
                insert_query = f"""
                INSERT INTO stg.customer 
                    (customer_id, first_name, last_name, email, phone, address, created_at, updated_at) 
                VALUES 
                    ('{customer_id}', '{first_name}', '{last_name}', '{email}', '{phone}', '{address}', '{created_at}', '{updated_at}')
                ON CONFLICT(customer_id) 
                DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    address = EXCLUDED.address,
                    updated_at = CASE WHEN 
                                        stg.customer.first_name <> EXCLUDED.first_name 
                                        OR stg.customer.last_name <> EXCLUDED.last_name 
                                        OR stg.customer.email <> EXCLUDED.email
                                        OR stg.customer.phone <> EXCLUDED.phone 
                                        OR stg.customer.address <> EXCLUDED.address 
                                THEN 
                                        '{self.current_local_time}'
                                ELSE
                                        stg.customer.updated_at
                                END;
                """

                # Execute the INSERT query
                cur_dwh.execute(insert_query)

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

                # Construct the SQL INSERT query
                insert_query = f"""
                INSERT INTO stg.orders 
                    (order_id, customer_id, order_date, status, created_at, updated_at) 
                VALUES 
                    ('{order_id}', '{customer_id}', '{order_date}', '{status}', '{created_at}', '{updated_at}')
                ON CONFLICT(order_id) 
                DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    order_date = EXCLUDED.order_date,
                    status = EXCLUDED.status,
                    updated_at = CASE WHEN 
                                        stg.orders.customer_id <> EXCLUDED.customer_id 
                                        OR stg.orders.order_date <> EXCLUDED.order_date 
                                        OR stg.orders.status <> EXCLUDED.status
                                THEN 
                                        '{self.current_local_time}'
                                ELSE
                                        stg.orders.updated_at
                                END;
                """

                # Execute the INSERT query
                cur_dwh.execute(insert_query)

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

                # Construct the SQL INSERT query
                insert_query = f"""
                INSERT INTO stg.product 
                    (product_id, name, subcategory_id, price, stock, created_at, updated_at) 
                VALUES 
                    ('{product_id}', '{name}', {subcategory_id}, {price}, {stock}, '{created_at}', '{updated_at}')
                ON CONFLICT(product_id) 
                DO UPDATE SET
                    name = EXCLUDED.name,
                    subcategory_id = EXCLUDED.subcategory_id,
                    price = EXCLUDED.price,
                    stock = EXCLUDED.stock,
                    updated_at = CASE WHEN 
                                        stg.product.name <> EXCLUDED.name 
                                        OR stg.product.subcategory_id <> EXCLUDED.subcategory_id 
                                        OR stg.product.price <> EXCLUDED.price 
                                        OR stg.product.stock <> EXCLUDED.stock 
                                        
                                THEN 
                                        '{self.current_local_time}'
                                ELSE
                                        stg.product.updated_at
                                END;
                """

                # Execute the INSERT query
                cur_dwh.execute(insert_query)

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

                # Construct the SQL INSERT query
                insert_query = f"""
                INSERT INTO stg.order_detail 
                    (order_detail_id, order_id, product_id, quantity, price, created_at, updated_at) 
                VALUES 
                    ('{order_detail_id}', '{order_id}', '{product_id}', '{quantity}', '{price}', '{created_at}', '{updated_at}')
                ON CONFLICT(order_detail_id) 
                DO UPDATE SET
                    order_id = EXCLUDED.order_id,
                    product_id = EXCLUDED.product_id,
                    quantity = EXCLUDED.quantity,
                    price = EXCLUDED.price,
                    updated_at = CASE WHEN 
                                        stg.order_detail.order_id <> EXCLUDED.order_id 
                                        OR stg.order_detail.product_id <> EXCLUDED.product_id 
                                        OR stg.order_detail.quantity <> EXCLUDED.quantity 
                                        OR stg.order_detail.price <> EXCLUDED.price 
                                THEN 
                                        '{self.current_local_time}'
                                ELSE
                                        stg.order_detail.updated_at
                                END;
                """

                # Execute the INSERT query
                cur_dwh.execute(insert_query)

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
            outputs.append(luigi.LocalTarget(f'{EXTRACT_OUTPUT_DIR}/{table_name}.csv'))
        return outputs
            
class Transform(luigi.Task):
    current_local_time = datetime.now()
    
    def requires(self):
        return Load()
    
    def run(self):
        try:
            # Establish connections to source and DWH databases
            _, _, conn_dwh, cur_dwh = db_connection()
            
            # dim_customer
            # Construct the SQL INSERT query
            insert_query = f"""
            INSERT INTO prod.dim_customer (
                customer_id,
                customer_nk,
                first_name,
                last_name,
                email,
                phone,
                address,
                created_at,
                updated_at
            )

            SELECT
                c.id AS customer_id,
                c.customer_id AS customer_nk,
                c.first_name,
                c.last_name,
                c.email,
                c.phone,
                c.address,
                c.created_at,
                c.updated_at
            FROM
                stg.customer c
                
            ON CONFLICT(customer_id) 
            DO UPDATE SET
                customer_nk = EXCLUDED.customer_nk,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                address = EXCLUDED.address,
                updated_at = CASE WHEN 
                                    prod.dim_customer.customer_nk <> EXCLUDED.customer_nk
                                    OR prod.dim_customer.first_name <> EXCLUDED.first_name 
                                    OR prod.dim_customer.last_name <> EXCLUDED.last_name 
                                    OR prod.dim_customer.email <> EXCLUDED.email 
                                    OR prod.dim_customer.phone <> EXCLUDED.phone 
                                    OR prod.dim_customer.address <> EXCLUDED.address 
                            THEN 
                                    '{self.current_local_time}'
                            ELSE
                                    prod.dim_customer.updated_at
                            END;
            """

            # Execute the INSERT query
            cur_dwh.execute(insert_query)

            # Commit the transaction
            conn_dwh.commit()
            
            # dim_product
            # Construct the SQL INSERT query
            insert_query = f"""
            INSERT INTO prod.dim_product (
                product_id,
                product_nk,
                "name",
                price,
                stock,
                category_name,
                category_desc,
                subcategory_name,
                subcategory_desc,
                created_at,
                updated_at
            )

            SELECT
                p.id AS product_id,
                p.product_id AS product_nk,
                p."name",
                p.price,
                p.stock,
                c."name" AS category_name,
                c.description AS category_desc,
                s."name" AS subcategory_name,
                s.description AS subcategory_desc,
                p.created_at,
                p.updated_at
            FROM
                stg.product p
                
            INNER JOIN
                stg.subcategory s ON p.subcategory_id = s.subcategory_id
            INNER JOIN
                stg.category c ON s.category_id = c.category_id
                
            ON CONFLICT(product_id) 
            DO UPDATE SET
                product_nk = EXCLUDED.product_nk,
                name = EXCLUDED.name,
                price = EXCLUDED.price,
                stock = EXCLUDED.stock,
                category_name = EXCLUDED.category_name,
                category_desc = EXCLUDED.category_desc,
                subcategory_name = EXCLUDED.subcategory_name,
                subcategory_desc = EXCLUDED.subcategory_desc,
                updated_at = CASE WHEN 
                                    prod.dim_product.product_nk <> EXCLUDED.product_nk
                                    OR prod.dim_product.name <> EXCLUDED.name 
                                    OR prod.dim_product.price <> EXCLUDED.price 
                                    OR prod.dim_product.stock <> EXCLUDED.stock 
                                    OR prod.dim_product.category_name <> EXCLUDED.category_name 
                                    OR prod.dim_product.category_desc <> EXCLUDED.category_desc 
                                    OR prod.dim_product.subcategory_name <> EXCLUDED.subcategory_name 
                                    OR prod.dim_product.subcategory_desc <> EXCLUDED.subcategory_desc 
                            THEN 
                                    '{self.current_local_time}'
                            ELSE
                                    prod.dim_product.updated_at
                            END;
            """

            # Execute the INSERT query
            cur_dwh.execute(insert_query)

            # Commit the transaction
            conn_dwh.commit()
            
            
            
            # fact_order
            # Construct the SQL INSERT query
            # Construct the SQL INSERT query
            insert_query = f"""
            INSERT INTO prod.fct_order (
                order_id,
                product_id,
                customer_id,
                date_id,
                quantity,
                status,
                created_at,
                updated_at
            )

            SELECT
                o.id AS order_id,
                dp.product_id,
                dc.customer_id,
                dd.date_id,
                od.quantity,
                o.status,
                od.created_at,
                od.updated_at
            FROM
                stg.order_detail od
                
            INNER JOIN 
                stg.orders o ON od.order_id = o.order_id
            INNER JOIN 
                prod.dim_customer dc ON o.customer_id = dc.customer_nk
            INNER JOIN 
                prod.dim_product dp on od.product_id = dp.product_nk 
            INNER JOIN 
                prod.dim_date dd on o.order_date = dd.date_actual
                
            ON CONFLICT (order_id, product_id, customer_id, date_id, quantity, status, created_at) 
            DO UPDATE SET 
                order_id = EXCLUDED.order_id,
                product_id = EXCLUDED.product_id,
                customer_id = EXCLUDED.customer_id,
                date_id = EXCLUDED.date_id,
                quantity = EXCLUDED.quantity,
                status = EXCLUDED.status,
                created_at = EXCLUDED.created_at,
                updated_at = CASE 
                    WHEN prod.fct_order.customer_id <> EXCLUDED.customer_id 
                        OR prod.fct_order.date_id <> EXCLUDED.date_id 
                        OR prod.fct_order.quantity <> EXCLUDED.quantity 
                        OR prod.fct_order.status <> EXCLUDED.status 
                        OR prod.fct_order.created_at <> EXCLUDED.created_at 
                    THEN '{self.current_local_time}' 
                    ELSE prod.fct_order.updated_at 
                END;
            """

            # Execute the INSERT query
            cur_dwh.execute(insert_query)

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