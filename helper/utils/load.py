from helper.utils.db_conn import db_connection
import pandas as pd
from datetime import datetime

current_local_time = datetime.now()

def load():
    """
    Load data from CSV files into the staging tables of the data warehouse (DWH).

    Returns:
    None

    Raises:
    Any exceptions raised by the underlying functions/db_connection, pd.read_csv,
    or database operations (INSERT queries and commits).
    """
    try:
        # Data to be loaded
        category = pd.read_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/category.csv')
        subcategory = pd.read_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/subcategory.csv')
        customer = pd.read_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/customer.csv')
        orders = pd.read_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/orders.csv')
        product = pd.read_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/product.csv')
        order_detail = pd.read_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/order_detail.csv')
        
        # Establish connections to source and DWH databases
        conn_src, cur_src, conn_dwh, cur_dwh = db_connection()
        
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
                                    '{current_local_time}'
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
                                    '{current_local_time}'
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
                                    '{current_local_time}'
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
                                    '{current_local_time}'
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
                                    '{current_local_time}'
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
                                    '{current_local_time}'
                            ELSE
                                    stg.order_detail.updated_at
                            END;
            """

            # Execute the INSERT query
            cur_dwh.execute(insert_query)

        # Commit the transaction
        conn_dwh.commit()
        
        # Close the cursor and connection
        conn_src.close()
        cur_src.close()
        conn_dwh.close()
        cur_dwh.close()
        
    except Exception as e:
        print(f"Error loading data: {e}")
        
# Execute the functions when the script is run
if __name__ == "__main__":
    load()