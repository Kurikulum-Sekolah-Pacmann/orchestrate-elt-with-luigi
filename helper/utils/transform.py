from db_conn import db_connection
import warnings
warnings.filterwarnings('ignore')
from datetime import datetime
import luigi

class Transform(luigi.Task):
    
    def run(self):
        try:
            # Establish connections to source and DWH databases
            _, _, conn_dwh, cur_dwh = db_connection()
            current_local_time = datetime.now()
            
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
                                    '{current_local_time}'
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
                                    '{current_local_time}'
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
                    THEN '{current_local_time}' 
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
    luigi.build([Transform()], local_scheduler=True)