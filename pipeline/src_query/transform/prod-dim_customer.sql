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