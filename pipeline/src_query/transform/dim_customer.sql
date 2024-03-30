INSERT INTO final.dim_customer (
    customer_id,
    customer_nk,
    first_name,
    last_name,
    email,
    phone,
    address
)

SELECT
    c.id AS customer_id,
    c.customer_id AS customer_nk,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.address
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
                        final.dim_customer.customer_nk <> EXCLUDED.customer_nk
                        OR final.dim_customer.first_name <> EXCLUDED.first_name
                        OR final.dim_customer.last_name <> EXCLUDED.last_name
                        OR final.dim_customer.email <> EXCLUDED.email
                        OR final.dim_customer.phone <> EXCLUDED.phone
                        OR final.dim_customer.address <> EXCLUDED.address
                THEN 
                        CURRENT_TIMESTAMP
                ELSE
                        final.dim_customer.updated_at
                END;