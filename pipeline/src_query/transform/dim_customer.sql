INSERT INTO final.dim_customer (
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
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;