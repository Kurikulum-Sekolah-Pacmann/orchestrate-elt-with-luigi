INSERT INTO stg.customer 
    (customer_id, first_name, last_name, email, phone, address, created_at, updated_at)

SELECT
    customer_id, 
    first_name, 
    last_name, 
    email, 
    phone, 
    address, 
    created_at, 
    updated_at

FROM public.customer

ON CONFLICT(customer_id) 
DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    address = EXCLUDED.address,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;