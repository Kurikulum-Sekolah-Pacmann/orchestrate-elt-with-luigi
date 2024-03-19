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