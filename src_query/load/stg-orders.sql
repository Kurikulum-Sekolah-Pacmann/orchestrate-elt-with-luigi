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