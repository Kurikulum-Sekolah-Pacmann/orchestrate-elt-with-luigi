INSERT INTO stg.orders 
    (order_id, customer_id, order_date, status, created_at, updated_at)

SELECT
    order_id, 
    customer_id, 
    order_date, 
    status, 
    created_at, 
    updated_at

FROM public.orders

ON CONFLICT(order_id) 
DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    order_date = EXCLUDED.order_date,
    status = EXCLUDED.status,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;