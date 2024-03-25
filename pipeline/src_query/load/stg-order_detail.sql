INSERT INTO stg.order_detail 
    (order_detail_id, order_id, product_id, quantity, price, created_at, updated_at) 

SELECT
    order_detail_id, 
    order_id, 
    product_id, 
    quantity, 
    price, 
    created_at, 
    updated_at

FROM public.order_detail

ON CONFLICT(order_detail_id) 
DO UPDATE SET
    order_id =EXCLUDED.order_id,
    product_id =EXCLUDED.product_id,
    quantity =EXCLUDED.quantity,
    price =EXCLUDED.price,
    created_at =EXCLUDED.created_at,
    updated_at =EXCLUDED.updated_at;