INSERT INTO stg.order_detail 
    (order_detail_id, order_id, product_id, quantity, price) 

SELECT
    order_detail_id, 
    order_id, 
    product_id, 
    quantity, 
    price

FROM public.order_detail

ON CONFLICT(order_detail_id) 
DO UPDATE SET
    order_id =EXCLUDED.order_id,
    product_id =EXCLUDED.product_id,
    quantity =EXCLUDED.quantity,
    price =EXCLUDED.price,
    updated_at = CASE WHEN 
                        stg.order_detail.order_id <> EXCLUDED.order_id
                        OR stg.order_detail.product_id <> EXCLUDED.product_id
                        OR stg.order_detail.quantity <> EXCLUDED.quantity
                        OR stg.order_detail.price <> EXCLUDED.price
                THEN 
                        CURRENT_TIMESTAMP
                ELSE
                        stg.order_detail.updated_at
                END;