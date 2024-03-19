INSERT INTO stg.order_detail 
    (order_detail_id, order_id, product_id, quantity, price, created_at, updated_at) 
VALUES 
    ('{order_detail_id}', '{order_id}', '{product_id}', '{quantity}', '{price}', '{created_at}', '{updated_at}')
ON CONFLICT(order_detail_id) 
DO UPDATE SET
    order_id = EXCLUDED.order_id,
    product_id = EXCLUDED.product_id,
    quantity = EXCLUDED.quantity,
    price = EXCLUDED.price,
    updated_at = CASE WHEN 
                        stg.order_detail.order_id <> EXCLUDED.order_id 
                        OR stg.order_detail.product_id <> EXCLUDED.product_id 
                        OR stg.order_detail.quantity <> EXCLUDED.quantity 
                        OR stg.order_detail.price <> EXCLUDED.price 
                THEN 
                        '{current_local_time}'
                ELSE
                        stg.order_detail.updated_at
                END;