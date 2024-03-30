INSERT INTO final.fct_order (
    order_id,
    product_id,
    customer_id,
    date_id,
    quantity,
    status
)

SELECT
    od.id AS order_id,
    dp.product_id,
    dc.customer_id,
    dd.date_id,
    od.quantity,
    o.status
FROM
    stg.order_detail od

JOIN stg.orders o 
    ON od.order_id = o.order_id

JOIN final.dim_customer dc 
    ON o.customer_id = dc.customer_nk

JOIN final.dim_product dp 
    ON od.product_id = dp.product_nk
     
JOIN final.dim_date dd 
    ON o.order_date = dd.date_actual

ON CONFLICT(order_id) 
DO UPDATE SET
    product_id = EXCLUDED.product_id,
    customer_id = EXCLUDED.customer_id,
    date_id = EXCLUDED.date_id,
    quantity = EXCLUDED.quantity,
    status = EXCLUDED.status,
    updated_at = CASE WHEN 
                        final.fct_order.quantity <> EXCLUDED.quantity
                        OR final.fct_order.status <> EXCLUDED.status
                THEN 
                        CURRENT_TIMESTAMP
                ELSE
                        final.fct_order.updated_at
                END;