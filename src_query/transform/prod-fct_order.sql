INSERT INTO prod.fct_order (
    order_id,
    product_id,
    customer_id,
    date_id,
    quantity,
    status,
    created_at,
    updated_at
)

SELECT
    o.id AS order_id,
    dp.product_id,
    dc.customer_id,
    dd.date_id,
    od.quantity,
    o.status,
    od.created_at,
    od.updated_at
FROM
    stg.order_detail od
    
INNER JOIN 
    stg.orders o ON od.order_id = o.order_id
INNER JOIN 
    prod.dim_customer dc ON o.customer_id = dc.customer_nk
INNER JOIN 
    prod.dim_product dp on od.product_id = dp.product_nk 
INNER JOIN 
    prod.dim_date dd on o.order_date = dd.date_actual
    
ON CONFLICT (order_id, product_id, customer_id, date_id, quantity, status, created_at) 
DO UPDATE SET 
    order_id = EXCLUDED.order_id,
    product_id = EXCLUDED.product_id,
    customer_id = EXCLUDED.customer_id,
    date_id = EXCLUDED.date_id,
    quantity = EXCLUDED.quantity,
    status = EXCLUDED.status,
    created_at = EXCLUDED.created_at,
    updated_at = CASE 
        WHEN prod.fct_order.customer_id <> EXCLUDED.customer_id 
            OR prod.fct_order.date_id <> EXCLUDED.date_id 
            OR prod.fct_order.quantity <> EXCLUDED.quantity 
            OR prod.fct_order.status <> EXCLUDED.status 
            OR prod.fct_order.created_at <> EXCLUDED.created_at 
        THEN '{current_local_time}' 
        ELSE prod.fct_order.updated_at 
    END;