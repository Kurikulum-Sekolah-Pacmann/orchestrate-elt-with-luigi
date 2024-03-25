INSERT INTO final.fct_order (
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
    LEAST(MIN(o.created_at), 
        MIN(dp.created_at),
        MIN(dc.created_at),
        MIN(od.created_at)) AS created_at,
    GREATEST(MAX(o.updated_at), 
            MAX(dp.updated_at),
            MAX(dc.updated_at),
            MAX(od.updated_at)) AS updated_at
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

GROUP BY
    o.id,
    dp.product_id,
    dc.customer_id,
    dd.date_id,
    od.quantity,
    o.status;
  