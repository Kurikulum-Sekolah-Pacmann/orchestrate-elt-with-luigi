INSERT INTO stg.product 
    (product_id, name, subcategory_id, price, stock, created_at, updated_at) 

SELECT
    product_id, 
    name, 
    subcategory_id, 
    price, 
    stock, 
    created_at, 
    updated_at

FROM public.product

ON CONFLICT(product_id) 
DO UPDATE SET
    name = EXCLUDED.name,
    subcategory_id = EXCLUDED.subcategory_id,
    price = EXCLUDED.price,
    stock = EXCLUDED.stock,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;