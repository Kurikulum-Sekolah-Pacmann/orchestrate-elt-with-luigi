INSERT INTO prod.dim_product (
    product_id,
    product_nk,
    "name",
    price,
    stock,
    category_name,
    category_desc,
    subcategory_name,
    subcategory_desc,
    created_at,
    updated_at
)

SELECT
    p.id AS product_id,
    p.product_id AS product_nk,
    p."name",
    p.price,
    p.stock,
    c."name" AS category_name,
    c.description AS category_desc,
    s."name" AS subcategory_name,
    s.description AS subcategory_desc,
    p.created_at,
    p.updated_at
FROM
    stg.product p
    
INNER JOIN
    stg.subcategory s ON p.subcategory_id = s.subcategory_id
INNER JOIN
    stg.category c ON s.category_id = c.category_id
    
ON CONFLICT(product_id) 
DO UPDATE SET
    product_nk = EXCLUDED.product_nk,
    name = EXCLUDED.name,
    price = EXCLUDED.price,
    stock = EXCLUDED.stock,
    category_name = EXCLUDED.category_name,
    category_desc = EXCLUDED.category_desc,
    subcategory_name = EXCLUDED.subcategory_name,
    subcategory_desc = EXCLUDED.subcategory_desc,
    updated_at = CASE WHEN 
                        prod.dim_product.product_nk <> EXCLUDED.product_nk
                        OR prod.dim_product.name <> EXCLUDED.name 
                        OR prod.dim_product.price <> EXCLUDED.price 
                        OR prod.dim_product.stock <> EXCLUDED.stock 
                        OR prod.dim_product.category_name <> EXCLUDED.category_name 
                        OR prod.dim_product.category_desc <> EXCLUDED.category_desc 
                        OR prod.dim_product.subcategory_name <> EXCLUDED.subcategory_name 
                        OR prod.dim_product.subcategory_desc <> EXCLUDED.subcategory_desc 
                THEN 
                        '{current_local_time}'
                ELSE
                        prod.dim_product.updated_at
                END;