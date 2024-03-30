INSERT INTO final.dim_product (
    product_id,
    product_nk,
    "name",
    price,
    stock,
    category_name,
    category_desc,
    subcategory_name,
    subcategory_desc
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
    s.description AS subcategory_desc
FROM
    stg.product p
    
JOIN stg.subcategory s 
    ON p.subcategory_id = s.subcategory_id
JOIN stg.category c 
    ON s.category_id = c.category_id

ON CONFLICT(product_id) 
DO UPDATE SET
    product_nk = EXCLUDED.product_nk,
    "name" = EXCLUDED."name",
    price = EXCLUDED.price,
    stock = EXCLUDED.stock,
    category_name = EXCLUDED.category_name,
    category_desc = EXCLUDED.category_desc,
    subcategory_name = EXCLUDED.subcategory_name,
    subcategory_desc = EXCLUDED.subcategory_desc,
    updated_at = CASE WHEN 
                        final.dim_product.product_nk <> EXCLUDED.product_nk
                        OR final.dim_product.name <> EXCLUDED.name
                        OR final.dim_product.price <> EXCLUDED.price
                        OR final.dim_product.stock <> EXCLUDED.stock
                        OR final.dim_product.category_name <> EXCLUDED.category_name
                        OR final.dim_product.category_desc <> EXCLUDED.category_desc
                        OR final.dim_product.subcategory_name <> EXCLUDED.subcategory_name
                        OR final.dim_product.subcategory_desc <> EXCLUDED.subcategory_desc
                THEN 
                        CURRENT_TIMESTAMP
                ELSE
                        final.dim_product.updated_at
                END;