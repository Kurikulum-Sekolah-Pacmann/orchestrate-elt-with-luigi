INSERT INTO final.dim_product (
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
    LEAST(MIN(p.created_at), 
        MIN(c.created_at),
        MIN(s.created_at)) AS created_at,
    GREATEST(MAX(p.updated_at), 
            MAX(c.updated_at),
            MAX(s.updated_at)) AS updated_at
FROM
    stg.product p
    
JOIN stg.subcategory s 
    ON p.subcategory_id = s.subcategory_id
JOIN stg.category c 
    ON s.category_id = c.category_id
    
GROUP BY
    p.id,
    p.product_id,
    p."name",
    p.price,
    p.stock,
    c."name",
    c.description,
    s."name",
    s.description

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
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;