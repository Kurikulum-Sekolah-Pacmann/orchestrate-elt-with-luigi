INSERT INTO stg.product 
    (product_id, name, subcategory_id, price, stock, created_at, updated_at) 
VALUES 
    ('{product_id}', '{name}', {subcategory_id}, {price}, {stock}, '{created_at}', '{updated_at}')
ON CONFLICT(product_id) 
DO UPDATE SET
    name = EXCLUDED.name,
    subcategory_id = EXCLUDED.subcategory_id,
    price = EXCLUDED.price,
    stock = EXCLUDED.stock,
    updated_at = CASE WHEN 
                        stg.product.name <> EXCLUDED.name 
                        OR stg.product.subcategory_id <> EXCLUDED.subcategory_id 
                        OR stg.product.price <> EXCLUDED.price 
                        OR stg.product.stock <> EXCLUDED.stock 
                        
                THEN 
                        '{current_local_time}'
                ELSE
                        stg.product.updated_at
                END;