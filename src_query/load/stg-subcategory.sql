INSERT INTO stg.subcategory 
    (subcategory_id, name, category_id, description, created_at, updated_at) 
VALUES 
    ('{subcategory_id}', '{name}', '{category_id}', '{description}', '{created_at}', '{updated_at}')
ON CONFLICT(subcategory_id) 
DO UPDATE SET
    name = EXCLUDED.name,
    category_id = EXCLUDED.category_id,
    description = EXCLUDED.description,
    updated_at = CASE WHEN 
                        stg.subcategory.name <> EXCLUDED.name 
                        OR stg.subcategory.category_id <> EXCLUDED.category_id 
                        OR stg.subcategory.description <> EXCLUDED.description 
                THEN 
                        '{current_local_time}'
                ELSE
                        stg.subcategory.updated_at
                END;