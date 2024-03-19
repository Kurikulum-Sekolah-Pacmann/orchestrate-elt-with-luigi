INSERT INTO stg.category 
    (category_id, name, description, created_at, updated_at) 
VALUES 
    ('{category_id}', '{name}', '{description}', '{created_at}', '{updated_at}')
ON CONFLICT(category_id) 
DO UPDATE SET
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    updated_at = CASE WHEN 
                        stg.category.name <> EXCLUDED.name 
                        OR stg.category.description <> EXCLUDED.description 
                THEN 
                        '{current_local_time}'
                ELSE
                        stg.category.updated_at
                END;