INSERT INTO stg.category 
    (category_id, name, description) 

SELECT
    category_id, 
    name, 
    description

FROM public.category

ON CONFLICT(category_id) 
DO UPDATE SET
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    updated_at = CASE WHEN 
                        stg.category.name <> EXCLUDED.name
                        OR stg.category.description <> EXCLUDED.description
                THEN 
                        CURRENT_TIMESTAMP
                ELSE
                        stg.category.updated_at
                END;