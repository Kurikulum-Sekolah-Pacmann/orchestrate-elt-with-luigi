INSERT INTO stg.subcategory 
    (subcategory_id, name, category_id, description) 

SELECT
    subcategory_id, 
    name, 
    category_id, 
    description

FROM public.subcategory

ON CONFLICT(subcategory_id) 
DO UPDATE SET
    subcategory_id = EXCLUDED.subcategory_id,
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    updated_at = CASE WHEN 
                        stg.subcategory.subcategory_id <> EXCLUDED.subcategory_id
                        OR stg.subcategory.name <> EXCLUDED.name
                        OR stg.subcategory.description <> EXCLUDED.description
                THEN 
                        CURRENT_TIMESTAMP
                ELSE
                        stg.subcategory.updated_at
                END;