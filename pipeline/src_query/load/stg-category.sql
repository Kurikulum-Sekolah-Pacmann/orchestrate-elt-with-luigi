INSERT INTO stg.category 
    (category_id, name, description, created_at, updated_at) 

SELECT
    category_id, 
    name, 
    description, 
    created_at, 
    updated_at

FROM public.category

ON CONFLICT(category_id) 
DO UPDATE SET
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;