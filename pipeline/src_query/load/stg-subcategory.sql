INSERT INTO stg.subcategory 
    (subcategory_id, name, category_id, description, created_at, updated_at) 

SELECT
    subcategory_id, 
    name, 
    category_id, 
    description, 
    created_at, 
    updated_at

FROM public.subcategory

ON CONFLICT(subcategory_id) 
DO UPDATE SET
    name = EXCLUDED.name,
    category_id = EXCLUDED.category_id,
    description = EXCLUDED.description,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;