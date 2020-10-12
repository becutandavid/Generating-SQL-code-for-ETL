with cte as(
SELECT

    tc.table_schema,

    tc.constraint_name,

    tc.table_name,

    kcu.column_name,

    ccu.table_schema AS foreign_table_schema,

    ccu.table_name AS foreign_table_name,

    ccu.column_name AS foreign_column_name

FROM

    information_schema.table_constraints AS tc

    JOIN information_schema.key_column_usage AS kcu

      ON tc.constraint_name = kcu.constraint_name

      AND tc.table_schema = kcu.table_schema

    JOIN information_schema.constraint_column_usage AS ccu

      ON ccu.constraint_name = tc.constraint_name

      AND ccu.table_schema = tc.table_schema

WHERE tc.constraint_type = 'FOREIGN KEY'
)
SELECT cc.table_schema, cc.table_name, cc.column_name, cc.udt_name, cc.is_nullable, cc.character_maximum_length, cte.foreign_table_schema, cte.foreign_table_name, cte.foreign_column_name
FROM information_schema.columns as cc left join cte as cte on (cc.table_schema = cte.table_schema and cc.table_name = cte.table_name and cte.column_name = cc.column_name)
WHERE cc.table_schema = 'platforma'