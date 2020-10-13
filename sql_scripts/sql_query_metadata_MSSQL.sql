with cte as(
SELECT
    tc.table_schema,

    tc.constraint_name,

    tc.table_name,

    kcu.column_name,

    ccu.table_schema AS foreign_table_schema,

    ccu.table_name AS foreign_table_name,

    ccu.column_name AS foreign_column_name
FROM AdventureWorks.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
INNER JOIN AdventureWorks.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu on (tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME and tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA)
INNER JOIN AdventureWorks.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu on (ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME and ccu.TABLE_SCHEMA = tc.TABLE_SCHEMA)
WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
)
SELECT cc.TABLE_SCHEMA, cc.TABLE_NAME, cc.COLUMN_NAME, cc.DATA_TYPE, cc.IS_NULLABLE, cc.CHARACTER_MAXIMUM_LENGTH, cte.foreign_table_schema, cte.foreign_table_name, cte.foreign_column_name
FROM AdventureWorks.INFORMATION_SCHEMA.COLUMNS cc left join cte as cte on (cc.TABLE_SCHEMA = cte.TABLE_SCHEMA and cc.TABLE_NAME = cte.TABLE_NAME and cc.COLUMN_NAME = cte.COLUMN_NAME)