
SELECT
    t.name AS table_name
    , s.name AS schema_name
FROM sys.tables t
INNER JOIN sys.schemas s
ON t.schema_id = s.schema_id

UNION

SELECT
        v.name AS table_name
    , s.name AS schema_name
FROM sys.views v
INNER JOIN sys.schemas s
ON v.schema_id = s.schema_id

ORDER BY schema_name, table_name;
