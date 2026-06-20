SELECT schema_name
FROM information_schema.schemata;

SELECT schemaname, tablename
FROM svv_external_tables;

SELECT schemaname,
       tablename
FROM svv_external_tables
WHERE schemaname = 'spectrum';
