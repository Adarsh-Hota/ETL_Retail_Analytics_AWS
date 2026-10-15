-- DESTRUCTIVE: removes Redshift-local serving objects only.
-- This does not delete Glue Catalog tables, Iceberg metadata, S3 data, or any other AWS resource.

DROP SCHEMA IF EXISTS retail_serving CASCADE;

-- Unregister the Redshift external-schema reference after native objects are gone.
DROP SCHEMA IF EXISTS gold_lakehouse CASCADE;
