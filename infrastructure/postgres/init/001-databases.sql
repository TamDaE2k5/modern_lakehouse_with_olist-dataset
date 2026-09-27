\set ON_ERROR_STOP on

\getenv iceberg_password ICEBERG_DB_PASSWORD
\getenv airflow_password AIRFLOW_DB_PASSWORD
\getenv superset_password SUPERSET_DB_PASSWORD

SELECT 'CREATE ROLE iceberg LOGIN'
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'iceberg')\gexec
SELECT format('ALTER ROLE iceberg WITH LOGIN PASSWORD %L', :'iceberg_password')\gexec

SELECT 'CREATE ROLE airflow LOGIN'
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow')\gexec
SELECT format('ALTER ROLE airflow WITH LOGIN PASSWORD %L', :'airflow_password')\gexec

SELECT 'CREATE ROLE superset LOGIN'
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'superset')\gexec
SELECT format('ALTER ROLE superset WITH LOGIN PASSWORD %L', :'superset_password')\gexec

SELECT 'CREATE DATABASE iceberg OWNER iceberg'
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_database WHERE datname = 'iceberg')\gexec
SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_database WHERE datname = 'airflow')\gexec
SELECT 'CREATE DATABASE superset OWNER superset'
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_database WHERE datname = 'superset')\gexec

REVOKE CREATE ON SCHEMA public FROM PUBLIC;

\connect iceberg
SET ROLE iceberg;

CREATE TABLE IF NOT EXISTS iceberg_tables (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location VARCHAR(1000),
    previous_metadata_location VARCHAR(1000),
    iceberg_type VARCHAR(5),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);

CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
    catalog_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    property_key VARCHAR(255) NOT NULL,
    property_value VARCHAR(1000),
    PRIMARY KEY (catalog_name, namespace, property_key)
);

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
RESET ROLE;
