-- =============================================================
-- Databricks (Unity Catalog)
-- Create Catalog and Schemas for Medallion Architecture
-- WARNING: Drops the entire catalog if it already exists (CASCADE)
-- =============================================================

-- Drop and recreate the 'DataWarehouse' catalog
DROP CATALOG IF EXISTS DataWarehouse CASCADE;

CREATE CATALOG DataWarehouse
COMMENT 'Top-level catalog for the Data Warehouse (bronze/silver/gold)';

-- Switch to the catalog
USE CATALOG DataWarehouse;

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Medallion bronze: raw ingestion';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Medallion silver: cleaned & conformed';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Medallion gold: curated & serving';

