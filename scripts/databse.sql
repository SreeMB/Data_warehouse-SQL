-- =============================================
-- Script to Drop and Recreate 'DataWarehouse' Database
-- Purpose: This script ensures that the 'DataWarehouse' database
--          is dropped if it exists and then recreated with
--          three schemas: Bronze, Silver, and Gold.
--
-- WARNING:
-- - Dropping the database will permanently delete all data!
-- - Ensure you have a backup before executing this script.
-- - All active connections will be terminated.
-- =============================================


use master;
go

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN 
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO 

-- Create Database
create Database DataWarehouse;
use DataWarehouse;

-- Creation of the Schemas 
create schema bronze;
go
create schema silver;
go
create schema gold;
go
