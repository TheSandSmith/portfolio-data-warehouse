/*
=============================================================
Create Database and Schemas
=============================================================
Purpose:
- checks if a database named 'DataWarehouse' already exists, and if so, drops it
- creates a new database named 'DataWarehouse'
- creates three new schemas: 'bronze', 'silver', and 'gold'

WARNING: RUNNING THIS SCRIPT WILL DROP THE ENTIRE 'DataWarehouse' DATABASE IF IT EXISTS!
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
