/*
=============================================================
Create Database and Schemas
=============================================================
Purpose:
- checks if a database named 'DataWarehouse' already exists, and if so, drops it
- creates a new database named 'DataWarehouse'
- creates three new schemas: 'bronze', 'silver', and 'gold'
- creates a user-defined function to construct file paths for data sources (modular approach)
- creates a user-defined function to construct table names for different layers (modular approach)

WARNING: RUNNING THIS SCRIPT WILL DROP THE ENTIRE 'DataWarehouse' DATABASE IF IT EXISTS!
=============================================================
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

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas for different layers
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

/*
===============================================================================
Function: Get Source File Path
===============================================================================
WHY?
Centralize file path construction for source data files to improve maintainability
and reduce code duplication in data loading procedures.

HOW?
- accept source system (crm/erp) and filename as parameters
- build the complete file path using a configurable base path
- return the built file path as NVARCHAR

SAMPLE USAGE:

-- if base local path is 'E:\DEV\_PORTFOLIO\data-warehouse-project', then:

`SELECT dbo.GetSourceFilePath('source_crm', 'cust_info.csv')`

returns: E:\DEV\_PORTFOLIO\data-warehouse-project\datasets\source_crm\cust_info.csv

and 

`SELECT dbo.GetSourceFilePath('source_erp', 'CUST_AZ12.csv')`

returns: E:\DEV\_PORTFOLIO\data-warehouse-project\datasets\source_erp\CUST_AZ12.csv
===============================================================================
*/
CREATE OR ALTER FUNCTION dbo.GetSourceFilePath(
    @source_system NVARCHAR(50), 
    @filename NVARCHAR(100)
)
RETURNS NVARCHAR(500)
AS
BEGIN
    DECLARE
        @base_project_path NVARCHAR(255) = 'E:\DEV\_PORTFOLIO\data-warehouse-project', -- replace by your base local path
        @full_path NVARCHAR(500);
    
    SET @full_path = @base_project_path + '\datasets\' + @source_system + '\' + @filename;
    
    RETURN @full_path;
END;
GO
