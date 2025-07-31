/* 
===========================
CREATE DATABASE AND SCHEMAS
===========================

-- Script Purpose:
  •  Create Database (DataWarehouse)
  •  Creae Schemas   (bronze, silver, gold)
    
*/


USE master;
GO

CREATE DATABASE DataWarehouse;
GO

--- Create the "DataWarehouse" database  
USE DataWarehouse;
GO

--- Create schemas   
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
