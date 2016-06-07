USE [taxi]
GO
CREATE LOGIN [RServicesUser] WITH PASSWORD='RServicesUser', CHECK_EXPIRATION=OFF,
CHECK_POLICY=OFF;
CREATE USER [RServicesUser] FOR LOGIN [RServicesUser] WITH DEFAULT_SCHEMA=[db_datareader]
ALTER ROLE [db_datareader] ADD MEMBER [RServicesUser]
ALTER ROLE [db_ddladmin] ADD MEMBER [RServicesUser]