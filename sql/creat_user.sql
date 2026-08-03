USE [master];
GO

-- 1. Cria o login no servidor
CREATE LOGIN [powerbi_user] WITH PASSWORD = '123', CHECK_POLICY = OFF;
GO

USE [ArgentinaBD];
GO

-- 2. Cria o usuário dentro do banco ArgentinaBD
CREATE USER [powerbi_user] FOR LOGIN [powerbi_user];
GO

-- 3. Dá permissão de leitura total para esse usuário
ALTER ROLE [db_datareader] ADD MEMBER [powerbi_user];
GO