--CRIA O BANCO DE DADOS
USE [master]
GO

DECLARE @defaultDataPath NVARCHAR(MAX) = CONVERT(VARCHAR(MAX), SERVERPROPERTY('InstanceDefaultDataPath'))
DECLARE @defaultLogPath NVARCHAR(MAX) = CONVERT(VARCHAR(MAX), SERVERPROPERTY('InstanceDefaultLogPath'))
DECLARE @sql NVARCHAR(MAX) = 

'CREATE DATABASE [TCCPUCDW]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N''TCCPUCDW'', FILENAME = N''' + @defaultDataPath + 'TCCPUCDW.mdf'' , SIZE = 204800KB , MAXSIZE = UNLIMITED, FILEGROWTH = 10000KB )
 LOG ON 
( NAME = N''TCCPUCDW_LOG'', FILENAME = N''' + @defaultLogPath + 'TCCPUCDW_LOG.ldf'' , SIZE = 663552KB , MAXSIZE = 2048GB , FILEGROWTH = 10000KB) '

PRINT @sql
EXEC (@sql)

--ALTERA O MODO PARA RECOVERY SIMPLE
ALTER DATABASE TCCPUCDW SET RECOVERY SIMPLE
GO

--CRIA O LOGIN UTILIZADO DENTRO DO CÓDIGO PYTHON
CREATE LOGIN [PUCDW_APP] WITH PASSWORD=N'PucTCC2023', DEFAULT_DATABASE=[TCCPUCDW], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
GO

--ACESSA O BANCO DE DADOS CRIADO
USE TCCPUCDW
GO

--CRIA O USUÁRIO DENTRO DO BANCO DE DADOS
CREATE USER [PUCDW_APP] FOR LOGIN [PUCDW_APP] WITH DEFAULT_SCHEMA=[PUCDW]
GO
--USUÁRIO SÓ POSUIRÁ PERMISSÃO DE LEITURA E ESCRITA
ALTER ROLE [DB_DATAREADER] ADD MEMBER [PUCDW_APP]
GO
ALTER ROLE [DB_DATAWRITER] ADD MEMBER [PUCDW_APP]
GO

--CRIA O SCHEMA QUE ARMAZENARÁ AS TABELAS
CREATE SCHEMA PUCDW
GO

--TABELAS DE DIMENSÃO. 
--TODOS OS CAMPOS POSSUEM O MESMO NOME DO CAMPO NO DATASET DISPONIBILIZADO.
CREATE TABLE [PUCDW].TARGET ( 
    TARGET_ID INT IDENTITY(1,1) PRIMARY KEY,  
    TARGTYPE1_TXT VARCHAR(255), 
    CONSTRAINT CONS_TARGET_UNIQUE UNIQUE (TARGTYPE1_TXT) 
)
GO
CREATE INDEX TARGET_IDX ON [PUCDW].TARGET (TARGTYPE1_TXT)
GO

CREATE TABLE [PUCDW].ATTACK ( 
    ATTACK_ID INT IDENTITY(1,1) PRIMARY KEY,  
    ATTACKTYPE1_TXT VARCHAR(255), 
    CONSTRAINT CONS_ATTACK_UNIQUE UNIQUE (ATTACKTYPE1_TXT) 
)
GO
CREATE INDEX ATTACK_IDX ON [PUCDW].ATTACK (ATTACKTYPE1_TXT)
GO

CREATE TABLE [PUCDW].REGION ( 
    REGION_ID INT IDENTITY(1,1) PRIMARY KEY,  
    REGION_TXT VARCHAR(255), 
    COUNTRY_TXT VARCHAR(255), 
    PROVSTATE VARCHAR(255), 
    CITY VARCHAR(255),
	LATITUDE VARCHAR(255),
	LONGITUDE VARCHAR(255)
    CONSTRAINT CONS_REGION_UNIQUE UNIQUE (REGION_TXT, COUNTRY_TXT, PROVSTATE, CITY, LATITUDE, LONGITUDE) 
)
GO
CREATE INDEX REGION_IDX ON [PUCDW].REGION (REGION_TXT,COUNTRY_TXT,PROVSTATE,CITY,LATITUDE,LONGITUDE)
GO

CREATE TABLE [PUCDW].WEAPON ( 
    WEAPON_ID INT IDENTITY(1,1) PRIMARY KEY,  
    WEAPTYPE1_TXT VARCHAR(255), 
    WEAPSUBTYPE1_TXT VARCHAR(255), 
    CONSTRAINT CONS_WEAPON_UNIQUE UNIQUE (WEAPTYPE1_TXT, WEAPSUBTYPE1_TXT) 
)
GO
CREATE INDEX WEAPON_IDX ON [PUCDW].WEAPON (WEAPTYPE1_TXT,WEAPSUBTYPE1_TXT)
GO

CREATE TABLE [PUCDW].GNAME ( 
    GNAME_ID INT IDENTITY(1,1) PRIMARY KEY,  
    GNAME VARCHAR(255), 
    GSUBNAME VARCHAR(255), 
    CONSTRAINT CONS_GNAME_UNIQUE UNIQUE (GNAME,GSUBNAME) 
)
GO
CREATE INDEX GNAME_IDX ON [PUCDW].GNAME (GNAME,GSUBNAME)
GO

CREATE TABLE [PUCDW].DATE (
    DATE_ID INT IDENTITY(1,1) PRIMARY KEY,
    IYEAR INT,
    IMONTH INT,
    IDAY INT,
    CONSTRAINT CONS_DATE_UNIQUE UNIQUE (IYEAR, IMONTH, IDAY)
)
GO
CREATE INDEX DATE_IDX ON [PUCDW].DATE (IYEAR,IMONTH,IDAY)
GO

--TABELA FATO
CREATE TABLE [PUCDW].EVENT (
    EVENT_ID INT IDENTITY(1,1) PRIMARY KEY,
    DATE_ID INT,
    REGION_ID INT,
    ATTACK_ID INT,
    TARGET_ID INT,
    GNAME_ID INT,
    WEAPON_ID INT,
    SUCCESS INT,   --STR NO PANDAS
    CLAIMED INT,   --STR NO PANDAS
    ISHOSTKID INT, --STR NO PANDAS
    NKILL INT,	   --STR NO PANDAS
    NWOUND INT,
    RANSOMPAID NUMERIC(38,2),
    PROPVALUE  NUMERIC(38,2)
)
GO

ALTER TABLE [PUCDW].EVENT ADD CONSTRAINT CONS_DATE_ID_FK FOREIGN KEY (DATE_ID) REFERENCES [PUCDW].DATE(DATE_ID)
GO
ALTER TABLE [PUCDW].EVENT ADD CONSTRAINT CONS_REGION_ID_FK FOREIGN KEY (REGION_ID) REFERENCES [PUCDW].REGION(REGION_ID)
GO
ALTER TABLE [PUCDW].EVENT ADD CONSTRAINT CONS_ATTACK_ID_FK FOREIGN KEY (ATTACK_ID) REFERENCES [PUCDW].ATTACK(ATTACK_ID)
GO
ALTER TABLE [PUCDW].EVENT ADD CONSTRAINT CONS_TARGET_ID_FK FOREIGN KEY (TARGET_ID) REFERENCES [PUCDW].TARGET(TARGET_ID)
GO
ALTER TABLE [PUCDW].EVENT ADD CONSTRAINT CONS_GNAME_ID_FK FOREIGN KEY (GNAME_ID) REFERENCES [PUCDW].GNAME(GNAME_ID)
GO
ALTER TABLE [PUCDW].EVENT ADD CONSTRAINT CONS_WEAPON_ID_FK FOREIGN KEY (WEAPON_ID) REFERENCES [PUCDW].WEAPON(WEAPON_ID)
GO
