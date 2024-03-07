SELECT SERVERPROPERTY('IsPolyBaseInstalled') AS IsPolyBaseInstalled;


--  ######Create master key Encrytion########
CREATE MASTER KEY ENCRYPTION BY PASSWORD='lE@RNaZURt556!'

--#####Database Scoped Credential#####
--   ?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2022-10-14T08:43:46Z&st=2022-10-07T00:43:46Z&spr=https&sig=s5x2AYhgX3OTEf3%2BzI8lhFiu5UBv8eNJwriAY3lVAYE%3D
create DATABASE SCOPED CREDENTIAL [SLCred555]
WITH IDENTITY='SHARED ACCESS SIGNATURE',
SECRET='sp=racwdlmeop&st=2024-03-07T22:08:28Z&se=2024-03-30T05:08:28Z&sv=2022-11-02&sr=c&sig=3XYab9gkeXMJMOTM3G%2Fk4em1C35DvIYKqwxhuFrBuUQ%3D'
GO


---### Create Datasource ######################
EXEC sp_configure 'polybase enabled', 1
 RECONFIGURE

CREATE EXTERNAL DATA SOURCE [Datasource55]
WITH ( LOCATION = N'https://slgen.blob.core.windows.net/data',
CREDENTIAL=[SLCred555])


----########cREATE eXTERNAL fILE fORMAT ######
CREATE EXTERNAL FILE FORMAT FileFormat
WITH (
FORMAT_TYPE=PARQUET
)
GO

--- ####### CREATE EXT. TABLE ####
create EXTERNAL TABLE tbl_TaxiRidee(
 [DateID] int,
	 [MedallionID] int,
	 [HackneyLicenseID] int,
	 [PickupTimeID] int,
	 [DropoffTimeID] int,
	 [PickupGeographyID] int,
	 [DropoffGeographyID] int,
	 [PickupLatitude] float,
	 [PickupLongitude] float,
	 [PickupLatLong] nvarchar(4000),
	 [DropoffLatitude] float,
	 [DropoffLongitude] float,
	 [DropoffLatLong] nvarchar(4000),
	 [PassengerCount] int,
	 [TripDurationSeconds] int,
	 [TripDistanceMiles] float,
	 [PaymentType] nvarchar(4000),
	 [FareAmount] numeric(19,4),
	 [SurchargeAmount] numeric(19,4),
	 [TaxAmount] numeric(19,4),
	 [TipAmount] numeric(19,4),
	 [TollsAmount] numeric(19,4),
	 [TotalAmount] numeric(19,4)
)
WITH (
LOCATION='/*.parquet',
DATA_SOURCE=Datasource55,
FILE_FORMAT=FileFormat
);



SELECT * FROM tbl_TaxiRidee

SELECT COUNT(1)
FROM tbl_TaxiRidee


---CSV-----
----########cREATE eXTERNAL fILE fORMAT csv ######

--CREATE EXTERNAL FILE FORMAT FileFormatCSV
--WITH (FORMAT_TYPE = DELIMITEDTEXT,
--      FORMAT_OPTIONS(
--          FIELD_TERMINATOR = ',',
--          STRING_DELIMITER = '"',
--          FIRST_ROW = 2,
--          USE_TYPE_DEFAULT = True)
--);

--CREATE  EXTERNAL TABLE Cars_prod (
--	Make nvarchar(100),
--	Model nvarchar(200),
--	Type nvarchar(100),
--	Origin nvarchar(10),
--	DriveTrain nvarchar(100),
--	Length int null
--)

--WITH (
--LOCATION='/cars.csv',
--DATA_SOURCE=Datasource55,
--FILE_FORMAT=FileFormatCSV
--);


----######################## Second Half #####################################
---#### Creating Tables in MPP databases ..... Dedicated Pools)
A table with no index is called a Heap. Index improves I/O operations

--- Hash---
CREATE TABLE tbl_TaxiRide_Hash(
     [DateID] int,
	 [MedallionID] int,
	 [HackneyLicenseID] int,
	 [PickupTimeID] int,
	 [DropoffTimeID] int,
	 [PickupGeographyID] int,
	 [DropoffGeographyID] int,
	 [PickupLatitude] float,
	 [PickupLongitude] float,
	 [PickupLatLong] nvarchar(4000),
	 [DropoffLatitude] float,
	 [DropoffLongitude] float,
	 [DropoffLatLong] nvarchar(4000),
	 [PassengerCount] int,
	 [TripDurationSeconds] int,
	 [TripDistanceMiles] float,
	 [PaymentType] nvarchar(4000),
	 [FareAmount] numeric(19,4),
	 [SurchargeAmount] numeric(19,4),
	 [TaxAmount] numeric(19,4),
	 [TipAmount] numeric(19,4),
	 [TollsAmount] numeric(19,4),
	 [TotalAmount] numeric(19,4)
	 )
	 WITH (
	 CLUSTERED COLUMNSTORE INDEX
	 ,DISTRIBUTION=HASH([DateID])
	 );

--Round Robin----
CREATE TABLE tbl_TaxiRide_RB(
     [DateID] int,
	 [MedallionID] int,
	 [HackneyLicenseID] int,
	 [PickupTimeID] int,
	 [DropoffTimeID] int,
	 [PickupGeographyID] int,
	 [DropoffGeographyID] int,
	 [PickupLatitude] float,
	 [PickupLongitude] float,
	 [PickupLatLong] nvarchar(4000),
	 [DropoffLatitude] float,
	 [DropoffLongitude] float,
	 [DropoffLatLong] nvarchar(4000),
	 [PassengerCount] int,
	 [TripDurationSeconds] int,
	 [TripDistanceMiles] float,
	 [PaymentType] nvarchar(4000),
	 [FareAmount] numeric(19,4),
	 [SurchargeAmount] numeric(19,4),
	 [TaxAmount] numeric(19,4),
	 [TipAmount] numeric(19,4),
	 [TollsAmount] numeric(19,4),
	 [TotalAmount] numeric(19,4)
	 )
	 WITH (
	 CLUSTERED COLUMNSTORE INDEX
	 ,DISTRIBUTION=ROUND_ROBIN)
	

--- Replicated ----
CREATE TABLE tbl_TaxiRide_RP_NON(
     [DateID] int,
	 [MedallionID] int,
	 [HackneyLicenseID] int,
	 [PickupTimeID] int,
	 [DropoffTimeID] int,
	 [PickupGeographyID] int,
	 [DropoffGeographyID] int,
	 [PickupLatitude] float,
	 [PickupLongitude] float,
	 [PickupLatLong] nvarchar(4000),
	 [DropoffLatitude] float,
	 [DropoffLongitude] float,
	 [DropoffLatLong] nvarchar(4000),
	 [PassengerCount] int,
	 [TripDurationSeconds] int,
	 [TripDistanceMiles] float,
	 [PaymentType] nvarchar(4000),
	 [FareAmount] numeric(19,4),
	 [SurchargeAmount] numeric(19,4),
	 [TaxAmount] numeric(19,4),
	 [TipAmount] numeric(19,4),
	 [TollsAmount] numeric(19,4),
	 [TotalAmount] numeric(19,4)
	 )
	 WITH (
	 CLUSTERED COLUMNSTORE INDEX
	 ,DISTRIBUTION=REPLICATE)



--- ##CTAS -- CREATE TABLE AS -----
CREATE TABLE tbride_CTAS_RB
 WITH (
	 CLUSTERED COLUMNSTORE INDEX
	 ,DISTRIBUTION=ROUND_ROBIN)

	 AS

SELECT * FROM [dbo].[tbl_TaxiRidee]


SELECT * FROM tbride_CTAS_RB


	 CREATE TABLE tbride_CTAS_RP
 WITH (
	 CLUSTERED COLUMNSTORE INDEX
	 ,DISTRIBUTION=REPLICATE)

	 AS

SELECT * FROM [dbo].[tbl_TaxiRidee]



	  CREATE TABLE tbride_CTAS_HASH
	  WITH (
	 CLUSTERED COLUMNSTORE INDEX
	 ,DISTRIBUTION=HASH([DateID])
	 )

	  AS

SELECT * 
FROM [dbo].[tbl_TaxiRidee]



-- #### Temp Tables In MPP ######
#, ##

 IF OBJECT_ID(N'tempdb..#tbride_CTAS_RP',N'U') IS NOT NULL
DROP TABLE #tbride_CTAS_RP
 	 CREATE TABLE #tbride_CTAS_RP
 WITH (
	 CLUSTERED COLUMNSTORE INDEX
	 ,DISTRIBUTION=ROUND_ROBIN)

	 AS

SELECT * FROM [dbo].[tbl_TaxiRidee]

select * from #tbride_CTAS_RP

-- ## CREATE MATERIALIZED VIEWS IN mpp/ SYNAPSE DED. POOL ####
SELECT * FROM [dbo].[tbl_TaxiRidee]

SELECT DateID, COUNT(1) AS Trip_Count
FROM [dbo].[tbl_TaxiRidee]
GROUP BY DateID


CREATE MATERIALIZED VIEW dbo.Trip_Count
 WITH (
	 CLUSTERED COLUMNSTORE INDEX
	 ,DISTRIBUTION=ROUND_ROBIN
	 )

	 AS
SELECT DateID, COUNT(1) AS Trip_Count FROM dbo.tbride_CTAS_RP GROUP BY DateID


select * from dbo.Trip_Count

---#### Determine if a table has a right  distribution ###########
DBCC PDW_SHOWSPACEUSED('tbride_CTAS_RB')
DBCC PDW_SHOWSPACEUSED('tbride_CTAS_hash')
DBCC PDW_SHOWSPACEUSED('tbride_CTAS_RP')




---#######################COPY INTO FOR CSV ###################################
CREATE  TABLE Cars (
	Make nvarchar(100),
	Model nvarchar(200),
	Type nvarchar(100),
	Origin nvarchar(100),
	DriveTrain nvarchar(100)
)

COPY INTO cars
FROM 'https://slgen.dfs.core.windows.net/data/csv/'
WITH (
    FILE_TYPE = 'CSV'
    ,CREDENTIAL=(IDENTITY= 'Shared Access Signature', 
	SECRET='sp=racwdlmeop&st=2024-03-07T22:08:28Z&se=2024-03-30T05:08:28Z&sv=2022-11-02&sr=c&sig=3XYab9gkeXMJMOTM3G%2Fk4em1C35DvIYKqwxhuFrBuUQ%3D')
   
    ,ROWTERMINATOR='0x0A'-- COPY command automatically prefixes the \r character when \n (newline) is specified. This results in carriage return newline (\r\n) for Windows based systems.
)

select * from cars

where make like '%Acura%'


select count(1) from cars




































---#####Update Stats---########
SELECT name, is_auto_create_stats_on
FROM sys.databases

Link:https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/develop-tables-statistics
SELECT
    sm.[name] AS [schema_name],
    tb.[name] AS [table_name],
    co.[name] AS [stats_column_name],
    st.[name] AS [stats_name],
    STATS_DATE(st.[object_id],st.[stats_id]) AS [stats_last_updated_date]
FROM
    sys.objects ob
    JOIN sys.stats st
        ON  ob.[object_id] = st.[object_id]
    JOIN sys.stats_columns sc
        ON  st.[stats_id] = sc.[stats_id]
        AND st.[object_id] = sc.[object_id]
    JOIN sys.columns co
        ON  sc.[column_id] = co.[column_id]
        AND sc.[object_id] = co.[object_id]
    JOIN sys.types  ty
        ON  co.[user_type_id] = ty.[user_type_id]
    JOIN sys.tables tb
        ON  co.[object_id] = tb.[object_id]
    JOIN sys.schemas sm
        ON  tb.[schema_id] = sm.[schema_id]
WHERE
    st.[user_created] = 1;


	CREATE PROCEDURE    [dbo].[prc_sqldw_create_stats]
(   @create_type    tinyint -- 1 default, 2 Fullscan, 3 Sample
,   @sample_pct     tinyint
)
AS

IF @create_type IS NULL
BEGIN
    SET @create_type = 1;
END;

IF @create_type NOT IN (1,2,3)
BEGIN
    THROW 151000,'Invalid value for @stats_type parameter. Valid range 1 (default), 2 (fullscan) or 3 (sample).',1;
END;

IF @sample_pct IS NULL
BEGIN;
    SET @sample_pct = 20;
END;

IF OBJECT_ID('tempdb..#stats_ddl') IS NOT NULL
BEGIN;
    DROP TABLE #stats_ddl;
END;

CREATE TABLE #stats_ddl
WITH    (   DISTRIBUTION    = HASH([seq_nmbr])
        ,   LOCATION        = USER_DB
        )
AS
WITH T
AS
(
SELECT      t.[name]                        AS [table_name]
,           s.[name]                        AS [table_schema_name]
,           c.[name]                        AS [column_name]
,           c.[column_id]                   AS [column_id]
,           t.[object_id]                   AS [object_id]
,           ROW_NUMBER()
            OVER(ORDER BY (SELECT NULL))    AS [seq_nmbr]
FROM        sys.[tables] t
JOIN        sys.[schemas] s         ON  t.[schema_id]       = s.[schema_id]
JOIN        sys.[columns] c         ON  t.[object_id]       = c.[object_id]
LEFT JOIN   sys.[stats_columns] l   ON  l.[object_id]       = c.[object_id]
                                    AND l.[column_id]       = c.[column_id]
                                    AND l.[stats_column_id] = 1
LEFT JOIN    sys.[external_tables] e    ON    e.[object_id]        = t.[object_id]
WHERE       l.[object_id] IS NULL
AND            e.[object_id] IS NULL -- not an external table
)
SELECT  [table_schema_name]
,       [table_name]
,       [column_name]
,       [column_id]
,       [object_id]
,       [seq_nmbr]
,       CASE @create_type
        WHEN 1
        THEN    CAST('CREATE STATISTICS '+QUOTENAME('stat_'+table_schema_name+ '_' + table_name + '_'+column_name)+' ON '+QUOTENAME(table_schema_name)+'.'+QUOTENAME(table_name)+'('+QUOTENAME(column_name)+')' AS VARCHAR(8000))
        WHEN 2
        THEN    CAST('CREATE STATISTICS '+QUOTENAME('stat_'+table_schema_name+ '_' + table_name + '_'+column_name)+' ON '+QUOTENAME(table_schema_name)+'.'+QUOTENAME(table_name)+'('+QUOTENAME(column_name)+') WITH FULLSCAN' AS VARCHAR(8000))
        WHEN 3
        THEN    CAST('CREATE STATISTICS '+QUOTENAME('stat_'+table_schema_name+ '_' + table_name + '_'+column_name)+' ON '+QUOTENAME(table_schema_name)+'.'+QUOTENAME(table_name)+'('+QUOTENAME(column_name)+') WITH SAMPLE '+CONVERT(varchar(4),@sample_pct)+' PERCENT' AS VARCHAR(8000))
        END AS create_stat_ddl
FROM T
;

DECLARE @i INT              = 1
,       @t INT              = (SELECT COUNT(*) FROM #stats_ddl)
,       @s NVARCHAR(4000)   = N''
;

WHILE @i <= @t
BEGIN
    SET @s=(SELECT create_stat_ddl FROM #stats_ddl WHERE seq_nmbr = @i);

    PRINT @s
    EXEC sp_executesql @s
    SET @i+=1;
END

DROP TABLE #stats_ddl;

EXEC [dbo].[prc_sqldw_create_stats] 1, NULL;


update statisitcs [tbl_TaxiRide_Hash]