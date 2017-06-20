/* ########## PRE ##########
USE [db_sgs_producao]
GO
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
IF OBJECT_ID('db_sgs_producao.dbo.pre_snapshot_ren_indices') IS NOT NULL TRUNCATE TABLE dbo.pre_snapshot_ren_indices
SELECT
	SchemaName = ss.name
	, TableName = st.name
	, IndexName = ISNULL(si.name, '')
	, IndexType = si.type_desc
	, user_updates = ISNULL(ius.user_updates, 0)
	, user_seeks = ISNULL(ius.user_seeks, 0)
	, user_scans = ISNULL(ius.user_scans, 0)
	, user_lookups = ISNULL(ius.user_lookups, 0)
	, ssi.rowcnt
	, ssi.rowmodctr
	, si.fill_factor
	, GETDATE() AS Data_Hora
INTO dbo.pre_snapshot_ren_indices
FROM sys.dm_db_index_usage_stats ius
RIGHT OUTER JOIN sys.indexes si ON ius.[object_id] = si.[object_id]
		AND ius.index_id = si.index_id
INNER JOIN sys.sysindexes ssi ON si.object_id = ssi.id
		AND si.name = ssi.name
INNER JOIN sys.tables st ON st.[object_id] = si.[object_id]
INNER JOIN sys.schemas ss ON ss.[schema_id] = st.[schema_id]
WHERE ius.database_id = DB_ID()
AND OBJECTPROPERTY(ius.[object_id], 'IsMsShipped') = 0

IF OBJECT_ID('db_sgs_producao.dbo.pre_snapshot_ren_consultas') IS NOT NULL TRUNCATE TABLE dbo.pre_snapshot_ren_consultas
SELECT *, GETDATE() AS Data_Hora
INTO dbo.pre_snapshot_ren_consultas
FROM sys.dm_exec_query_stats
############################## */




/* ########## PóS ##########
USE [db_sgs_producao]
GO
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
IF OBJECT_ID('db_sgs_producao.dbo.pos_snapshot_ren_indices') IS NOT NULL TRUNCATE TABLE dbo.pos_snapshot_ren_indices
SELECT
	SchemaName = ss.name
	, TableName = st.name
	, IndexName = ISNULL(si.name, '')
	, IndexType = si.type_desc
	, user_updates = ISNULL(ius.user_updates, 0)
	, user_seeks = ISNULL(ius.user_seeks, 0)
	, user_scans = ISNULL(ius.user_scans, 0)
	, user_lookups = ISNULL(ius.user_lookups, 0)
	, ssi.rowcnt
	, ssi.rowmodctr
	, si.fill_factor
	, GETDATE() AS Data_Hora
INTO dbo.pos_snapshot_ren_indices
FROM sys.dm_db_index_usage_stats ius
RIGHT OUTER JOIN sys.indexes si ON ius.[object_id] = si.[object_id]
		AND ius.index_id = si.index_id
INNER JOIN sys.sysindexes ssi ON si.object_id = ssi.id
		AND si.name = ssi.name
INNER JOIN sys.tables st ON st.[object_id] = si.[object_id]
INNER JOIN sys.schemas ss ON ss.[schema_id] = st.[schema_id]
WHERE ius.database_id = DB_ID()
AND OBJECTPROPERTY(ius.[object_id], 'IsMsShipped') = 0

IF OBJECT_ID('db_sgs_producao.dbo.pos_snapshot_ren_consultas') IS NOT NULL TRUNCATE TABLE dbo.pos_snapshot_ren_consultas
SELECT *, GETDATE() AS Data_Hora
INTO dbo.pos_snapshot_ren_consultas
FROM sys.dm_exec_query_stats
########################## */




USE [master]
GO

SELECT 
       SUBSTRING([PARENT QUERY],1,80) [PARENT QUERY],
       MIN(CREATION_TIME) [CRIAÇÃO DO PLANO],
       MAX(LAST_EXECUTION_TIME) [ÚLTIMA EXECUÇÃO], 
       SUM(EXECUTION_COUNT) [TOTAL EXECUÇÕES], 
       SUM(TOTAL_WORKER_TIME) / 1000000. [TEMPO TOTAL EM SEGUNDOS], 
       (SUM(TOTAL_WORKER_TIME) / 1000000.) /  SUM(EXECUTION_COUNT) [MÉDIA TEMPO],
       MIN(MIN_WORKER_TIME) / 1000000. [MENOR TEMPO EXECUÇÃO], 
       MAX(MAX_WORKER_TIME) / 1000000. [MAIOR TEMPO EXECUÇÃO],
       SUM(TOTAL_PHYSICAL_READS) [TOTAL_PHYSICAL_READS],
       SUM(TOTAL_PHYSICAL_READS) / SUM(EXECUTION_COUNT) [MÉDIA LEIT. FÍSICA],
       SUM(TOTAL_LOGICAL_READS) [TOTAL_LOGICAL_READS],
       SUM(TOTAL_LOGICAL_READS) / SUM(EXECUTION_COUNT) [MÉDIA LEIT. LÓGICAS],
       SUM(TOTAL_LOGICAL_WRITES) [TOTAL_LOGICAL_WRITES],
       [PARENT QUERY] AS [FULL QUERY]
FROM
(
	SELECT
	(P2.TOTAL_ELAPSED_TIME - ISNULL(P1.TOTAL_ELAPSED_TIME, 0)) / 1000000. AS [DURAÇÃO EM SEGUNDOS]
	 ,SUBSTRING (QT.TEXT,P2.STATEMENT_START_OFFSET/2 + 1,
	((CASE WHEN P2.STATEMENT_END_OFFSET = -1
	THEN LEN(CONVERT(NVARCHAR(MAX), QT.TEXT)) * 2
	ELSE P2.STATEMENT_END_OFFSET
	END - P2.STATEMENT_START_OFFSET)/2) + 1 ) AS [INDIVIDUAL QUERY]
	, QT.TEXT AS [PARENT QUERY]
	, DB_NAME(QT.DBID) AS DATABASENAME
	, P2.CREATION_TIME
	, P2.LAST_EXECUTION_TIME
	, ABS(ISNULL(P2.EXECUTION_COUNT,0) - ISNULL(P1.EXECUTION_COUNT,0)) AS EXECUTION_COUNT
	, ISNULL(P2.TOTAL_WORKER_TIME,0) - ISNULL(P1.TOTAL_WORKER_TIME,0) AS TOTAL_WORKER_TIME
	, P2.LAST_WORKER_TIME
	, P2.MIN_WORKER_TIME
	, P2.MAX_WORKER_TIME
	, ISNULL(P2.TOTAL_PHYSICAL_READS,0) - ISNULL(P1.TOTAL_PHYSICAL_READS,0) TOTAL_PHYSICAL_READS
	, ISNULL(P2.TOTAL_LOGICAL_READS,0) - ISNULL(P1.TOTAL_LOGICAL_READS,0) TOTAL_LOGICAL_READS 
	, P2.TOTAL_LOGICAL_WRITES
	, P2.TOTAL_ELAPSED_TIME
	, P2.LAST_ELAPSED_TIME
	, P2.MIN_ELAPSED_TIME
	, P2.MAX_ELAPSED_TIME
FROM [db_sgs_producao].dbo.pre_snapshot_ren_consultas P1
	RIGHT OUTER JOIN  [db_sgs_producao].dbo.pos_snapshot_ren_consultas P2 
ON P2.SQL_HANDLE = ISNULL(P1.SQL_HANDLE, P2.SQL_HANDLE)
						AND P2.PLAN_HANDLE = ISNULL(P1.PLAN_HANDLE, P2.PLAN_HANDLE)
						AND P2.STATEMENT_START_OFFSET = ISNULL(P1.STATEMENT_START_OFFSET, P2.STATEMENT_START_OFFSET)
						AND P2.STATEMENT_END_OFFSET = ISNULL(P1.STATEMENT_END_OFFSET, P2.STATEMENT_END_OFFSET)
	CROSS APPLY SYS.DM_EXEC_SQL_TEXT(P2.SQL_HANDLE) AS QT
	WHERE P2.EXECUTION_COUNT != ISNULL(P1.EXECUTION_COUNT, 0))A
GROUP BY [PARENT QUERY]
ORDER BY 5 DESC