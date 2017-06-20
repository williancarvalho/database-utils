CREATE PROCEDURE [dbo].[usp_Inxed_Statistics_Maintenance]
@DBName AS NVARCHAR(128)
AS

DECLARE @ERRORE INT--Check Database Error
DBCC CHECKDB WITH NO_INFOMSGS
SET @ERRORE = @@ERRORIF @ERRORE = 0 
BEGIN
        DECLARE @RC INT
        DECLARE @Messaggio VARCHAR(MAX)
        DECLARE @Rebild AS VARCHAR(MAX)
        DECLARE @Reorganize AS VARCHAR(MAX)

        SET @Reorganize = ''
        SET @Rebild = ''

        SELECT  @Reorganize = @Reorganize + ' ' + _
        'ALTER INDEX [' + i.[name] + '] ON [dbo].[' + t.[name] + '] 
                        REORGANIZE WITH ( LOB_COMPACTION = ON )'
        FROM   sys.dm_db_index_physical_stats
                          (DB_ID(@DBName ), NULL, NULL, NULL , 'DETAILED') fi
                        inner join sys.tables t
                         on fi.[object_id] = t.[object_id]
                        inner join sys.indexes i
                         on fi.[object_id] = i.[object_id] and
                                fi.index_id = i.index_id
        where t.[name] is not null and i.[name] is not null 
                        and avg_fragmentation_in_percent > 10   
                        and avg_fragmentation_in_percent <=35
        order by t.[name]

        EXEC (@Reorganize)

        SELECT  @Rebild = @Rebild + ' ' + _
        'ALTER INDEX [' + i.[name] + '] ON [dbo].[' + t.[name] + '] 
                        REBUILD WITH (ONLINE = OFF )'
        FROM   sys.dm_db_index_physical_stats
                          (DB_ID(@DBName ), NULL, NULL, NULL , 'DETAILED') fi
                        inner join sys.tables t
                         on fi.[object_id] = t.[object_id]
                        inner join sys.indexes i
                         on fi.[object_id] = i.[object_id] and
                                fi.index_id = i.index_id
        where avg_fragmentation_in_percent > 35 and t.[name] is not null and i.[name] is not null
        order by t.[name]

        EXEC (@Rebild)
END

-- if there are not error update statistics
SET @ERRORE = @@ERRORIF @ERRORE = 0
        BEGIN
                EXEC sp_updatestats
        END
        
;

