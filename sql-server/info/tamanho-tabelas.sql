SELECT Lower(s.NAME + '.' + t.NAME) AS TableName, 
       p.rows                       AS RowCounts, 
       Sum(a.used_pages) * 8        AS UsedSpaceKB 
FROM   sys.tables t 
       INNER JOIN sys.schemas s 
               ON s.schema_id = t.schema_id 
       INNER JOIN sys.indexes i 
               ON t.object_id = i.object_id 
       INNER JOIN sys.partitions p 
               ON i.object_id = p.object_id 
                  AND i.index_id = p.index_id 
       INNER JOIN sys.allocation_units a 
               ON p.partition_id = a.container_id 
WHERE  t.NAME NOT LIKE 'dt%' -- filter out system tables for diagramming 
       AND t.is_ms_shipped = 0 
       AND i.object_id > 255 
GROUP  BY t.NAME, 
          s.NAME, 
          p.rows 
ORDER  BY s.NAME, 
          t.NAME 