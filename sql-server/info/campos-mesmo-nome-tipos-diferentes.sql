WITH UniqueColumnDefinitions AS
(SELECT
        lower(o .[name]) AS [Table] ,
        lower(c .[name]) AS [Column] ,
        lower(t .[name]) AS [Data Type] ,
              c .[max_length] AS [Max Lenght],
        c .[precision] AS [Precision],
        c .[scale] AS [Scale],        
        [check_sum] = binary_checksum (lower( c.[name] ), lower (t. [name]), c.[max_length], c.[precision] , c. [scale])
FROM       [sys].[objects] o
JOIN          [sys].[schemas] s     ON s. [schema_id] = o .[schema_id]
JOIN        [sys].[columns] c    ON c. [object_id] = o .[object_id]
JOIN        [sys].[types] t      ON c. [system_type_id] = t .[system_type_id] AND t.[name] <> 'sysname'
WHERE   o. [type] = 'U' and
              s .[name] = 'cob')

SELECT  MIN ([column]) AS [Column] ,
        MIN([data type] ) AS [Data Type],
               MIN([max lenght] ) AS [Max Lenght],
        MIN([precision] ) AS [Precision],
        MIN([Scale] ) AS [Scale],       
        COUNT(*) AS [Usage Count] FROM UniqueColumnDefinitions
GROUP BY check_sum
ORDER BY [Column]
