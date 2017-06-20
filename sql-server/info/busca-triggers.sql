SELECT db_name () AS [Database Name],
       T .[name] AS [TableName],
       TR .[Name] AS [TriggerName],
       [Status] = CASE WHEN OBJECTPROPERTY(TR .[id], 'ExecIsTriggerDisabled') = 1
               THEN 'Disabled' ELSE 'Enabled' END
FROM   sysobjects T
JOIN   sysobjects TR ON t .[ID] = TR.[parent_obj]
WHERE  T. [xtype] = 'U'
        AND TR. [xtype] = 'TR'
        AND OBJECTPROPERTY (TR. [id], 'ExecIsTriggerDisabled') = 1 --Use 1 to list Disabled triggers and 0 for Enabled triggers
