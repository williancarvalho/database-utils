/*
DESCRIPTION:
This script will generate a summary report of the stored procedures, 
views, functions and triggers in the current database to show the 
number of lines of code, the number of referenced objects and the 
number of parameters. These metrics can be used to judge the size 
and the complexity of the code according to user's own preferences.

The included CASE statement is generic. You can modify its boundary
values that are more granular or write your own statements that look 
at individual factors i.e. the number of code lines, number of 
referenced objects or number of parameters.

The calculation of the number of lines of code can not be standard because 
it depends on the developer's coding style and their use of whitespace.

CHANGE HISTORY:
2012/Dec/01 - Version 1
2013/Feb/15 - Version 1.1 Added the CASE statement for complexity.
2014/Apr/29 - Version 1.2 Added Database and Schema name to the report.
                          Included objects that may not be refering to other objects.
                          Included views, functions and triggers.

INPUT PARAMETERS: 
None

COMPATIBILITY: 
SQL Server version 2008 and later due to use of sys.sql_expression_dependencies

HOW TO USE: 
Select the database and execute the script. It creates and drops one temporary table.

SOURCE: 
http://aalamrangi.wordpress.com/2012/12/24/calculate-tsql-stored-procedure-complexity/
*/

IF OBJECT_ID('tempdb..#t') IS NOT NULL 
  DROP TABLE #t; 

/* Get dependencies */
;WITH refitems 
     AS (SELECT ISNULL(referencing_id, o.object_id)								  AS referencing_id,
				DB_NAME(DB_ID())                                                  AS referencing_database_name,
                OBJECT_SCHEMA_NAME (ISNULL(referencing_id, o.object_id))          AS referencing_schema_name,
                OBJECT_NAME(ISNULL(referencing_id, o.object_id))                  AS referencing_entity_name,
                o.type_desc                                                       AS referencing_desciption,
                COALESCE(COL_NAME(referencing_id, referencing_minor_id), '(n/a)') AS referencing_minor_id,
                referencing_class_desc, 
                referenced_class_desc, 
                referenced_database_name, 
                referenced_schema_name, 
                referenced_entity_name, 
                COALESCE(COL_NAME(referenced_id, referenced_minor_id), '(n/a)')   AS referenced_column_name,
                ISNULL(r.type_desc, referenced_class_desc)                        AS referenced_desciption,
                is_caller_dependent, 
                is_ambiguous 
         FROM   sys.sql_expression_dependencies AS sed 
                FULL OUTER JOIN sys.objects AS o 
                             ON sed.referencing_id = o.object_id                              
                LEFT OUTER JOIN sys.objects AS r 
                             ON OBJECT_ID(sed.referenced_entity_name) = r.object_id 
        WHERE o.type_desc IN ('SQL_STORED_PROCEDURE', 'VIEW', 'SQL_SCALAR_FUNCTION', 'SQL_INLINE_TABLE_VALUED_FUNCTION', 'SQL_TRIGGER')
        /* -- put your stored proc names if need to filter
          AND OBJECT_NAME(referencing_id) 
          IN  
          ( 
          'spMyStoredProc_1', 
          'spMyStoredProc_2',
          'spMyStoredProc_n', 
          ) 
        */ 
        ) 
SELECT referencing_id,
	   referencing_database_name,
	   referencing_schema_name,
	   referencing_entity_name,
	   referencing_desciption,
       NULL AS numcodelines, 
       COUNT(DISTINCT ISNULL(referenced_database_name+'.', '') + ISNULL(referenced_schema_name+'.', '') + referenced_entity_name) AS numreferencedobjects 
INTO   #t 
FROM   refitems 
GROUP  BY referencing_id, referencing_database_name, referencing_schema_name, referencing_entity_name, referencing_desciption

/* select * from #t */

DECLARE @i AS INT /* for stored proc object id */
DECLARE @n AS VARCHAR(128) /* for stored proc name */
DECLARE @c INT /* for line count */
DECLARE @txt TABLE /* for stored proc text */
  ( 
     txt VARCHAR(max) 
  ) 

/* Loop through all stored procs to extract their text and count number of lines */
WHILE EXISTS (SELECT 1 
              FROM   #t 
              WHERE  NumCodeLines IS NULL) 
  BEGIN 
      SELECT TOP 1 
      @i = referencing_id, 
      @n = ISNULL(referencing_database_name+'.', '') + ISNULL(referencing_schema_name+'.', '') + referencing_entity_name
      FROM   #t 
      WHERE  NumCodeLines IS NULL 

      INSERT INTO @txt 
                  (txt) 
      EXEC SP_HELPTEXT 
        @n 

      SELECT @c = COUNT(*) 
      FROM   @txt 

      UPDATE #t 
      SET    NumCodeLines = @c 
      WHERE  referencing_id = @i

      DELETE FROM @txt 
  END 

/* Create report */
;WITH spsummary (DatabaseName, SchemaName, ObjectName, ObjectType, NumberOfCodeLines, NumberOfReferencedObjects, NumberOfParameters) 
     AS (SELECT t1.referencing_database_name,
				t1.referencing_schema_name,
				t1.referencing_entity_name, 
				t1.referencing_desciption,
                t1.NumCodeLines, 
                t1.NumReferencedObjects, 
                COUNT(c1.name) AS numparameters 
         FROM   #t AS t1 
                LEFT OUTER JOIN sys.syscolumns AS c1                              
                             ON c1.id = t1.referencing_id
         GROUP  BY t1.referencing_database_name,
				   t1.referencing_schema_name,
				   t1.referencing_entity_name, 
				   t1.referencing_desciption,
                   t1.NumCodeLines, 
                   t1.NumReferencedObjects) 
SELECT DISTINCT DatabaseName, 
				SchemaName, 
				ObjectName, 
				ObjectType,
                NumberOfCodeLines, 
                NumberOfReferencedObjects, 
                NumberOfParameters,                
                CASE 
					WHEN NumberOfCodeLines * NumberOfReferencedObjects * NumberOfParameters < 5000 THEN 'SIMPLE'
					WHEN NumberOfCodeLines * NumberOfReferencedObjects * NumberOfParameters < 10000 THEN 'MEDIUM'
					ELSE 'COMPLEX'
				END AS Complexity
FROM   spsummary 
ORDER  BY DatabaseName, SchemaName, ObjectType, ObjectName

/* Clean-up */
DROP TABLE #t; 