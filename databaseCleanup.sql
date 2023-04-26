-- clean up everything (used for resetting the whole thing)
DECLARE @JobID UNIQUEIDENTIFIER;
DECLARE @ScheduleID INT;
-- Get the job ID
SELECT @JobID = job_id FROM msdb.dbo.sysjobs WHERE name = 'HourlyCleanup';
-- Get the schedule ID
SELECT @ScheduleID = schedule_id FROM msdb.dbo.sysschedules WHERE name = 'HourlyCleanup';
-- Detach the schedule from the job
EXEC msdb.dbo.sp_detach_schedule @job_id = @JobID, @schedule_id = @ScheduleID;
-- Delete the schedule
EXEC msdb.dbo.sp_delete_schedule @schedule_id = @ScheduleID;
-- delete the job and its steps
EXEC msdb.dbo.sp_delete_job @job_name = 'HourlyCleanup';
-- drop all procedures, tables, databases
DROP PROCEDURE HourlyCleanupProcess
DROP PROCEDURE dbo.CreateArchiveDatabaseAndTables
DROP PROCEDURE dbo.GetArchivedTableName;
DROP TABLE dbo.CleanupConfig
DROP TABLE dbo.CleanupLog
DROP DATABASE Archives

--------------------------------------------------------------------------------------------------------------

-- Create cleanup table config
CREATE TABLE dbo.CleanupConfig (
    TableName NVARCHAR(128),
    IdColumn NVARCHAR(128),
    DateTimeColumn NVARCHAR(128),
    AdditionalQuery NVARCHAR(4000),
    DaysOld INT NOT NULL CHECK (DaysOld > 0),
    StartTime TIME NOT NULL,
    EndTime TIME NOT NULL,
    BatchSize INT NOT NULL CHECK (BatchSize > 0),
    ForceCascade BIT DEFAULT 1,
    ShouldBackup BIT DEFAULT 1
);

-- Create cleanup table log
CREATE TABLE dbo.CleanupLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    TableName NVARCHAR(128),
    DeletedRows INT,
    ExecutionTime DATETIME,
    ErrorMessage NVARCHAR(MAX)
);

-- Insert a sample row into the CleanupConfig table
INSERT INTO dbo.CleanupConfig (TableName, IdColumn, DateTimeColumn, AdditionalQuery, DaysOld, StartTime, EndTime, BatchSize, ForceCascade, ShouldBackup)
VALUES
    ('QueueItems',          'Id',    'CreationTime',          'status=3',            60,  '01:00:00', '05:00:00',  1000, 1, 1),
    ('Logs',                'Id',    'TimeStamp',             '1=1',                 180, '01:00:00', '05:00:00', 10000, 0, 1),
    ('RobotLicenseLogs',    'Id',    'EndDate',               'EndDate is not null', 60,  '01:00:00', '05:00:00', 10000, 0, 1),
    ('TenantNotifications', 'Id',    'CreationTime',          '1=1',                 60,  '01:00:00', '05:00:00', 10000, 1, 0),
    ('jobs',                'Id',    'CreationTime',          'State in (4, 5, 6)',  60,  '01:00:00', '05:00:00', 10000, 0, 1),
    ('AuditLogs',           'Id',    'ExecutionTime',         '1=1',                 180, '01:00:00', '05:00:00', 10000, 1, 1),
    ('Tasks',               'Id',    'DeletionTime',          'IsDeleted = 1',       60,  '01:00:00', '05:00:00', 10000, 0, 1),
    ('Tasks',               'Id',    'LastModificationTime',  'Status = 2',          60,  '01:00:00', '05:00:00', 10000, 0, 1),
    ('Sessions',            'Id',    'ReportingTime',         '1=1',                 180, '01:00:00', '05:00:00', 10000, 1, 0),
    ('Ledger',              'Id',    'CreationTime',          '1=1',                 180, '01:00:00', '05:00:00', 10000, 0, 0),
    ('LedgerDeliveries',    'Id',    'LastUpdatedTime',       '1=1',                 180, '01:00:00', '05:00:00', 10000, 0, 0),
    ('CleanupLog',          'LogID', 'ExecutionTime',         '1=1',                 30,  '01:00:00', '05:00:00',  5000, 0, 1);
GO


CREATE PROCEDURE dbo.GetArchivedTableName
    @TableName NVARCHAR(128),
    @Output NVARCHAR(128) OUTPUT
AS
BEGIN
    DECLARE @ColumnNames NVARCHAR(MAX);
    DECLARE @Hash NVARCHAR(64);
    DECLARE @TenCharHash NVARCHAR(10);

    -- Concatenate column names
    SELECT @ColumnNames = COALESCE(@ColumnNames + ', ', '') + QUOTENAME(c.name)
    FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    WHERE t.name = @TableName;

    -- Generate a SHA-256 hash
    SET @Hash = CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', @ColumnNames), 2);

    -- Take the first 10 characters of the hash
    SET @TenCharHash = SUBSTRING(@Hash, 1, 10);

    -- Prepend the table name
    SET @Output = @TableName + '_' + @TenCharHash;
END;
GO

CREATE PROCEDURE dbo.CreateArchiveDatabaseAndTables
    @TableName NVARCHAR(128),
    @Output NVARCHAR(128) OUTPUT
AS
BEGIN
    -- create the archives database if it does not exist
    IF NOT EXISTS (
        SELECT 1
        FROM sys.databases
        WHERE name = 'Archives'
    )
    BEGIN
        CREATE DATABASE Archives;
    END

    DECLARE @ArchivedTableName NVARCHAR(128);
    EXEC dbo.GetArchivedTableName @TableName = @TableName, @Output = @ArchivedTableName OUTPUT;
    IF NOT EXISTS (
        SELECT 1
        FROM Archives.sys.tables
        WHERE name = @ArchivedTableName
    )
    BEGIN
        -- The table does not exist
        DECLARE @DynamicSQL NVARCHAR(MAX) = 'select * into [Archives].'+@ArchivedTableName+'] from '+@TableName+' where 1=2';
        print @DynamicSQL
        EXEC sp_executesql @DynamicSQL;
    END;
    SET @Output = @ArchivedTableName;
END;

-- Create the cleanup stored procedure
CREATE PROCEDURE dbo.HourlyCleanupProcess
AS
BEGIN
    -- this is where we store the IDs to be deleted
    IF OBJECT_ID('#TempDeletedIds', 'U') IS NULL
        CREATE TABLE #TempDeletedIds (IdToDelete INT);

    DECLARE @TableName NVARCHAR(128), @IdColumn NVARCHAR(128), @DateTimeColumn NVARCHAR(128), @AdditionalQuery NVARCHAR(4000), @StartTime TIME, @EndTime TIME, @BatchSize INT, @DaysOld INT, @ForceCascade BIT, @ShouldBackup BIT;

    DECLARE @DeletedRows INT, @TotalDeletedRows INT, @CurrentTime TIME;
    DECLARE @DynamicSQL NVARCHAR(MAX), @ErrorMessage NVARCHAR(4000);

    DECLARE ConfigCursor CURSOR LOCAL FOR
        SELECT TableName, IdColumn, DateTimeColumn, AdditionalQuery, StartTime, EndTime, BatchSize, DaysOld, ForceCascade, ShouldBackup
        FROM dbo.CleanupConfig;

    OPEN ConfigCursor;

    FETCH NEXT FROM ConfigCursor INTO @TableName, @IdColumn, @DateTimeColumn, @AdditionalQuery, @StartTime, @EndTime, @BatchSize, @DaysOld, @ForceCascade, @ShouldBackup;

    -- for each row in the CleanupConfig table
    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            SET @DeletedRows = -1;
            SET @TotalDeletedRows = 0;
            SET @CurrentTime = CAST(GETDATE() AS TIME);

            -- Check to see if we should delete from this one
            WHILE @CurrentTime >= @StartTime AND @CurrentTime <= @EndTime AND @DeletedRows != 0
            BEGIN
                BEGIN TRANSACTION
                    -- get the IDs to be deleted
                    TRUNCATE TABLE #TempDeletedIds;
                    SET @DynamicSQL = N'INSERT INTO #TempDeletedIds SELECT TOP ' + CAST(@BatchSize AS NVARCHAR(10)) + N' ' + @IdColumn  + N' AS IdToDelete FROM ' + (@TableName) + N' WHERE ' + @AdditionalQuery + N' AND DATEADD(DAY, -' + CAST(@DaysOld AS NVARCHAR(10)) + N', GETDATE()) > ' + CAST(@DateTimeColumn AS NVARCHAR(128));
                    print @DynamicSQL
                    EXEC sp_executesql @DynamicSQL;
                    SET @DeletedRows = @@ROWCOUNT;

                    -- check to see if there are any rows to be deleted
                    IF @DeletedRows > 0
                    BEGIN
                        IF @ForceCascade = 1
                        BEGIN
                            -- go one level deeper in the DB schema
                            DECLARE @FKName NVARCHAR(255);
                            DECLARE @FKTableName NVARCHAR(255);
                            DECLARE @ArchivedTableName NVARCHAR(128);

                            DECLARE ReferenceCursor CURSOR LOCAL FOR 
                            SELECT c_parent.name AS FKName, t_parent.name AS FKTableName FROM
                                sys.foreign_keys fk 
                                INNER JOIN sys.foreign_key_columns fkc
                                    ON fkc.constraint_object_id = fk.object_id
                                INNER JOIN sys.tables t_parent
                                    ON t_parent.object_id = fk.parent_object_id
                                INNER JOIN sys.columns c_parent
                                    ON fkc.parent_column_id = c_parent.column_id  
                                    AND c_parent.object_id = t_parent.object_id 
                                INNER JOIN sys.tables t_child
                                    ON t_child.object_id = fk.referenced_object_id
                                WHERE t_child.name = @TableName;

                            OPEN ReferenceCursor;

                            FETCH NEXT FROM ReferenceCursor INTO @FKName, @FKTableName;

                            WHILE @@FETCH_STATUS = 0
                            BEGIN
                                if @ShouldBackup = 1
                                BEGIN
                                    -- backup
                                    EXEC dbo.CreateArchiveDatabaseAndTables @TableName = @FKTableName, @Output = @ArchivedTableName OUTPUT;
                                    SET @DynamicSQL = 'INSERT INTO [Archives].'+@ArchivedTableName+'] SELECT * FROM '+@FKTableName+' WHERE ' + (@FKName) + N' IN (SELECT IdToDelete FROM #TempDeletedIds)';
                                    print @DynamicSQL
                                    EXEC sp_executesql @DynamicSQL;
                                END;

                                -- delete from tables following FKs
                                SET @DynamicSQL = N'DELETE FROM ' + (@FKTableName) + N' WHERE ' + (@FKName) + N' IN (SELECT IdToDelete FROM #TempDeletedIds)';
                                print @DynamicSQL
                                EXEC sp_executesql @DynamicSQL;
                                FETCH NEXT FROM ReferenceCursor INTO @FKName, @FKTableName;
                            END;

                            CLOSE ReferenceCursor;
                            DEALLOCATE ReferenceCursor;
                        END
                        if @ShouldBackup = 1
                        BEGIN
                            -- backup
                            EXEC dbo.CreateArchiveDatabaseAndTables @TableName = @TableName, @Output = @ArchivedTableName OUTPUT;
                            SET @DynamicSQL = 'INSERT INTO [Archives].'+@ArchivedTableName+'] SELECT * FROM ' + @TableName + ' WHERE ' + @IdColumn + N' IN (SELECT IdToDelete FROM #TempDeletedIds)';
                            print @DynamicSQL
                            EXEC sp_executesql @DynamicSQL;
                        END;

                        -- delete records
                        SET @DynamicSQL = N'DELETE FROM ' + (@TableName) + N' WHERE ' + @IdColumn + N' IN (SELECT IdToDelete FROM #TempDeletedIds)';
                        print @DynamicSQL
                        EXEC sp_executesql @DynamicSQL;
                    END
                COMMIT TRANSACTION
                
                IF @DeletedRows > 0
                BEGIN
                    SET @TotalDeletedRows = @TotalDeletedRows + @DeletedRows;
                    -- Wait for 5 seconds before running the next batch if at least 1 row was deleted
                    WAITFOR DELAY '00:00:05';
                END
                SET @CurrentTime = CAST(GETDATE() AS TIME);
            END
            IF @TotalDeletedRows > 0
                INSERT INTO dbo.CleanupLog (TableName, DeletedRows, ExecutionTime, ErrorMessage) VALUES (@TableName, @TotalDeletedRows, GETDATE(), NULL); -- log the fact that we deleted some rows

        END TRY
        BEGIN CATCH
            -- Capture the error message and continue with the next iteration
            SET @ErrorMessage = ERROR_MESSAGE();
            INSERT INTO dbo.CleanupLog (TableName, DeletedRows, ExecutionTime, ErrorMessage) VALUES (@TableName, NULL, GETDATE(), @ErrorMessage);
            PRINT 'Error while processing table ' + (@TableName) + ': ' + @ErrorMessage;
        END CATCH;

        FETCH NEXT FROM ConfigCursor INTO @TableName, @IdColumn, @DateTimeColumn, @AdditionalQuery, @StartTime, @EndTime, @BatchSize, @DaysOld, @ForceCascade, @ShouldBackup;

    END

    CLOSE ConfigCursor;
    DEALLOCATE ConfigCursor;
    DROP TABLE #TempDeletedIds
END;
GO

-- create the 'HourlyCleanup' job to be called every hour
EXEC msdb.dbo.sp_add_job
    @job_name = N'HourlyCleanup',
    @enabled = 1,
    @description = 'Clean up the database of old records'
GO

DECLARE @DatabaseName NVARCHAR(128) = DB_NAME();
-- create a cleanup job step (one is required)
EXEC msdb.dbo.sp_add_jobstep
    @job_name = N'HourlyCleanup',
    @step_name = N'HourlyCleanup',
    @step_id = 1,
    @cmdexec_success_code = 0,
    @on_success_action = 1,
    @on_success_step_id = 0,
    @on_fail_action = 2,
    @on_fail_step_id = 0,
    @retry_attempts = 0,
    @retry_interval = 0,
    @os_run_priority = 0,
    @subsystem = N'TSQL',
    @command = N'
EXEC dbo.HourlyCleanupProcess
',
    @database_name = @DatabaseName,
    @flags = 0;
GO

-- create the hourly schedule
EXEC msdb.dbo.sp_add_jobschedule
    @job_name = N'HourlyCleanup',
    @name = N'HourlyCleanup',
    @enabled = 1,
    @freq_type = 4, -- Daily
    @freq_interval = 1,
    @freq_subday_type = 8, -- Hourly
    @freq_subday_interval = 1, -- Every 1 hour
    @freq_relative_interval = 0,
    @freq_recurrence_factor = 0,
    @active_start_date = 20230418, -- YYYYMMDD format
    @active_end_date = 99991231,
    @active_start_time = 0, -- HHMMSS format
    @active_end_time = 235959;
GO

-- attach a job server to our job
EXEC msdb.dbo.sp_add_jobserver
    @job_name = 'HourlyCleanup',
    @server_name = @@SERVERNAME;
GO