DECLARE @DatabaseName NVARCHAR(128) = DB_NAME();

-- Create required cleanup tables
CREATE TABLE dbo.CleanupConfig (
    TableName NVARCHAR(128),
    IdColumn NVARCHAR(128),
    DateTimeColumn NVARCHAR(128),
    AdditionalQuery NVARCHAR(4000),
    DaysOld INT NOT NULL CHECK (DaysOld > 0),
    StartTime TIME NOT NULL,
    EndTime TIME NOT NULL,
    BatchSize INT NOT NULL CHECK (BatchSize > 0),
    ForceCascade BIT DEFAULT 1
);

CREATE TABLE dbo.CleanupLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    TableName NVARCHAR(128),
    DeletedRows INT,
    ExecutionTime DATETIME,
    ErrorMessage NVARCHAR(MAX)
);

-- Insert a sample row into the CleanupConfig table
INSERT INTO dbo.CleanupConfig (TableName, IdColumn, DateTimeColumn, AdditionalQuery, DaysOld, StartTime, EndTime, BatchSize, ForceCascade)
VALUES
    ('[dbo].[QueueItems]',          'Id',    'CreationTime',          'status=3',            60,  '01:00:00', '05:00:00',  1000, 1),
    ('[dbo].[Logs]',                'Id',    'TimeStamp',             '1=1',                 180, '01:00:00', '05:00:00', 10000, 0),
    ('[dbo].[RobotLicenseLogs]',    'Id',    'EndDate',               'EndDate is not null', 60,  '01:00:00', '05:00:00', 10000, 0),
    ('[dbo].[TenantNotifications]', 'Id',    'CreationTime',          '1=1',                 60,  '01:00:00', '05:00:00', 10000, 1),
    ('[dbo].[jobs]',                'Id',    'CreationTime',          'State in (4, 5, 6)',  60,  '01:00:00', '05:00:00', 10000, 0),
    ('[dbo].[AuditLogs]',           'Id',    'ExecutionTime',         '1=1',                 180, '01:00:00', '05:00:00', 10000, 1),
    ('[dbo].[Tasks]',               'Id',    'DeletionTime',          'IsDeleted = 1',       60,  '01:00:00', '05:00:00', 10000, 0),
    ('[dbo].[Tasks]',               'Id',    'LastModificationTime',  'Status = 2',          60,  '01:00:00', '05:00:00', 10000, 0),
    ('[dbo].[Sessions]',            'Id',    'ReportingTime',         '1=1',                 180, '01:00:00', '05:00:00', 10000, 1),
    ('[dbo].[Ledger]',              'Id',    'CreationTime',          '1=1',                 180, '01:00:00', '05:00:00', 10000, 0),
    ('[dbo].[LedgerDeliveries]',    'Id',    'LastUpdatedTime',       '1=1',                 180, '01:00:00', '05:00:00', 10000, 0),
    ('[dbo].[CleanupLog]',          'LogID', 'ExecutionTime',         '1=1',                 30,  '01:00:00', '05:00:00',  5000, 0);
GO

-- Create the cleanup stored procedure
CREATE PROCEDURE dbo.HourlyCleanupProcess
AS
BEGIN
    IF OBJECT_ID('#TempDeletedIds', 'U') IS NULL
        CREATE TABLE #TempDeletedIds (IdToDelete INT);

    -- Cleanup process with error handling
    DECLARE @TableName NVARCHAR(128), @IdColumn NVARCHAR(128), @DateTimeColumn NVARCHAR(128), @AdditionalQuery NVARCHAR(4000), @StartTime TIME, @EndTime TIME, @BatchSize INT, @DaysOld INT, @ForceCascade BIT;

    DECLARE @DeletedRows INT, @TotalDeletedRows INT, @CurrentTime TIME;
    DECLARE @DynamicSQL NVARCHAR(MAX), @ErrorMessage NVARCHAR(4000);

    DECLARE ConfigCursor CURSOR LOCAL FOR
        SELECT TableName, IdColumn, DateTimeColumn, AdditionalQuery, StartTime, EndTime, BatchSize, DaysOld, ForceCascade
        FROM dbo.CleanupConfig;

    OPEN ConfigCursor;

    FETCH NEXT FROM ConfigCursor INTO @TableName, @IdColumn, @DateTimeColumn, @AdditionalQuery, @StartTime, @EndTime, @BatchSize, @DaysOld, @ForceCascade;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            SET @DeletedRows = -1;
            SET @TotalDeletedRows = 0;
            SET @CurrentTime = CAST(GETDATE() AS TIME);

            WHILE @CurrentTime >= @StartTime AND @CurrentTime <= @EndTime AND @DeletedRows != 0
            BEGIN
                BEGIN TRANSACTION
                    -- get the IDs to be deleted
                    DELETE from #TempDeletedIds;
                    SET @DynamicSQL = N'INSERT INTO #TempDeletedIds SELECT TOP ' + CAST(@BatchSize AS NVARCHAR(10)) + N' ' + CAST(@IdColumn AS NVARCHAR(10)) + N' AS IdToDelete FROM ' + (@TableName) + N' WHERE ' + @AdditionalQuery + N' AND DATEADD(DAY, -' + CAST(@DaysOld AS NVARCHAR(10)) + N', GETDATE()) > ' + CAST(@DateTimeColumn AS NVARCHAR(128));
                    print @DynamicSQL
                    EXEC sp_executesql @DynamicSQL;
                    SET @DeletedRows = @@ROWCOUNT;
                    IF @DeletedRows > 0
                    BEGIN
                        IF @ForceCascade = 1
                        BEGIN
                            -- go one level deeper in the DB schema
                            DECLARE @FKName NVARCHAR(255);
                            DECLARE @FKTableName NVARCHAR(255);

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
                                SET @DynamicSQL = N'DELETE FROM ' + (@FKTableName) + N' WHERE ' + (@FKName) + N' IN (SELECT IdToDelete FROM #TempDeletedIds)';
                                print @DynamicSQL
                                EXEC sp_executesql @DynamicSQL;
                                FETCH NEXT FROM ReferenceCursor INTO @FKName, @FKTableName;
                            END;

                            CLOSE ReferenceCursor;
                            DEALLOCATE ReferenceCursor;
                        END
                        -- delete records
                        SET @DynamicSQL = N'DELETE FROM ' + (@TableName) + N' WHERE ' + CAST(@IdColumn AS NVARCHAR(10)) + N' IN (SELECT IdToDelete FROM #TempDeletedIds)';
                        print @DynamicSQL
                        EXEC sp_executesql @DynamicSQL;
                    END
                COMMIT TRANSACTION
                -- Wait for 5 seconds before running the next batch if at least 1 row was deleted
                IF @DeletedRows > 0
                BEGIN
                    SET @TotalDeletedRows = @TotalDeletedRows + @DeletedRows;
                    WAITFOR DELAY '00:00:05';
                END
                SET @CurrentTime = CAST(GETDATE() AS TIME);
            END
            IF @TotalDeletedRows > 0
                INSERT INTO dbo.CleanupLog (TableName, DeletedRows, ExecutionTime, ErrorMessage) VALUES (@TableName, @TotalDeletedRows, GETDATE(), NULL);

        END TRY
        BEGIN CATCH
            -- Capture the error message and continue with the next iteration
            SET @ErrorMessage = ERROR_MESSAGE();
            INSERT INTO dbo.CleanupLog (TableName, DeletedRows, ExecutionTime, ErrorMessage) VALUES (@TableName, NULL, GETDATE(), @ErrorMessage);
            PRINT 'Error while processing table ' + (@TableName) + ': ' + @ErrorMessage;
        END CATCH;

        FETCH NEXT FROM ConfigCursor INTO @TableName, @IdColumn, @DateTimeColumn, @AdditionalQuery, @StartTime, @EndTime, @BatchSize, @DaysOld, @ForceCascade;
    END

    CLOSE ConfigCursor;
    DEALLOCATE ConfigCursor;
    DROP TABLE #TempDeletedIds
END;
GO

USE msdb;
GO

EXEC msdb.dbo.sp_add_job
    @job_name = N'HourlyCleanup',
    @enabled = 1,
    @description = 'Clean up the database of old records'
GO

-- create the cleanup job to be called every hour
EXEC sp_add_jobstep
    @job_name = N'HourlyCleanup',
    @step_name = N'HourlyCleanupOfOldRows',
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
    @database_name = @DatabaseName, -- Set the context to msdb database as we are using msdb.dbo.sysjobs and msdb.dbo.sysjobactivity
    @flags = 0;
GO

-- create the hourly schedule
EXEC sp_add_jobschedule
    @job_name = N'HourlyCleanup',
    @name = N'Cleanup Every Hour',
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

EXEC msdb.dbo.sp_add_jobserver
    @job_name = 'HourlyCleanup',
    @server_name = @@SERVERNAME;
GO