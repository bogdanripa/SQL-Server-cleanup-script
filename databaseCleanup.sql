DECLARE @DatabaseName NVARCHAR(128);
SET @DatabaseName = N'YourDatabaseName'; -- change this to match your database name

EXEC('USE ' + QUOTENAME(@DatabaseName) + ';');
GO

-- Create required cleanup tables
CREATE TABLE dbo.CleanupConfig (
    TableName NVARCHAR(128),
    IdColumn NVARCHAR(128),
    DateTimeColumn NVARCHAR(128),
    AdditionalQuery NVARCHAR(4000),
    DaysOld INT NOT NULL CHECK (DaysOld > 0),
    StartTime TIME NOT NULL,
    EndTime TIME NOT NULL CHECK (EndTime > StartTime),
    BatchSize INT NOT NULL CHECK (BatchSize > 0),
    ForceCascade BOOLEAN DEFAULT true
);

CREATE TABLE dbo.CleanupLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    TableName NVARCHAR(128),
    DeletedRows INT,
    ExecutionTime DATETIME,
    ErrorMessage NVARCHAR(MAX)
) WITH (SCHEMABINDING);

-- Insert a sample row into the CleanupConfig table
INSERT INTO dbo.CleanupConfig (TableName, IdColumn, DateTimeColumn, AdditionalQuery, DaysOld, StartTime, EndTime, BatchSize, ForceCascade)
VALUES
    ('[UiPath].[dbo].[QueueItems]',          'Id',    'CreationTime',          'status=3',            60,  '01:00:00', '05:00:00',  1000, true),
    ('[UiPath].[dbo].[Logs]',                'Id',    'TimeStamp',             '1=1',                 180, '01:00:00', '05:00:00', 10000, false),
    ('[UiPath].[dbo].[RobotLicenseLogs]',    'Id',    'EndDate',               'EndDate is not null', 60,  '01:00:00', '05:00:00', 10000, false),
    ('[UiPath].[dbo].[TenantNotifications]', 'Id',    'CreationTime',          '1=1',                 60,  '01:00:00', '05:00:00', 10000, true),
    ('[UiPath].[dbo].[jobs]',                'Id',    'CreationTime',          'State in (4, 5, 6)',  60,  '01:00:00', '05:00:00', 10000, false),
    ('[UiPath].[dbo].[AuditLogs]',           'Id',    'ExecutionTime',         '1=1',                 180, '01:00:00', '05:00:00', 10000, true),
    ('[UiPath].[dbo].[Tasks]',               'Id',    'DeletionTime',          'IsDeleted = 1',       60,  '01:00:00', '05:00:00', 10000, false),
    ('[UiPath].[dbo].[Tasks]',               'Id',    'LastModificationTime',  'Status = 2',          60,  '01:00:00', '05:00:00', 10000, false),
    ('[UiPath].[dbo].[Sessions]',            'Id',    'ReportingTime',         '1=1',                 180, '01:00:00', '05:00:00', 10000, true),
    ('[UiPath].[dbo].[Ledger]',              'Id',    'CreationTime',          '1=1',                 180, '01:00:00', '05:00:00', 10000, false),
    ('[UiPath].[dbo].[LedgerDeliveries]',    'Id',    'LastUpdatedTime',       '1=1',                 180, '01:00:00', '05:00:00', 10000, false),
    ('[UiPath].[dbo].[CleanupLog]',          'LogID', 'ExecutionTime',         '1=1',                 30,  '01:00:00', '05:00:00',  5000, false);
GO

-- Create the cleanup stored procedure
CREATE PROCEDURE dbo.HourlyCleanupProcess
AS
BEGIN
    -- Cleanup process with error handling
    DECLARE @TableName NVARCHAR(128), @IdColumn NVARCHAR(128), @DateTimeColumn NVARCHAR(128), @AdditionalQuery NVARCHAR(4000), @StartTime TIME, @EndTime TIME, @BatchSize INT, @DaysOld INT, @ForceCascade BOOLEAN;

    DECLARE @DeletedRows INT, @TotalDeletedRows INT, @CurrentTime TIME;
    DECLARE @DynamicSQL NVARCHAR(MAX), @ErrorMessage NVARCHAR(4000);

    DECLARE ConfigCursor CURSOR FOR
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
                    SET @DynamicSQL = N'SELECT TOP ' + CAST(@BatchSize AS NVARCHAR(10)) + N' ' + CAST(@IdColumn AS NVARCHAR(10)) + N' AS IdToDelete INTO #TempDeletedIds FROM ' + QUOTENAME(@TableName) + N' WHERE ' + @AdditionalQuery + N' AND DATEDIFF(DAY, ' + CAST(@DaysOld AS NVARCHAR(10)) + N', GETDATE()) > ' + CAST(@DaysOld AS NVARCHAR(10));
                    EXEC sp_executesql @DynamicSQL;
                    SET @DeletedRows = @@ROWCOUNT;
                    IF @DeletedRows > 0
                    BEGIN
                        IF @ForceCascade
                        BEGIN
                            -- go one level deeper in the DB schema
                            DECLARE @FKName NVARCHAR(255);
                            DECLARE @FKTableName NVARCHAR(255);
                            DECLARE @DynamicSQL NVARCHAR(MAX);

                            DECLARE ReferenceCursor CURSOR FOR 
                            SELECT FK.name AS FKName, PT.name AS FKTableName FROM sys.foreign_keys AS FK INNER JOIN sys.tables AS PT ON FK.parent_object_id = PT.object_id INNER JOIN sys.tables AS RT ON FK.referenced_object_id = RT.object_id
                            WHERE RT.name = @TableName;

                            OPEN ReferenceCursor;

                            FETCH NEXT FROM ReferenceCursor INTO @FKName, @FKTableName;

                            WHILE @@FETCH_STATUS = 0
                            BEGIN
                                SET @DynamicSQL = N'DELETE FROM ' + QUOTENAME(@FKTableName) + N' WHERE ' + QUOTENAME(@FKName) + N' IN (SELECT IdToDelete FROM #TempDeletedIds)';
                                EXEC sp_executesql @DynamicSQL;
                                FETCH NEXT FROM ReferenceCursor INTO @FKName, @FKTableName;
                            END;

                            CLOSE ReferenceCursor;
                            DEALLOCATE ReferenceCursor;
                        END
                        -- delete records
                        SET @DynamicSQL = N'DELETE FROM ' + QUOTENAME(@TableName) + N' WHERE ' + CAST(@IdColumn AS NVARCHAR(10)) + N' IN (SELECT IdToDelete FROM #TempDeletedIds)';
                        EXEC sp_executesql @DynamicSQL;
                    END
                    DROP TABLE #TempDeletedIds
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
            PRINT 'Error while processing table ' + QUOTENAME(@TableName) + ': ' + @ErrorMessage;
        END CATCH;

        FETCH NEXT FROM ConfigCursor INTO @TableName, @DateTimeColumn, @AdditionalQuery, @StartTime, @EndTime, @BatchSize, @DaysOld;
    END

    CLOSE ConfigCursor;
    DEALLOCATE ConfigCursor;

END;
GO

USE msdb;
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
DECLARE @JobStatus INT;

-- Check if the job is already running
SELECT @JobStatus = CASE WHEN EXISTS (
    SELECT * 
    FROM msdb.dbo.sysjobs j
    INNER JOIN msdb.dbo.sysjobactivity ja ON j.job_id = ja.job_id
    WHERE j.name = ''HourlyCleanup''
        AND ja.run_requested_date IS NOT NULL
        AND ja.stop_execution_date IS NULL
) THEN 1 ELSE 0 END;

IF @JobStatus = 1
BEGIN
    PRINT ''HourlyCleanup job is already running. Exiting script.'';
    RETURN;
END;

DECLARE @DatabaseName NVARCHAR(128);
SET @DatabaseName = ''' + @DatabaseName + ''';

-- Use dynamic SQL to switch to the desired database and execute the stored procedure
EXEC (''USE '' + QUOTENAME(@DatabaseName) + ''; EXEC dbo.HourlyCleanupProcess;'');
',
    @database_name = N'msdb', -- Set the context to msdb database as we are using msdb.dbo.sysjobs and msdb.dbo.sysjobactivity
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