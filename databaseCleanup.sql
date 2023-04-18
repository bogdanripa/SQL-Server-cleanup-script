DECLARE @DatabaseName NVARCHAR(128);
SET @DatabaseName = N'YourDatabaseName'; -- change this to match your database name

USE @DatabaseName;
GO

-- Create required cleanup tables
CREATE TABLE dbo.CleanupConfig (
    TableName NVARCHAR(128),
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
    DateTimeColumn NVARCHAR(128),
    AdditionalQuery NVARCHAR(4000),
    DeletedRows INT,
    ExecutionTime DATETIME,
    ErrorMessage NVARCHAR(MAX)
) WITH (SCHEMABINDING);

-- Insert a sample row into the CleanupConfig table
INSERT INTO dbo.CleanupConfig (TableName, DateTimeColumn, AdditionalQuery, DaysOld, StartTime, EndTime, BatchSize, ForceCascade)
VALUES
    ('dbo.QueueItems', 'ProcessedOn', 'Processed = 1', 180, '01:00:00', '04:59:00', 1000, true), -- delete processed queue items that are older than 180 days in batches of 1000 rows
    ('dbo.CleanupLog', 'ExecutionTime', 'true', 30, '01:00:00', '04:59:00', 5000, true); -- delete cleanup logs that are older than 30 days in batches of 5000 rows
GO

-- Create the cleanup stored procedure
CREATE PROCEDURE dbo.HourlyCleanupProcess
AS
BEGIN
    -- Cleanup process with error handling
    DECLARE @TableName NVARCHAR(128), @DateTimeColumn NVARCHAR(128), @AdditionalQuery NVARCHAR(4000), @StartTime TIME, @EndTime TIME, @BatchSize INT, @DaysOld INT, @ForceCascade BOOLEAN;

    DECLARE @DeletedRows INT, @TotalDeletedRows INT, @CurrentTime TIME;
    DECLARE @DynamicSQL NVARCHAR(MAX), @ErrorMessage NVARCHAR(4000);

    DECLARE ConfigCursor CURSOR FOR
        SELECT TableName, DateTimeColumn, AdditionalQuery, StartTime, EndTime, BatchSize, DaysOld, ForceCascade
        FROM dbo.CleanupConfig;

    OPEN ConfigCursor;

    FETCH NEXT FROM ConfigCursor INTO @TableName, @DateTimeColumn, @AdditionalQuery, @StartTime, @EndTime, @BatchSize, @DaysOld, @ForceCascade;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            SET @DeletedRows = 1;
            SET @TotalDeletedRows = 0;
            SET @CurrentTime = CAST(GETDATE() AS TIME);

            WHILE @CurrentTime >= @StartTime AND @CurrentTime <= @EndTime AND @DeletedRows > 0
            BEGIN
                SET @DynamicSQL = N'';
                IF @ForceCascade
                    BEGIN
                    -- force on delete cascade
                    SET @DynamicSQL += N'WITH DeleteCascade AS (' +
                        N'    SELECT T.Name AS TableName, C.Name AS ColumnName' +
                        N'    FROM sys.foreign_key_columns F' +
                        N'    JOIN sys.tables T ON T.object_id = F.referenced_object_id' +
                        N'    JOIN sys.columns C ON C.object_id = F.referenced_object_id AND C.column_id = F.referenced_column_id' +
                        N'    WHERE OBJECT_NAME(F.parent_object_id) = ''' + @TableName + N''' UNION ALL' +
                        N'    SELECT T.Name AS TableName, C.Name AS ColumnName' +
                        N'    FROM DeleteCascade DC' +
                        N'    JOIN sys.foreign_key_columns F ON F.referenced_object_id = OBJECT_ID(DC.TableName)' +
                        N'    JOIN sys.tables T ON T.object_id = F.parent_object_id' +
                        N'    JOIN sys.columns C ON C.object_id = F.parent_object_id AND C.column_id = F.parent_column_id' +
                        N'    WHERE NOT EXISTS (' +
                        N'        SELECT 1 FROM DeleteCascade WHERE TableName = T.Name AND ColumnName = C.Name' +
                        N'    )' +
                        N');';
                    END
                SET @DynamicSQL += N'
                    DELETE TOP (' + CAST(@BatchSize AS NVARCHAR(10)) + N') FROM ' + QUOTENAME(@TableName) + 
                    N' WHERE ' + @AdditionalQuery + N' AND ' + QUOTENAME(@DateTimeColumn) + N' < DATEADD(DAY, -' + CAST(@DaysOld AS NVARCHAR(10)) + N', GETDATE());';

                EXEC sp_executesql @DynamicSQL;
                SET @DeletedRows = @@ROWCOUNT;

                -- Wait for 5 seconds before running the next batch if at least 1 row was deleted
                IF @DeletedRows  > 0
                BEGIN
                    SET @TotalDeletedRows = @TotalDeletedRows + @DeletedRows;
                    WAITFOR DELAY '00:00:05';
                END
                SET @CurrentTime = CAST(GETDATE() AS TIME);
            END
            IF @TotalDeletedRows > 0
                INSERT INTO dbo.CleanupLog (TableName, DateTimeColumn, AdditionalQuery, DeletedRows, ExecutionTime, ErrorMessage) VALUES (@TableName, @DateTimeColumn, @AdditionalQuery, @TotalDeletedRows, GETDATE(), NULL);

        END TRY
        BEGIN CATCH
            -- Capture the error message and continue with the next iteration
            SET @ErrorMessage = ERROR_MESSAGE();
            INSERT INTO dbo.CleanupLog (TableName, DateTimeColumn, AdditionalQuery, DeletedRows, ExecutionTime, ErrorMessage) VALUES (@TableName, @DateTimeColumn, @AdditionalQuery, NULL, GETDATE(), @ErrorMessage);
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