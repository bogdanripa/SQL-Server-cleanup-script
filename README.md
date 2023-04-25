# SQL Server Hourly Cleanup Script for Old Rows

This script is designed to clean up old rows in specific tables within a SQL Server database on an hourly basis. The cleanup process is managed by a stored procedure and an SQL Server Agent job, which automatically runs the cleanup process every hour. The script includes error handling and logging to ensure reliable operation.

Features:

1. Customizable cleanup configuration: The script creates a CleanupConfig table where users can define the target tables, date/time columns, additional conditions for deletion, start and end times for the cleanup window, batch sizes, and the age threshold for old rows.

2.The deleted rows are backed up in the "Archives" database, where corresponding tables are automatically created. If the schema of a target table changes, another corresponding archived table is created. This is achieved by appending a hash of all column names at the end of the corresponding table name.

3. Error handling and logging: The script logs the number of deleted rows and any errors encountered during the cleanup process in a CleanupLog table. This makes it easier to monitor the script's performance and troubleshoot issues if needed.

4. Batch processing: The cleanup process deletes rows in batches to reduce the impact on system performance. The batch size can be configured for each table in the CleanupConfig table.

5. Constraints for validation: The script uses CHECK constraints to ensure valid values for DaysOld, StartTime, and EndTime in the CleanupConfig table.

6. SQL Server Agent job: The script creates an SQL Server Agent job that runs the cleanup process every hour. The job checks if it is already running to prevent overlapping executions.

How to use:

1. Execute the script to create the required tables, stored procedure, and SQL Server Agent job.
2. Customize the CleanupConfig table by adding rows for the tables and columns you want to clean up, along with other required parameters.
3. The SQL Server Agent job will now run the cleanup process every hour within the specified time window.

This script helps maintain a clean and efficient database by automatically removing old and unnecessary rows from the specified tables.



