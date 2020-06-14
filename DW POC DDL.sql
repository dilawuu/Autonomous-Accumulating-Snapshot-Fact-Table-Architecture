CREATE DATABASE [POC_DW];
GO
USE POC_DW;
GO
-- Dyanmic script to drop constraints and tables, should rebuilding the structure be required
--DECLARE @SQL NVARCHAR(150), @Constraint VARCHAR(75), @Table VARCHAR(50), @Increment TINYINT= 0, @Schema VARCHAR(5)= '';
--WHILE @Increment <
--(
--    SELECT COUNT(*)
--    FROM
--    (
--        SELECT CASE
--                   WHEN OBJECTPROPERTY([CONSTID], 'CNSTISDISABLED') = 0
--                   THEN 'ENABLED'
--                   ELSE 'DISABLED'
--               END AS STATUS, 
--               OBJECT_NAME([CONSTID]) AS [CONSTRAINT_NAME], 
--               OBJECT_NAME([FKEYID]) AS [TABLE_NAME], 
--               COL_NAME([FKEYID], [FKEY]) AS [COLUMN_NAME], 
--               OBJECT_NAME([RKEYID]) AS [REFERENCED_TABLE_NAME], 
--               COL_NAME([RKEYID], [RKEY]) AS [REFERENCED_COLUMN_NAME]
--        FROM [SYSFOREIGNKEYS]
--    ) AS [x1]
--)
--    BEGIN
--        SET @Constraint =
--        (
--            SELECT OBJECT_NAME([CONSTID]) AS [CONSTRAINT_NAME]
--            FROM [SYSFOREIGNKEYS]
--            ORDER BY [fkeyid], 
--                     [constid]
--            OFFSET @Increment ROWS FETCH NEXT 1 ROW ONLY
--        );
--        SET @Schema =
--        (
--            SELECT [s].[Name]
--            FROM [SYSFOREIGNKEYS] AS [fk]
--                 INNER JOIN [sys].[tables] AS [T] ON [fk].[fkeyid] = [T].object_id
--                 INNER JOIN [sys].[schemas] AS [s] ON [T].schema_id = [s].schema_id
--            ORDER BY [fkeyid], 
--                     [constid]
--            OFFSET @Increment ROWS FETCH NEXT 1 ROW ONLY
--        );
--        SET @Table =
--        (
--            SELECT OBJECT_NAME([FKEYID]) AS [TABLE_NAME]
--            FROM [SYSFOREIGNKEYS] AS [fk]
--                 INNER JOIN [sys].[tables] AS [T] ON [fk].[fkeyid] = [T].object_id
--                 INNER JOIN [sys].[schemas] AS [s] ON [T].schema_id = [s].schema_id
--            ORDER BY [fkeyid], 
--                     [constid]
--            OFFSET @Increment ROWS FETCH NEXT 1 ROW ONLY
--        );
--        SET @SQL = N'ALTER TABLE ' + @Schema + N'.' + @Table + N' DROP CONSTRAINT ' + @Constraint;
--        EXECUTE (@SQL);
--    END;
--GO
--DECLARE @SQL NVARCHAR(150), @Constraint VARCHAR(75), @Table VARCHAR(50), @Increment TINYINT= 0, @Schema VARCHAR(5)= '';
--WHILE @Increment <
--(
--    SELECT COUNT(*)
--    FROM sys.tables
--)
--    BEGIN
--        SET @Schema =
--        (
--            SELECT s.name
--            FROM [sys].[tables] AS [T]
--                 INNER JOIN [sys].[schemas] AS [s] ON [T].schema_id = [s].schema_id
--            ORDER BY t.object_id
--            OFFSET @Increment ROWS FETCH NEXT 1 ROW ONLY
--        );
--        SET @Table =
--        (
--            SELECT t.name
--            FROM [sys].[tables] AS [T]
--                 INNER JOIN [sys].[schemas] AS [s] ON [T].schema_id = [s].schema_id
--            ORDER BY t.object_id
--            OFFSET @Increment ROWS FETCH NEXT 1 ROW ONLY
--        );
--        SET @SQL = N'DROP TABLE ' + @Schema + N'.' + @Table;
--        EXECUTE (@SQL);
--    END;
--GO
CREATE SCHEMA mst AUTHORIZATION dbo;
GO
CREATE SCHEMA stg AUTHORIZATION dbo;
GO
CREATE SCHEMA raw AUTHORIZATION dbo;
GO
-- Staging Maintenance will store metadata for every raw/staging table, using a DB-level trigger. This will be useful when auto-truncating raw and staging as part of the ETL process.

CREATE TABLE [mst].[stagingMaintenance]
([TableID]             INT IDENTITY(1, 1), 
 [TableName]           VARCHAR(150), 
 [DateTimeCreated]     DATETIME2(0) DEFAULT NULL, 
 [CreatedBy]           VARCHAR(100), 
 [DateTimeLastUpdated] DATETIME2(0) DEFAULT NULL, 
 [LastUpdatedBy]       VARCHAR(100)
);
GO

-- tr_newStaging will fire only when there's a CREATE or ALTER DML executed for a table that's part of the raw/staging schema. This will help keep a record of all tables that require truncation as part of the DW daily maintenance job.
-- Yes, we truncate both raw and staging as we keep a record of the data in Azure DataLake

CREATE OR ALTER TRIGGER [tr_newStaging] ON DATABASE FOR CREATE_TABLE, ALTER_TABLE AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Username VARCHAR(50)=
    (
        SELECT [nt_username]
        FROM [sysprocesses]
        WHERE [spid] = @@Spid
    ), @Data XML= EVENTDATA();
    IF @Data.value('(/EVENT_INSTANCE/SchemaName)[1]', 'nvarchar(max)') = 'stg'
    AND @Data.value('(/EVENT_INSTANCE/EventType)[1]', 'nvarchar(max)') = 'CREATE_TABLE'
    OR @Data.value('(/EVENT_INSTANCE/SchemaName)[1]', 'nvarchar(max)') = 'raw'
    AND @Data.value('(/EVENT_INSTANCE/EventType)[1]', 'nvarchar(max)') = 'CREATE_TABLE'
        BEGIN
            INSERT INTO [mst].[stagingMaintenance]
            VALUES
            (@Data.value('(/EVENT_INSTANCE/ObjectName)[1]', 'nvarchar(max)'), 
             @Data.value('(/EVENT_INSTANCE/PostTime)[1]', 'nvarchar(max)'), 
             @Data.value('(/EVENT_INSTANCE/LoginName)[1]', 'nvarchar(max)'), 
             NULL, 
             NULL
            );
        END;
        ELSE
        BEGIN
            IF @Data.value('(/EVENT_INSTANCE/SchemaName)[1]', 'nvarchar(max)') = 'stg'
            AND @Data.value('(/EVENT_INSTANCE/EventType)[1]', 'nvarchar(max)') = 'ALTER_TABLE'
        OR @Data.value('(/EVENT_INSTANCE/SchemaName)[1]', 'nvarchar(max)') = 'raw'
            AND @Data.value('(/EVENT_INSTANCE/EventType)[1]', 'nvarchar(max)') = 'ALTER_TABLE'
                BEGIN
                    UPDATE [mst].[stagingMaintenance]
                      SET 
                          [DateTimeLastUpdated] = @Data.value('(/EVENT_INSTANCE/PostTime)[1]', 'nvarchar(max)'), 
                          [LastUpdatedBy] = @Data.value('(/EVENT_INSTANCE/LoginName)[1]', 'nvarchar(max)')
                    WHERE [TableName] = @Data.value('(/EVENT_INSTANCE/ObjectName)[1]', 'nvarchar(max)');
                END;
        END;
END;
 GO

-- Creating date dimension
CREATE TABLE [dbo].[Dim_Date]
([DateKeySK]               INT NOT NULL UNIQUE, 
 [Date]                    [DATE] NULL, 
 Day                       TINYINT NULL, 
 [DaySuffix]               CHAR(2) NULL, 
 [Weekday]                 TINYINT NULL, 
 [WeekDayName]             VARCHAR(10) NULL, 
 [WeekDayName_Short]       CHAR(3) NULL, 
 [WeekDayName_FirstLetter] CHAR(1) NULL, 
 [DOWInMonth]              TINYINT NULL, 
 [DayOfYear]               [SMALLINT] NULL, 
 [WeekOfMonth]             TINYINT NULL, 
 [WeekOfYear]              TINYINT NULL, 
 [Month]                   TINYINT NULL, 
 MonthName                 VARCHAR(10) NULL, 
 [MonthName_Short]         CHAR(3) NULL, 
 [MonthName_FirstLetter]   CHAR(1) NULL, 
 Quarter                   TINYINT NULL, 
 [QuarterName]             VARCHAR(6) NULL, 
 [Year]                    INT NULL, 
 [MMYYYY]                  CHAR(6) NULL, 
 [MonthYear]               CHAR(7) NULL, 
 [IsWeekend]               BIT NULL, 
 [IsHoliday]               BIT NULL, 
 [HolidayName]             VARCHAR(20) NULL, 
 [SpecialDays]             VARCHAR(20) NULL, 
 [FinancialYear]           INT NULL, 
 [FinancialQuarter]        INT NULL, 
 [FinancialMonth]          INT NULL, 
 [FirstDateofYear]         DATE NULL, 
 [LastDateofYear]          DATE NULL, 
 [FirstDateofQuater]       DATE NULL, 
 [LastDateofQuater]        DATE NULL, 
 [FirstDateofMonth]        DATE NULL, 
 [LastDateofMonth]         DATE NULL, 
 [FirstDateofWeek]         DATE NULL, 
 [LastDateofWeek]          DATE NULL, 
 [CurrentYear]             [SMALLINT] NULL, 
 [CurrentQuater]           [SMALLINT] NULL, 
 [CurrentMonth]            [SMALLINT] NULL, 
 [CurrentWeek]             [SMALLINT] NULL, 
 [CurrentDay]              [SMALLINT] NULL, 
 PRIMARY KEY CLUSTERED([DateKeySK] ASC)
);

-- Creating time dimension
CREATE TABLE [dbo].[Dim_Time]
([TimeKeySK]      INT NOT NULL UNIQUE, 
 [Time]           TIME(0) NULL, 
 [Hour12]         TINYINT NULL, 
 [Hour24]         TINYINT NULL, 
 [MinuteOfHour]   TINYINT NULL, 
 [SecondOfMinute] TINYINT NULL, 
 [ElapsedMinutes] [SMALLINT] NULL, 
 [ElapsedSeconds] INT NULL, 
 [AMPM]           CHAR(2) NULL, 
 [HHMMSS]         CHAR(8) NULL, 
 CONSTRAINT [pk_dimtime] PRIMARY KEY CLUSTERED([TimeKeySK])
);
  GO
CREATE TABLE [dbo].[dim_ManagerStaff]
([StaffSK]       INT IDENTITY(1, 1) PRIMARY KEY, 
 [SourceStaffID] VARCHAR(50), 
 [StaffName]     VARCHAR(100), 
 [Role]          VARCHAR(75), 
 [ImageURL]      VARCHAR(150), 
 [StartDate]     DATE, 
 [EndDate]       DATE, 
 [ValidFrom]     DATETIME2(0) DEFAULT GETDATE(), 
 [ValidTo]       DATETIME2(0) DEFAULT NULL, 
 [CurrentFlag]   BIT DEFAULT 1
);
  GO
CREATE TABLE [dbo].[dim_EmployeeStaff]
([StaffSK]       INT IDENTITY(1, 1) PRIMARY KEY, 
 [SourceStaffID] VARCHAR(50), 
 [StaffName]     VARCHAR(100), 
 [Role]          VARCHAR(75), 
 [ImageURL]      VARCHAR(150), 
 [StartDate]     DATE, 
 [EndDate]       DATE, 
 [ManagerID]     INT FOREIGN KEY REFERENCES [dbo].[dim_ManagerStaff]([StaffSK]), 
 [ValidFrom]     DATETIME2(0) DEFAULT GETDATE(), 
 [ValidTo]       DATETIME2(0) DEFAULT NULL, 
 [CurrentFlag]   BIT DEFAULT 1
);
  GO
CREATE TABLE [dbo].[dim_Status]
([StatusSK]          INT IDENTITY(1, 1) PRIMARY KEY, 
 [SourceStatusID]    VARCHAR(50), 
 [StatusName]        VARCHAR(100), 
 [StatusDescription] VARCHAR(100), 
 [ValidFrom]         DATETIME2(0) DEFAULT GETDATE(), 
 [ValidTo]           DATETIME2(0) DEFAULT NULL, 
 [CurrentFlag]       BIT DEFAULT 1
);
  GO
CREATE TABLE [dbo].[dim_Location]
([LocationSK]       INT IDENTITY(1, 1) PRIMARY KEY, 
 [SourceLocationID] VARCHAR(50), 
 [OfficeName]       VARCHAR(100), 
 [City]             VARCHAR(100), 
 [County]           VARCHAR(50), 
 [OfficePostcode]   VARCHAR(20), 
 [ValidFrom]        DATETIME2(0) DEFAULT GETDATE(), 
 [ValidTo]          DATETIME2(0) DEFAULT NULL, 
 [CurrentFlag]      BIT DEFAULT 1
);
GO
CREATE TABLE [dbo].[dim_WorkItem]
([WorkItemSK]       INT IDENTITY(1, 1) PRIMARY KEY, 
 [SourceWorkItemID] VARCHAR(50), 
 [StartDate]        DATE, 
 [TargetDate]       DATE, 
 [CompletedDate]    DATE, 
 [SLA]              INT, 
 [Priority]         VARCHAR(256), 
 [ValidFrom]        DATETIME2(0) DEFAULT GETDATE(), 
 [ValidTo]          DATETIME2(0) DEFAULT NULL, 
 [CurrentFlag]      BIT DEFAULT 1
);
  GO
CREATE TABLE [dbo].[fact_WorkItemStateHistory]
([RN]                  INT IDENTITY(1, 1), 
 [StaffSK]             INT FOREIGN KEY REFERENCES [dim_EmployeeStaff]([StaffSK]), 
 [LocationSK]          INT FOREIGN KEY REFERENCES [dim_Location]([LocationSK]), 
 [StatusSK]            INT FOREIGN KEY REFERENCES [dim_Status]([StatusSK]), 
 [WorkItemSK]          INT FOREIGN KEY REFERENCES [dim_WorkItem]([WorkItemSK]), 
 [SnapshotStartDateSK] INT FOREIGN KEY REFERENCES [dim_Date]([DateKeySK]), 
 [SnapshotStartTimeSK] INT FOREIGN KEY REFERENCES [dim_Time]([TimeKeySK]), 
 [SnapshotEndDateSK]   INT FOREIGN KEY REFERENCES [dim_Date]([DateKeySK]), 
 [SnapshotEndTimeSK]   INT FOREIGN KEY REFERENCES [dim_Time]([TimeKeySK]), 
 [SnapshotCurrent]     BIT DEFAULT 1
);
GO
ALTER TABLE [dbo].[fact_WorkItemStateHistory]
ADD CONSTRAINT [UQ_factWISH] UNIQUE([StaffSK], [LocationSK], [StatusSK], [WorkItemSK], [SnapshotStartDateSK], [SnapshotStartTimeSK], [SnapshotEndDateSK], [SnapshotEndTimeSK], [SnapshotCurrent]);
GO
CREATE TABLE [stg].[dim_ManagerStaff]
([StaffSK]       INT, 
 [SourceStaffID] VARCHAR(50), 
 [StaffName]     VARCHAR(100), 
 [Role]          VARCHAR(75), 
 [ImageURL]      NVARCHAR(150), 
 [StartDate]     DATE, 
 [EndDate]       DATE, 
 [ValidFrom]     DATETIME2(0) DEFAULT GETDATE(), 
 [ValidTo]       DATETIME2(0) DEFAULT NULL, 
 [CurrentFlag]   BIT DEFAULT 1
);
CREATE TABLE [stg].[dim_EmployeeStaff]
([StaffSK]       INT, 
 [SourceStaffID] VARCHAR(50), 
 [StaffName]     VARCHAR(100), 
 [Role]          VARCHAR(75), 
 [ImageURL]      NVARCHAR(150), 
 [StartDate]     DATE, 
 [EndDate]       DATE, 
 [ManagerID]     INT, 
 [ValidFrom]     DATETIME2(0) DEFAULT GETDATE(), 
 [ValidTo]       DATETIME2(0) DEFAULT NULL, 
 [CurrentFlag]   BIT DEFAULT 1
);
CREATE TABLE [stg].[dim_Status]
([StatusSK]          INT, 
 [SourceStatusID]    VARCHAR(50), 
 [StatusName]        VARCHAR(100), 
 [StatusDescription] VARCHAR(100), 
 [ValidFrom]         DATETIME2(0) DEFAULT GETDATE(), 
 [ValidTo]           DATETIME2(0) DEFAULT NULL, 
 [CurrentFlag]       BIT DEFAULT 1
);
CREATE TABLE [stg].[dim_Location]
([LocationSK]       INT, 
 [SourceLocationID] VARCHAR(50), 
 [OfficeName]       VARCHAR(100), 
 [City]             VARCHAR(100), 
 [County]           VARCHAR(50), 
 [OfficePostcode]   VARCHAR(20), 
 [ValidFrom]        DATETIME2(0) DEFAULT GETDATE(), 
 [ValidTo]          DATETIME2(0) DEFAULT NULL, 
 [CurrentFlag]      BIT DEFAULT 1
);
CREATE TABLE [stg].[dim_WorkItem]
([WorkItemSK]       INT, 
 [SourceWorkItemID] VARCHAR(50), 
 [StartDate]        DATE, 
 [TargetDate]       DATE, 
 [CompletedDate]    DATE, 
 [SLA]              INT, 
 [Priority]         VARCHAR(256), 
 [ValidFrom]        DATETIME2(0) DEFAULT GETDATE(), 
 [ValidTo]          DATETIME2(0) DEFAULT NULL, 
 [CurrentFlag]      BIT DEFAULT 1
);
CREATE TABLE [stg].[fact_WorkItemStateHistory]
([StaffSK]             INT, 
 [LocationSK]          INT, 
 [StatusSK]            INT, 
 [WorkItemSK]          INT, 
 [SnapshotStartDateSK] INT, 
 [SnapshotStartTimeSK] INT, 
 [SnapshotEndDateSK]   INT, 
 [SnapshotEndTimeSK]   INT, 
 [SnapshotCurrent]     BIT DEFAULT 1
);
GO
CREATE TABLE [raw].[WorkItem]
([WorkItemID]      INT NOT NULL, 
 [Description]     VARCHAR(256) NOT NULL, 
 [SLA]             TINYINT NOT NULL, 
 [Subject]         VARCHAR(256) NOT NULL, 
 [PriorityID]      TINYINT NOT NULL, 
 [StartDate]       DATETIME NULL, 
 [TargetDate]      DATETIME NULL, 
 [CompletedDate]   DATETIME NULL, 
 [DaysPausedFor]   TINYINT NULL, 
 [PausedUntilDate] DATETIME NULL, 
 [IsNew]           BIT NULL, 
 [AssignedTo]      INT NOT NULL
)
ON [PRIMARY];
GO
CREATE TABLE [raw].[Location]
(LocationSK INT, 
 OfficeName VARCHAR(100), 
 Country    VARCHAR(50), 
 City       VARCHAR(50)
);
GO
CREATE TABLE [raw].[Priority]
([PriorityID]  TINYINT NOT NULL, 
 [Description] VARCHAR(256) NOT NULL
)
ON [PRIMARY];
GO
CREATE TABLE [raw].[CurrentStatus]
([StatusID]    TINYINT NOT NULL, 
 [Description] VARCHAR(256) NOT NULL
)
ON [PRIMARY];
GO

CREATE TABLE [raw].[EmployeeStaff]
([Staff_ID]   INT NULL, 
 [Forename]   VARCHAR(20) NULL, 
 [Surname]    VARCHAR(20) NULL, 
 [Initials]   VARCHAR(3) NULL, 
 [Role]       INT NULL, 
 [imageurl]   VARCHAR(150), 
 [Location]   INT NULL, 
 [Start_Date] DATETIME NULL, 
 [End_Date]   DATETIME NULL
)
ON [PRIMARY];
GO
CREATE TABLE [raw].[ManagerStaff]
([Staff_ID]   INT NULL, 
 [Forename]   VARCHAR(20) NULL, 
 [Surname]    VARCHAR(20) NULL, 
 [Initials]   VARCHAR(3) NULL, 
 [Role]       INT NULL, 
 [imageurl]   VARCHAR(150), 
 [Location]   INT NULL, 
 [Start_Date] DATETIME NULL, 
 [End_Date]   DATETIME NULL
)
ON [PRIMARY];
GO
CREATE TABLE [raw].[StaffRole]
([Role_ID]     INT NOT NULL, 
 [Role]        VARCHAR(100) NULL, 
 [CreatedDate] DATETIME NULL, 
 [CreatedBy]   VARCHAR(100) NULL
)
ON [PRIMARY];
GO
CREATE TABLE [raw].[WorkItemStateHistory]
([WorkItemStateHistoryID] INT NULL, 
 [WorkItemID]             INT NOT NULL, 
 [StatusID]               TINYINT NOT NULL, 
 [LocationID]             TINYINT NOT NULL, 
 [StartDate]              DATETIME NULL, 
 [EndDate]                DATETIME NULL, 
 [ElapsedDays]            INT NULL
);
GO
CREATE TABLE [mst].[DimensionMergeComponents]
([SourceTable]     NVARCHAR(100), 
 [TargetTable]     NVARCHAR(100), 
 [Condition]       NVARCHAR(500), 
 [SCDType]         TINYINT, 
 [ChangingColumns] NVARCHAR(300), 
 [MergeVariable]   NVARCHAR(1500)
);
GO


CREATE TABLE [mst].[FactMergeComponents]
([ComponentNumber] INT, 
 [SourceTable]     VARCHAR(200), 
 [TargetTable]     VARCHAR(200), 
 [MergeColumns]    VARCHAR(500), 
 [MergeCondition]  VARCHAR(2000), 
 [MergeException]  VARCHAR(1000), 
 [MergeStatement] AS IIF([ComponentNumber] = 1, 'MERGE ' + [TargetTable] + ' AS [target] USING ' + [SourceTable] + ' AS [source] ON ' + [MergeCondition] + IIF([MergeException] IS NULL, '', ' WHEN NOT MATCHED AND NOT EXISTS (' + [MergeException] + ')') + ' THEN INSERT (' + [MergeColumns] + ') VALUES (' + [MergeColumns] + ');', IIF([ComponentNumber] = 2, ' INSERT INTO ##x1 SELECT ' + [MergeColumns] + ' FROM ( MERGE ' + [TargetTable] + ' AS [target] USING ' + [SourceTable] + ' AS [source] ON ' + [MergeCondition] + ' WHEN MATCHED THEN UPDATE SET [target].[SnapshotEndDateSK] = [source].[SnapshotStartDateSK], [target].[SnapshotEndTimeSK] = [source].[SnapshotStartTimeSK], [target].[SnapshotCurrent] = 0 OUTPUT $ACTION [ActionOut], ' + REPLACE(CONCAT('[source].', [MergeColumns]), ', ', ', [source].') + ') AS [MergeOut] WHERE [MergeOut].[ActionOut] = ''UPDATE''; INSERT INTO ' + [TargetTable] + '(' + [MergeColumns] + ') SELECT ' + [MergeColumns] + ' FROM [##x1];', ' MERGE ' + [TargetTable] + ' AS [target] USING ' + [SourceTable] + ' AS [source] ON ' + [MergeCondition] + ' WHEN MATCHED THEN UPDATE SET [target].[SnapshotEndDateSK] = [source].[SnapshotEndDateSK], [target].[SnapshotEndTimeSK] = [source].[SnapshotEndTimeSK], [target].[SnapshotCurrent] = [source].[SnapshotCurrent];'))
);
GO

CREATE OR ALTER PROCEDURE [mst].[AddFactMergeComponent] @SourceTable       NVARCHAR(100), 
                                                        @TargetTable       NVARCHAR(100), 
                                                        @ConditionColumns1 VARCHAR(500), 
                                                        @ConditionColumns2 VARCHAR(500), 
                                                        @ExceptionColumns  VARCHAR(100), 
                                                        @MergingColumns    VARCHAR(250)
AS
    BEGIN
        DECLARE @MergeException VARCHAR(1000), @Condition1 VARCHAR(2000), @Condition2 VARCHAR(2000), @Condition3 VARCHAR(2000);
        SET @Condition1 =
        (
            SELECT REPLACE(STRING_AGG(CONCAT(concat('source.', TRIM(value), ''), ' = ', REPLACE(concat('source.', TRIM(value), ''), 'source', 'target')), ', '), ', ', ' AND ')
            FROM STRING_SPLIT(@ConditionColumns1, ',')
        );
        WITH CTE
             AS (SELECT TRIM(value) AS [ct]
                 FROM STRING_SPLIT(@ConditionColumns2, ','))
             SELECT @Condition2 = CONCAT(REPLACE(STRING_AGG(CONCAT(concat('source.', TRIM([ct]), ''), ' = ', REPLACE(concat('source.', TRIM([ct]), ''), 'source', 'target')), ', '), ', ', ' AND '), ' AND [target].[SnapshotCurrent] = 1 AND [source].[snapshotcurrent] = 1 AND [source].[SnapshotStartDateSK] != [target].[SnapshotStartDateSK] AND [source].[SnapshotStartTimeSK] != [target].[SnapshotStartTimeSK]')
             FROM [cte];
        WITH CTE
             AS (SELECT TRIM(value) AS [ct]
                 FROM STRING_SPLIT(@MergingColumns, ',')
                 WHERE value NOT LIKE '%Snapshot%')
             SELECT @Condition3 = CONCAT(REPLACE(STRING_AGG(CONCAT(concat('source.', TRIM([ct]), ''), ' = ', REPLACE(concat('source.', TRIM([ct]), ''), 'source', 'target')), ', '), ', ', ' AND '), ' AND [source].[SnapshotStartDateSK] = [target].[SnapshotStartDateSK] AND [source].[SnapshotStartTimeSK] = [target].[SnapshotStartTimeSK] AND ([source].[SnapshotEndDateSK] > [target].[SnapshotEndDateSK] OR [source].[SnapshotEndTimeSK] > [target].[SnapshotEndTimeSK])')
             FROM [cte];
        SET @MergeException =
        (
            SELECT CONCAT('SELECT ', STRING_AGG(CONCAT('[source].', TRIM(value)), ', '), ' WHERE [source].[SnapshotCurrent] = 1 INTERSECT SELECT ', STRING_AGG(CONCAT('[target].', TRIM(value)), ', '), ' FROM ', @TargetTable, ' AS [target] WHERE [target].[SnapshotCurrent] = 1')
            FROM STRING_SPLIT(@ExceptionColumns, ',')
        );
        INSERT INTO [mst].[FactMergeComponents]
        ([ComponentNumber], 
         [SourceTable], 
         [TargetTable], 
         [MergeColumns], 
         [MergeCondition], 
         [MergeException]
        )
        VALUES
        (1, 
         @SourceTable, 
         @TargetTable, 
         @MergingColumns, 
         @Condition1, 
         @MergeException
        );
        INSERT INTO [mst].[FactMergeComponents]
        ([ComponentNumber], 
         [SourceTable], 
         [TargetTable], 
         [MergeColumns], 
         [MergeCondition], 
         [MergeException]
        )
        VALUES
        (2, 
         @SourceTable, 
         @TargetTable, 
         @MergingColumns, 
         @Condition2, 
         NULL
        );
        INSERT INTO [mst].[FactMergeComponents]
        ([ComponentNumber], 
         [SourceTable], 
         [TargetTable], 
         [MergeColumns], 
         [MergeCondition], 
         [MergeException]
        )
        VALUES
        (3, 
         @SourceTable, 
         @TargetTable, 
         @MergingColumns, 
         @Condition3, 
         NULL
        );
    END;
GO

/* 

[mst].[AddDimensionMergeComponent]

This SP will compute the filter component of the merge statement with the appropriate SCD Type logic

LEGEND: "SourceCOLUMN_NAMEID" is always the Business Key 

Example of SP calling to compute component for SCD Type 1 for column "imgURL": EXECUTE [mst].[AddDimensionMergeComponent] 'stg.dim_Employee', 'dbo.dim_Employee', 'SourceEmployeeID', imgURL, 1
Example of SP calling to compute component for SCD Type 2 for columns "EmployeeName, Role, EndDate, ManagerID": EXECUTE [mst].[AddDimensionMergeComponent] 'stg.dim_Employee', 'dbo.dim_Employee', 'SourceEmployeeID', EmployeeName, Role, EndDate, ManagerID, 2

Output for above SP executions:
1 - WHEN MATCHED AND EXISTS (SELECT source.imageURL EXCEPT SELECT target.imageURL) THEN UPDATE SET target.imageURL = source.imageURL
2 - WHEN MATCHED AND target.ValidTo IS NULL AND EXISTS (SELECT source.EmployeeName, source.Role, source.EndDate, source.ManagerID EXCEPT SELECT target.EmployeeName, target.Role, target.EndDate, target.ManagerID) THEN UPDATE SET ValidTo = GETDATE(), CurrentFlag = 0

 */

CREATE OR ALTER PROCEDURE [mst].[AddDimensionMergeComponent] @SourceTable     NVARCHAR(100), 
                                                             @TargetTable     NVARCHAR(100), 
                                                             @Condition       NVARCHAR(500), 
                                                             @ChangingColumns VARCHAR(250), 
                                                             @SCDType         TINYINT
AS
    BEGIN
        DECLARE @SourceList VARCHAR(500), @TargetList VARCHAR(500), @MergeVariable NVARCHAR(1000), @SCD1Var VARCHAR(500);
        SET @SourceList =
        (
            SELECT LEFT(STRING_AGG(concat('source.', TRIM(value), ','), ' '), LEN(STRING_AGG(concat('source.', TRIM(value), ','), ' ')) - 1)
            FROM STRING_SPLIT(@ChangingColumns, ',')
        );
        SET @TargetList = REPLACE(@SourceList, 'source.', 'target.');
        SET @Condition =
        (
            SELECT REPLACE(STRING_AGG(CONCAT(concat('source.source', TRIM(value), 'id'), ' = ', concat('target.source', TRIM(value), 'id')), ', '), ', ', ' AND ')
            FROM STRING_SPLIT(REPLACE(@TargetTable, 'dbo.dim_', ''), ',')
        );
        SET @Condition =
        (
            SELECT REPLACE(REPLACE(@Condition, 'sourceEmployeeStaffid', 'sourceStaffid'), 'sourceManagerStaffid', 'sourceStaffid')
        );
        IF @SCDType = 2
            BEGIN
                SET @MergeVariable = CONCAT('WHEN MATCHED AND target.ValidTo IS NULL AND EXISTS (SELECT ', @SourceList, ' EXCEPT SELECT ', @TargetList, ') THEN UPDATE SET ValidTo = GETDATE(), CurrentFlag = 0');
            END;
            ELSE
            BEGIN
                IF @SCDType = 1
                    BEGIN
                        SET @SourceList =
                        (
                            SELECT LEFT(STRING_AGG(concat('source.', TRIM(value), ','), ' '), LEN(STRING_AGG(concat('source.', TRIM(value), ','), ' ')) - 1)
                            FROM STRING_SPLIT(@ChangingColumns, ',')
                        );
                        SET @TargetList = REPLACE(@SourceList, 'source', 'target');
                        SET @SCD1Var =
                        (
                            SELECT STRING_AGG(CONCAT(concat('target.', TRIM(value), ''), ' = ', REPLACE(concat('target.', TRIM(value), ''), 'target', 'source')), ', ')
                            FROM STRING_SPLIT(@ChangingColumns, ',')
                        );
                        SET @MergeVariable = CONCAT('WHEN MATCHED AND EXISTS (SELECT ', @SourceList, ' EXCEPT SELECT ', @TargetList, ') THEN UPDATE SET ', @SCD1Var);
                    END;
                    ELSE
                    BEGIN
                        SET @MergeVariable = NULL;
                    END;
            END;
        INSERT INTO [mst].[DimensionMergeComponents]
        VALUES
        (@SourceTable, 
         @TargetTable, 
         @Condition, 
         @SCDType, 
         @ChangingColumns, 
         @MergeVariable
        );
    END;
GO

/*

mst.[sp_mergeFinal]

This SP is split in two:
  - The first part only executes if we're processing a dimension table, due to it being very different from the fact table (duh!), most notable differences being SCD Types and Identity columns bypassing.
  - The second part only executes for a fact table.


First Part [Dimension]:
	After we've established the SourceTable, TargetTable, ColumnList and that it's got an identity, we obtain the SCD components that we've stored in mst.[DimensionMergeComponents]. 
	We create a global temp table (it needs to be global in order to be accessible from both within and outside of the dynamic sql) to replicate all columns of the table in question. All columns apart from the last one in each table will be of VARCHAR(300) apart from the last one being a BIT column.
	This temp table acts as an intermediary container for the data, where the data will be inserted prior to it then being merged with the final fact table. 

	This is necessary in order to overcome the below error SQL constantly kept spitting me with:
	"The target table 'dbo.fact_WorkItemStateHistory' of the INSERT statement cannot be on either side of a (primary key, foreign key) relationship when the FROM clause contains a nested INSERT, UPDATE, DELETE, or MERGE statement. Found reference constraint 'FK__fact_Work__WorkI__4F7CD00D'."

	Once the temp table is created, we can proceed in computing the Dynamic SQL for the merge. The first component to this will be a basic merge for any new rows that don't exist in the target dimension yet. To that we append the SCD Types 1 and 2, if there are any. Once we've got this, we're ready to execute the merge and insert new rows while accounting for SCD Types where needed.




Second Part [Fact]:
	Once SourceTable, TargetTable and ColumnList have been established, we create the temp table required for the merge. 
	
	At this point, you'll notice I'm implicitly creating a local temp table called #forceOrder. You can replace that with your target fact table. This is a completely optional and custom thing, here's why:
		- For this particular project, I used DAX to compute a calculated table that expands every Start to End interval for every WorkItem entry, in order to enable reporting that provides the "Close of play" status of every WorkItem, on any given day. The ability to query the model in this way was very important, as it allowed us to filter for just one day and see the state of affairs for that day, for every WorkItem, together with some other calculated values such as IsOverdue, DaysOverSLA, etc. Imagine this view as being one where you look back in time and see what was current then.
		  However, because the new calculated table didn't have a row number identifier, one had to be computed in order to be used for the purposes of calculating exactly which row number from the now expanded WorkItem fact table was the current record for each day.
		  The problem with this is that my DAX logic now required that any new WorkItems be inserted in an ascending order, rather than randomly. Obviously, SQL couldn't care less about my order requirement, so I used a temp table and put a clustered index on it according to my desired order, in order to force the engine to read the clustered index and not the table heap, and thus inserting the rows in an ordered fashion.
		  This was a one-off requirement for this scenario and I don't recommend this practice unless absolutely required.

	Once we've past this point, we append all 3 components of the fact merge, that we stored in mst.FactMergeComponents, to the @MergeSQL variable and then execute it.

	Merge is now complete.

*/

CREATE OR ALTER PROCEDURE [mst].[sp_mergeFinal] @SourceTable VARCHAR(100), 
                                                @TargetTable VARCHAR(100), 
                                                @ColumnList  VARCHAR(300), 
                                                @HasIdentity INT, 
                                                @TargetType  VARCHAR(10)
AS
    BEGIN
        DECLARE @MergeSQL NVARCHAR(MAX)= '', @SCDVariable NVARCHAR(500), @Condition NVARCHAR(500)=
        (
            SELECT TOP 1 [Condition]
            FROM [mst].[DimensionMergeComponents]
            WHERE [SourceTable] = REPLACE(REPLACE(@SourceTable, '[', ''), ']', '')
                  AND [TargetTable] = REPLACE(REPLACE(@TargetTable, '[', ''), ']', '')
        ), @SCD1 NVARCHAR(500)=
        (
            SELECT [MergeVariable]
            FROM [mst].[DimensionMergeComponents]
            WHERE [SourceTable] = REPLACE(REPLACE(@SourceTable, '[', ''), ']', '')
                  AND [TargetTable] = REPLACE(REPLACE(@TargetTable, '[', ''), ']', '')
                  AND [SCDType] = 1
        ), @SCD2 NVARCHAR(500)=
        (
            SELECT [MergeVariable]
            FROM [mst].[DimensionMergeComponents]
            WHERE [SourceTable] = REPLACE(REPLACE(@SourceTable, '[', ''), ']', '')
                  AND [TargetTable] = REPLACE(REPLACE(@TargetTable, '[', ''), ']', '')
                  AND [SCDType] = 2
        ), @RawColumns VARCHAR(250)= REPLACE(@ColumnList, 'target.', ''), @SCD2Component NVARCHAR(MAX)= '', @TempTable NVARCHAR(400);
        IF @HasIdentity = 1
            BEGIN
                SET @TempTable = N'CREATE TABLE ##x1 (' + replace(TRIM(SUBSTRING(@RawColumns, CHARINDEX(' ', @RawColumns), LEN(@RawColumns) - CHARINDEX(' ', @RawColumns) + 2)), ',', ' VARCHAR(300),') + ' BIT)';
                EXECUTE (@TempTable);
                SET @MergeSQL = CONCAT(N'SET IDENTITY_INSERT ', @TargetTable, ' ON; MERGE ' + @TargetTable, ' AS target USING ', @SourceTable, ' AS source ON ', @Condition, ' WHEN NOT MATCHED BY TARGET AND ', LEFT(@Condition, CHARINDEX(' ', @Condition) - 1), ' IS NOT NULL THEN INSERT (', @RawColumns, ') VALUES (', @RawColumns, ') ', ISNULL(@SCD1 + ';', ';'), ' SET IDENTITY_INSERT ', @TargetTable, ' OFF;');
                IF @SCD2 IS NOT NULL
                    BEGIN
                        SET @SCD2Component = CONCAT(' INSERT INTO ##x1 SELECT ', TRIM(SUBSTRING(@RawColumns, CHARINDEX(' ', @RawColumns), LEN(@RawColumns) - CHARINDEX(' ', @RawColumns) + 2)) + ' FROM (MERGE ', @TargetTable + ' AS target USING ', @SourceTable + ' AS source ON ', @Condition + ' ', ISNULL(@SCD2, '') + ' OUTPUT $ACTION ActionOut, ', REPLACE(@ColumnList, 'target.', 'source.') + ') AS MergeOut WHERE MergeOut.ActionOut = ''UPDATE'' AND ', LEFT(@RawColumns, CHARINDEX(',', @RawColumns) - 1) + ' != -1; INSERT INTO ', @TargetTable + '(', TRIM(SUBSTRING(@RawColumns, CHARINDEX(' ', @RawColumns), LEN(@RawColumns) - CHARINDEX(' ', @RawColumns) + 2)) + ') SELECT ', REPLACE(TRIM(SUBSTRING(@RawColumns, CHARINDEX(' ', @RawColumns), LEN(@RawColumns) - CHARINDEX(' ', @RawColumns) + 2)), '[ValidFrom]', 'GETDATE()') + ' FROM ##x1;');
						SET @MergeSQL = CONCAT(@MergeSQL, @SCD2Component)
                    END;
            END;
            ELSE
            BEGIN
                IF @TargetType = 'fact'
                    BEGIN
                        DECLARE @Component1 VARCHAR(2000), @Component2 VARCHAR(2000), @Component3 VARCHAR(2000);
                        SET @RawColumns = TRIM(SUBSTRING(@RawColumns, CHARINDEX(' ', @RawColumns), LEN(@RawColumns) - CHARINDEX(' ', @RawColumns) + 2));
                        SET @ColumnList = TRIM(SUBSTRING(@ColumnList, CHARINDEX(' ', @ColumnList), LEN(@ColumnList) - CHARINDEX(' ', @ColumnList) + 2));
                    END;
                SET @TempTable = N'CREATE TABLE ##x1 (' + REPLACE(TRIM(@RawColumns), ',', ' VARCHAR(300),') + ' BIT)';
                EXECUTE (@TempTable);
                SET @Component1 =
                (
                    SELECT [MergeStatement]
                    FROM [mst].[FactMergeComponents]
                    WHERE [TargetTable] = @TargetTable
                          AND [ComponentNumber] = 1
                );
                SET @Component2 =
                (
                    SELECT [MergeStatement]
                    FROM [mst].[FactMergeComponents]
                    WHERE [TargetTable] = @TargetTable
                          AND [ComponentNumber] = 2
                );
                SET @Component3 =
                (
                    SELECT [MergeStatement]
                    FROM [mst].[FactMergeComponents]
                    WHERE [TargetTable] = @TargetTable
                          AND [ComponentNumber] = 3
                );
                SET @Component1 = CONCAT('SELECT * INTO #forceOrder FROM stg.fact_WorkItemStateHistory ORDER BY WorkItemSK, SnapshotStartDateSK, SnapshotStartTimeSK; CREATE CLUSTERED INDEX IX_CL_FO ON #forceOrder([WorkItemSK], [SnapshotStartDateSK], [SnapshotStartTimeSK]); ', @Component1);
                SET @Component3 = CONCAT(@Component3, ' DROP TABLE #forceOrder;');
                SET @MergeSQL = CONCAT(@Component1, @Component2, @Component3);
            END;
        EXECUTE (@MergeSQL);
        DROP TABLE [##x1];
    END;
GO

/*

[mst].[loadStagingToFinal]

This SP will initiate the merge process (this process spans over multiple SPs) for the table input type (i.e. fact or dim).

In this SP we'll get the number of tables to iterate through, depending of the table input, in order to obtain the source table syntax (i.e. stg.dim_Employee), the target table syntax (i.e. dbo.dim_Employee), the full column list for the table in within the current iteration, and determine whether the table has an identity column.

All these parameters will get passed through to mst.[sp_mergeFinal], which will compute the dynamic SQL for the merge and execute it.


*/

CREATE OR ALTER PROCEDURE [mst].[loadStagingToFinal] @TableInput VARCHAR(4)
AS
    BEGIN
        DECLARE @MergeSQL VARCHAR(MAX)= '', @TableColCount INT= 0, @ColumnName VARCHAR(75)= '', @CurrentTableCol INT= 0, @Columnlist VARCHAR(8000)= '', @TargetTable VARCHAR(50)= '', @TableINCR INT= 0, @NoOfTables INT, @SourceTable VARCHAR(500)= '', @StagingSchema INT=
        (
            SELECT schema_id
            FROM [sys].[schemas]
            WHERE [name] = 'stg'
        ), @DboSchema INT=
        (
            SELECT schema_id
            FROM [sys].[schemas]
            WHERE [name] = 'dbo'
        ), @HasIdentity INT= 0;
        IF @TableInput IS NULL
            BEGIN
                SET @NoOfTables =
                (
                    SELECT COUNT([tablename])
                    FROM [mst].[stagingmaintenance]
                    WHERE [tableName] LIKE '%dim%'
                          OR [tableName] LIKE '%fact%'
                );
            END;
            ELSE
            BEGIN
                SET @NoOfTables =
                (
                    SELECT COUNT([tablename])
                    FROM [mst].[stagingmaintenance]
                    WHERE [tableName] LIKE '%' + @TableInput + '%'
                );
            END;
        WHILE @TableINCR < @NoOfTables
            BEGIN
                IF @TableInput IS NULL
                    BEGIN
                        SET @SourceTable = concat('stg.[',
                        (
                            SELECT [tablename]
                            FROM [mst].[stagingmaintenance]
                            WHERE [tableName] LIKE '%dim%'
                                  OR [tableName] LIKE '%fact%'
                            ORDER BY [tableid]
                            OFFSET @TableINCR ROWS FETCH NEXT 1 ROW ONLY
                        ), ']');
                    END;
                    ELSE
                    BEGIN
                        SET @SourceTable = concat('stg.[',
                        (
                            SELECT [tablename]
                            FROM [mst].[stagingmaintenance]
                            WHERE [tableName] LIKE '%' + @TableInput + '%'
                            ORDER BY [tableid]
                            OFFSET @TableINCR ROWS FETCH NEXT 1 ROW ONLY
                        ), ']');
                    END;
                SET @TargetTable = replace(@SourceTable, 'stg', 'dbo');
                SET @TableColCount =
                (
                    SELECT COUNT([c].[name])
                    FROM [sys].[tables] AS [t]
                         INNER JOIN [sys].[columns] AS [c] ON [t].object_id = [c].object_id
                    WHERE [t].[name] = LEFT(replace(@SourceTable, 'stg.[', ''), LEN(replace(@SourceTable, 'stg.[', '')) - 1)
                          AND [t].schema_id = @DboSchema
                );
                SET @Columnlist = '';
                SET @CurrentTableCol = 0;
                WHILE @CurrentTableCol < @TableColCount
                    BEGIN
                        SET @ColumnName =
                        (
                            SELECT [c].[name]
                            FROM [sys].[tables] AS [t]
                                 INNER JOIN [sys].[columns] AS [c] ON [t].object_id = [c].object_id
                            WHERE [t].[name] = LEFT(replace(@SourceTable, 'stg.[', ''), LEN(replace(@SourceTable, 'stg.[', '')) - 1)
                                  AND [t].schema_id = @DboSchema
                            ORDER BY [column_id]
                            OFFSET @CurrentTableCol ROWS FETCH NEXT 1 ROW ONLY
                        );
                        SET @Columnlist+=CONCAT('target.[', @ColumnName, ']', ', ');
                        SET @CurrentTableCol+=1;
                    END;
                SET @Columnlist = LEFT(@Columnlist, LEN(@Columnlist) - 1);
                IF @TableInput = 'dim'
                    BEGIN
                        SET @HasIdentity = 1;
                    END;
                    ELSE
                    BEGIN
                        SET @HasIdentity = 0;
                    END;
                EXECUTE [mst].[sp_mergeFinal] 
                        @SourceTable, 
                        @TargetTable, 
                        @ColumnList, 
                        @HasIdentity, 
                        @TableInput;
                SET @TableINCR+=1;
            END;
    END;
    GO
CREATE OR ALTER PROCEDURE [mst].[RawToStaging_WorkItem]
AS
    BEGIN
        DECLARE @Id NVARCHAR(10)= ISNULL(IDENT_CURRENT('dbo.dim_WorkItem'), 1), @Sql NVARCHAR(MAX);
        SET @SQL = N'SELECT IDENTITY(INT, ' + @Id + ', 1) AS [WorkItemSK],
   [WorkItemID] AS              [SourceWorkItemID],
   [StartDate],
   [TargetDate],
   [CompletedDate],
   [SLA],
   [p].[Description] AS                       [Priority]
   into #dim_stgWorkItem
FROM   [raw].[WorkItem] AS [WI]
   LEFT JOIN [raw].[Priority] AS [p] ON [wi].[priorityid] = [p].[priorityid]
   insert into stg.dim_WorkItem (WorkItemSK, SourceWorkItemID, StartDate, TargetDate, CompletedDate, SLA, Priority) select workitemsk, sourceworkitemID, [StartDate], [TargetDate], [CompletedDate], [SLA], priority from #dim_stgWorkItem';
        EXECUTE (@SQL);
    END;
GO
CREATE OR ALTER PROCEDURE [mst].[RawToStaging_Staff]
AS
    BEGIN
        DECLARE @Id NVARCHAR(10), @Sql NVARCHAR(MAX);
        SET @Id = ISNULL(IDENT_CURRENT('dbo.dim_ManagerStaff'), 1);
        SET @SQL = N'
SELECT IDENTITY(INT, ' + @Id + ', 1) AS [StaffSK],
   [Staff_ID] AS                    [SourceStaffID],
   concat([ms].[forename], '' '', [ms].[surname]) AS [StaffName],
   [mst].[role] AS                               [Role],
   [ms].[imageurl] AS                            [ImageURL],
   [start_Date] AS                               [StartDate],
   [end_date] AS                                 [EndDate],
   case when ISNULL(end_date, ''2050-12-31'') > getdate() then 1 else 0 end as [CurrentFlag]
   into #dim_stgMNGStaff
FROM [raw].[ManagerStaff] AS [ms] 
   LEFT JOIN [raw].[StaffRole] AS [mst] ON [ms].[role] = [mst].[role_id]
   INSERT INTO stg.dim_ManagerStaff (StaffSK, SourceStaffID, StaffName, Role, ImageURL, StartDate, EndDate, CurrentFlag) SELECT StaffSK, [SourceStaffID], [StaffName], [Role], [ImageURL], [StartDate], [EndDate], [CurrentFlag] FROM #dim_stgMNGStaff WHERE patindex(''%[a-z]%'', [StaffName]) > 0';
        EXEC (@Sql);
        SET @ID = ISNULL(IDENT_CURRENT('dbo.dim_EmployeeStaff'), 1);
        SET @Sql = N'
SELECT IDENTITY(INT, ' + @Id + ', 1) AS [StaffSK],
   [Staff_ID] AS                    [SourceStaffID],
   concat([ms].[forename], '' '', [ms].[surname]) AS [StaffName],
   [mst].[role] AS                               [Role],
   [Staff_ID] AS                            [ManagerID],
   [ms].[imageurl] AS                            [ImageURL],
   [start_Date] AS                               [StartDate],
   [end_date] AS                                 [EndDate],
   case when ISNULL(end_date, ''2050-12-31'') > getdate() then 1 else 0 end as [CurrentFlag]
   into #dim_stgEmpStaff
FROM [raw].[EmployeeStaff] AS [ms] 
 LEFT JOIN [raw].[StaffRole] AS [mst] ON [ms].[role] = [mst].[role_id]
 INNER JOIN(SELECT DISTINCT
[assignedto]
  FROM   [raw].[workitem]) AS [wi] ON [wi].[assignedto] = [ms].[staff_id];
   INSERT INTO stg.dim_EmployeeStaff (StaffSK, SourceStaffID, StaffName, Role, ImageURL, StartDate, EndDate, CurrentFlag, ManagerID) SELECT StaffSK, [SourceStaffID], [StaffName], [Role], [ImageURL], [StartDate], [EndDate], [CurrentFlag], [ManagerID] FROM #dim_stgEmpStaff WHERE patindex(''%[a-z]%'', [StaffName]) > 0';
        EXEC (@Sql);
    END;
GO
CREATE OR ALTER PROCEDURE [mst].[RawToStaging_Location]
AS
    BEGIN
        DECLARE @Id NVARCHAR(10)= ISNULL(IDENT_CURRENT('dbo.dim_Location'), 1), @Sql NVARCHAR(MAX);
        SET @SQL = N'SELECT IDENTITY(INT, ' + @Id + ', 1) AS [LocationSK],
   [LocationSK] AS              [SourceLocationID],
   OfficeName,
   City
   into #dim_stgLocation
FROM   [raw].[Location]
   insert into stg.dim_Location (LocationSK, SourceLocationID, OfficeName, City) select LocationSK, SourceLocationID, [OfficeName], [City] from #dim_stgLocation';
        EXECUTE (@SQL);
    END;
GO
CREATE OR ALTER PROCEDURE [mst].[RawToStaging_Status]
AS
    BEGIN
        DECLARE @Id NVARCHAR(10)= ISNULL(IDENT_CURRENT('dbo.dim_Status'), 1), @Sql NVARCHAR(MAX);
        SET @SQL = N'SELECT IDENTITY(INT, ' + @Id + ', 1) AS [StatusSK],
   [StatusID] AS [SourceStatusID],
   [Description] AS            [StatusName],
   [Description] AS            [StatusDescription]
   INTO #dim_stgStatus
FROM   [raw].[CurrentStatus] AS [el]
   insert into stg.dim_Status (StatusSK, SourceStatusID, StatusName, StatusDescription) select StatusSK, SourceStatusID, StatusName, StatusDescription from #dim_stgStatus';
        EXECUTE (@SQL);
    END;
GO

/*

mst.[MergingFlow]

This table will store a 10,000 feet view of the process flow to be executed for each system, when re-processing is required.

This requires manual inserts as it requires careful mapping of every step of the flow; however, making this a fully autonomous insert is not outside the realm of possibility.
The table will be used to iterate and execute through every step and sub-step (step component) for the system within the current iteration of the process.

*/

CREATE TABLE [mst].[MergingFlow]
([System]                VARCHAR(100), 
 [Step]                  TINYINT, 
 [Step Description]      VARCHAR(100), 
 [Step Component]        TINYINT, 
 [Component Description] VARCHAR(100), 
 [Table Type]            VARCHAR(20), 
 [Action]                NVARCHAR(100)
);
GO

/*

mst.[MergeFlowMaster]

This table will store a summary of every DW system and a reference of the SP required to call in order to process the System. By default, every system is locked, marking the System as not ready for (re-)processing.
A Locked value of 0 means that the system is not locked, and thus will be picked up by mst.[CheckSystemsForProcess], which will trigger the (re-)processing of every unlocked system, and then locking it back again once processing is finished.

*/

CREATE TABLE [mst].[MergeFlowMaster]
([System]         VARCHAR(100), 
 [Locked]         BIT DEFAULT 1, 
 ACTION AS 'EXECUTE mst.MergeFlow ''' + [System] + ''';', 
 [Last Processed] DATETIME2(0) DEFAULT NULL
);
GO

/*
[mst].[SystemRawTablesMapping]

This table will map every raw table to every DW system, so that mst.[CheckSystemsForProcess] will iterate through every raw table, check whether all raw tables are populated, thus establishing which systems require (re-)processing by performing an update on mst.[MergeFlowMaster] and marking them as unlocked.

Just like mst.[MergingFlow], a manual insertion is performed here, however making this autonomous wouldn't be a problem.

*/

CREATE TABLE [mst].[SystemRawTablesMapping]
([System]    VARCHAR(100), 
 [Raw Table] VARCHAR(100)
);
GO

/*

mst.[MergeFlow]

This SP iterates through every step and sub-step (step component) for the system it's been called to process, executing the referenced SPs found in table mst.[MergingFlow]. This process will ensure it executes a copy from raw to staging, perform any transformation required, and then merge staging with final tables (fact/dim).

*/

CREATE OR ALTER PROCEDURE [mst].[MergeFlow] @System VARCHAR(100)
AS
    BEGIN
        BEGIN TRY
            BEGIN TRANSACTION;
            SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
            SET NOCOUNT ON;
            DECLARE @Step TINYINT= 1, @StepComponent TINYINT= 1, @MaxStep TINYINT, @MaxStepComponent TINYINT, @Action NVARCHAR(MAX), @ComponentDescription VARCHAR(125);
            DECLARE @ErrorNumber INT, @ErrorSeverity TINYINT, @ErrorMessage VARCHAR(200);
            SET @MaxStep =
            (
                SELECT MAX([Step])
                FROM [mst].[MergingFlow]
                WHERE [System] = @System
            );
            WHILE @Step <= @MaxStep
                BEGIN
                    SET @MaxStepComponent =
                    (
                        SELECT MAX([Step Component])
                        FROM [mst].[MergingFlow]
                        WHERE [System] = @System
                              AND [Step] = @Step
                    );
                    WHILE @StepComponent <= @MaxStepComponent
                        BEGIN
                            SET @Action =
                            (
                                SELECT [Action]
                                FROM [mst].[MergingFlow]
                                WHERE [System] = @System
                                      AND [Step] = @Step
                                      AND [Step Component] = @StepComponent
                            );
                            SET @ComponentDescription =
                            (
                                SELECT [Component Description]
                                FROM [mst].[MergingFlow]
                                WHERE [System] = @System
                                      AND [Step] = @Step
                                      AND [Step Component] = @StepComponent
                            );
                            PRINT CONCAT(N'Executing step ', @Step, N' part ', @StepComponent, N': ', @ComponentDescription, N'.');
                            EXECUTE (@Action);
                            SET @StepComponent+=1;
                        END;
                    SET @StepComponent = 1;
                    SET @Step+=1;
                END;
            COMMIT TRANSACTION;
        END TRY
        BEGIN CATCH
            -- Probably some logging would be made here in the future
            SELECT @ErrorNumber = ERROR_NUMBER(), 
                   @ErrorSeverity = ERROR_SEVERITY(), 
                   @ErrorMessage = ERROR_MESSAGE();
            PRINT CONCAT('There has been an error. Error number: ', @ErrorNumber, ' Severity level: ', @ErrorSeverity, ' Message: ', @ErrorMessage, '.');
            ROLLBACK TRANSACTION;
        END CATCH;
    END;
    GO

/*
	mst.[ProcessUnlockedSystems]

	This SP will query the mst.[MergeFlowMaster] table and will iterate through every unlocked DW system, calling the appropriate SP to trigger the processing for each DW.

	*/

CREATE OR ALTER PROCEDURE [mst].[ProcessUnlockedSystems]
AS
    BEGIN
        SET NOCOUNT ON;
        DECLARE @UnlockedSystems TINYINT=
        (
            SELECT COUNT([System])
            FROM [mst].[MergeFlowMaster]
            WHERE [Locked] = 0
        ), @Increment TINYINT= 0, @CurrentSystem VARCHAR(100), @Action NVARCHAR(200);
        WHILE @Increment < @UnlockedSystems
            BEGIN
                SET @CurrentSystem =
                (
                    SELECT [System]
                    FROM [MergeFlowMaster]
                    WHERE [Locked] = 0
                    ORDER BY [System]
                    OFFSET @Increment ROWS FETCH NEXT 1 ROW ONLY
                );
                SET @Action =
                (
                    SELECT [Action]
                    FROM [MergeFlowMaster]
                    WHERE [System] = @CurrentSystem
                );
                PRINT CONCAT('Processing ', @CurrentSystem, '...');
                EXECUTE (@Action);
                PRINT CONCAT('Processing ', @CurrentSystem, ' complete.');
                UPDATE [mst].[MergeFlowMaster]
                  SET 
                      [Last Processed] = GETDATE(), 
                      [Locked] = 1
                WHERE [System] = @CurrentSystem;
                SET @Increment+=1;
            END;
    END;
    GO

/*

mst.[CheckSystemsForProcess]

This SP will iterate through every raw table for every DW system, mapped in mst.[SystemRawTablesMapping], and check whether all of the raw tables for the currently iterating DW system are populated. If they are, this SP will update the corresponding DW System in mst.[MergeFlowMaster] and mark it as unlocked, and then calls SP mst.[ProcessUnlockedSystems] to process every unlocked DW system.

If even one of the raw tables has not been populated but others have (4/5 populated), this SP will simply move on to the next System and will start check for its raw tables.

*/

CREATE OR ALTER PROCEDURE [mst].[CheckSystemsForProcess]
AS
    BEGIN
        DECLARE @SystemCount TINYINT= 0, @RawTableCount TINYINT= 0, @MaxSystem TINYINT, @MaxRawTables TINYINT, @System VARCHAR(100), @RawTable NVARCHAR(100), @CheckIfLoaded NVARCHAR(750);
        CREATE TABLE [##FilledTables]([A] BIT);
        SET @MaxSystem =
        (
            SELECT COUNT(DISTINCT [System])
            FROM [mst].[SystemRawTablesMapping]
        );
        WHILE @SystemCount <= @MaxSystem
            BEGIN
                SET @System =
                (
                    SELECT DISTINCT 
                           [System]
                    FROM [mst].[SystemRawTablesMapping]
                    ORDER BY [System]
                    OFFSET @SystemCount ROWS FETCH NEXT 1 ROW ONLY
                );
                SET @MaxRawTables =
                (
                    SELECT COUNT([Raw Table])
                    FROM [mst].[SystemRawTablesMapping]
                    WHERE [System] = @System
                );
                WHILE @RawTableCount < @MaxRawTables
                    BEGIN
                        SET @RawTable =
                        (
                            SELECT [Raw Table]
                            FROM [mst].[SystemRawTablesMapping]
                            WHERE [System] = @System
                            ORDER BY [Raw Table]
                            OFFSET @RawTableCount ROWS FETCH NEXT 1 ROW ONLY
                        );
                        SET @CheckIfLoaded = CONCAT(N'DECLARE @RawTable VARCHAR(100); SET @RawTable = (SELECT [Raw Table]
FROM   mst.[SystemRawTablesMapping]
WHERE  [System] = ''', @System, '''
ORDER BY [Raw Table]
OFFSET ', @RawTableCount, ' ROWS FETCH NEXT 1 ROW ONLY) IF EXISTS (SELECT TOP 1 * FROM ', @RawTable, ') INSERT INTO ##FilledTables VALUES(1)');
                        EXECUTE (@CheckIfLoaded);
                        SET @RawTableCount+=1;
                    END;
                IF
                (
                    SELECT COUNT([A])
                    FROM [##FilledTables]
                ) = @MaxRawTables
                    BEGIN
                        UPDATE [mst].[MergeFlowMaster]
                          SET 
                              [LOCKED] = 0
                        WHERE [System] = @System;
                    END;
                DELETE FROM [##FilledTables];
                SET @RawTableCount = 0;
                SET @SystemCount+=1;
            END;
        DROP TABLE [##FilledTables];
        EXECUTE [mst].[ProcessUnlockedSystems];
    END;
GO

/*

[mst].[TruncateRawStaging]

This SP assumes that you have NO DW Systems processing concurrently, and thus will truncate EVERY staging/raw table, regardless of system.

Of course, this could be improved by making use of what's already in mst.[StagingMaintenance] to make the insert to mst.[SystemRawTablesMapping] autonomous.

*/

CREATE OR ALTER PROCEDURE [mst].[TruncateRawStaging]
AS
    BEGIN
        DECLARE @TableNames TABLE([Name] VARCHAR(100));
        INSERT INTO @TableNames
               SELECT 'stg.' + [tablename] AS [TableName]
               FROM [mst].[stagingmaintenance]
               WHERE [tablename] LIKE '%dim%'
                     OR [tablename] LIKE '%fact%'
               UNION
               SELECT 'raw.' + [tablename] AS [TableName]
               FROM [mst].[stagingmaintenance]
               WHERE [tablename] NOT LIKE '%dim%'
                     AND [tablename] NOT LIKE '%fact%';
        DECLARE @Count TINYINT=
        (
            SELECT COUNT([Name])
            FROM @TableNames
        ), @Current TINYINT= 0, @Truncate NVARCHAR(150), @Table VARCHAR(100);
        WHILE @Current < @Count
            BEGIN
                SET @Table =
                (
                    SELECT [Name]
                    FROM @TableNames
                    ORDER BY [Name]
                    OFFSET @Current ROWS FETCH NEXT 1 ROW ONLY
                );
                SET @Truncate = CONCAT(N'TRUNCATE TABLE ', @Table, ';');
                EXECUTE (@Truncate);
                SET @Current+=1;
            END;
    END;
	GO

-- Transforms data from raw tables to the staging fact talbe
CREATE OR ALTER PROCEDURE [mst].[PopulateStagingfactWorkItemStateHistory]
AS
    BEGIN 
        CREATE TABLE [#x1]
        ([StaffSK]             INT, 
         [LocationSK]          INT, 
         [StatusSK]            INT, 
         [WorkItemSK]          INT, 
         [SnapshotStartDateSK] INT, 
         [SnapshotStartTimeSK] INT, 
         [SnapshotEndDateSK]   INT, 
         [SnapshotEndTimeSK]   INT, 
         [SnapshotCurrent]     BIT DEFAULT 1
        );
        WITH CTE
             AS (SELECT [wi].[assignedto] AS [StaffSK], 
                        LocationID [LocationSK], 
                        [StatusID] AS [StatusFK], 
                        [wish].[WorkItemID] AS [WorkItemFK], 
                        CONVERT(DATE, [wish].[startdate]) AS [SnapshotStartDate], 
                        CONVERT(TIME(0), [wish].[startdate]) AS [SnapshotStartTime], 
                        CONVERT(DATE, [wish].[enddate]) AS [SnapshotEndDate], 
                        CONVERT(TIME(0), [wish].[enddate]) AS [SnapshotEndTime]
                 FROM [raw].[WorkItemStateHistory] AS [wish]
                      LEFT JOIN [raw].[workitem] AS [wi] ON [wish].[workitemid] = [wi].[workitemid]
                      LEFT JOIN [raw].[employeestaff] AS [s] ON [wi].[assignedto] = [s].[staff_id]
                 WHERE [wish].[startdate] IS NOT NULL)
             INSERT INTO [#x1]
             ([StaffSK], 
              [LocationSK], 
              [StatusSK], 
              [WorkItemSK], 
              [SnapshotStartDateSK], 
              [SnapshotStartTimeSK], 
              [SnapshotEndDateSK], 
              [SnapshotEndTimeSK], 
              [SnapshotCurrent]
             )
                    SELECT [StaffSK], 
                           [LocationSK], 
                           [StatusSK], 
                           [WorkItemSK], 
                           [SnapshotStartDateSK], 
                           [SnapshotStartTimeSK], 
                           [SnapshotEndDateSK], 
                           [SnapshotEndTimeSK], 
                           0 AS [SnapshotCurrent]
                    FROM
                    (
                        SELECT [s].[staffsk], 
                               [l].[locationsk], 
                               [sts].[statussk], 
                               [wi].[workitemsk], 
                               ISNULL([SSD].[datekeysk], -1) AS [SnapshotStartDateSK], 
                               ISNULL([SST].[timekeysk], -1) AS [SnapshotStartTimeSK], 
                               ISNULL([SED].[datekeysk], -1) AS [SnapshotEndDateSK], 
                               ISNULL([SET].[timekeysk], -1) AS [SnapshotEndTimeSK]
                        FROM [CTE]
                             INNER JOIN [dbo].[dim_Location] AS [l] ON [cte].[locationsk] = [l].[sourcelocationID]
                                                                       AND [l].[currentflag] = 1
                             INNER JOIN [dbo].[dim_employeestaff] AS [s] ON [cte].[staffsk] = [s].[sourcestaffid]
                                                                            AND [s].[currentflag] = 1
                             INNER JOIN [dbo].[dim_status] AS [sts] ON [cte].[statusfk] = [sts].[sourcestatusid]
                                                                       AND [sts].[currentflag] = 1
                             INNER JOIN [dbo].[dim_workItem] AS [wi] ON [cte].[workitemfk] = [wi].[sourceworkitemid]
                                                                        AND [wi].[currentflag] = 1
                             LEFT JOIN [dbo].[dim_date] AS [SSD] ON [SSD].[datekeysk] = replace([snapshotstartdate], '-', '')
                             LEFT JOIN [dbo].[dim_date] AS [SED] ON [SED].[datekeysk] = replace([snapshotenddate], '-', '')
                             LEFT JOIN [dbo].[dim_Time] AS [SST] ON [SST].[timekeysk] = replace([snapshotstartTime], ':', '')
                             LEFT JOIN [dbo].[dim_Time] AS [SET] ON [SET].[timekeysk] = replace([snapshotendTime], ':', '')
                    ) AS [x1];
        WITH CTE
             AS (SELECT ROW_NUMBER() OVER(PARTITION BY [staffsk], 
                                                       [locationsk], 
                                                       [statussk], 
                                                       [workitemsk], 
                                                       [snapshotstartdatesk], 
                                                       [snapshotstarttimesk], 
                                                       [snapshotenddatesk], 
                                                       [snapshotendtimesk], 
                                                       [snapshotcurrent]
                        ORDER BY [workitemsk]) AS [RowNumber], 
                        [staffsk], 
                        [locationsk], 
                        [statussk], 
                        [workitemsk], 
                        [snapshotstartdatesk], 
                        [snapshotstarttimesk], 
                        [snapshotenddatesk], 
                        [snapshotendtimesk], 
                        [snapshotcurrent]
                 FROM [#x1])
             DELETE FROM [cte]
             WHERE [RowNumber] > 1;
        UPDATE [#x1]
          SET 
              [snapshotcurrent] = 1
        FROM [#x1]
             INNER JOIN
        (
            SELECT [StaffSK], 
                   [LocationSK], 
                   [WorkItemSK], 
                   MAX(CONCAT([SnapshotStartDateSK], [SnapshotStartTimeSK])) AS [SnapshotStartDateTimeSK], 
                   REPLACE(MAX(CONCAT(IIF([SnapshotEndDateSK] = -1, 99999999, [SnapshotEndDateSK]), IIF([SnapshotEndTimeSK] = -1, 99999999, [SnapshotEndTimeSK]))), 99999999, -1) AS [SnapshotEndDateTimeSK]
            FROM [#x1]
            GROUP BY [StaffSK], 
                     [LocationSK], 
                     [WorkItemSK]
        ) [x1] ON [#x1].[StaffSK] = [x1].[staffsk]
                  AND [#x1].[LocationSK] = [x1].[LocationSK]
                  AND [#x1].[WorkItemSK] = [x1].[WorkItemSK]
                  AND CONCAT([#x1].[SnapshotStartDateSK], [#x1].[SnapshotStartTimeSK]) = [x1].[SnapshotStartDateTimeSK]
                  AND CONCAT([#x1].[SnapshotEndDateSK], [#x1].[SnapshotEndTimeSK]) = [x1].[SnapshotEndDateTimeSK];
        INSERT INTO [stg].[fact_WorkItemStateHistory]
        ([StaffSK], 
         [LocationSK], 
         [StatusSK], 
         [WorkItemSK], 
         [SnapshotStartDateSK], 
         [SnapshotStartTimeSK], 
         [SnapshotEndDateSK], 
         [SnapshotEndTimeSK], 
         [SnapshotCurrent]
        )
               SELECT [StaffSK], 
                      [LocationSK], 
                      [StatusSK], 
                      [WorkItemSK], 
                      [SnapshotStartDateSK], 
                      [SnapshotStartTimeSK], 
                      [SnapshotEndDateSK], 
                      [SnapshotEndTimeSK], 
                      [SnapshotCurrent]
               FROM [#x1];
        DROP TABLE [#x1];
    END;
