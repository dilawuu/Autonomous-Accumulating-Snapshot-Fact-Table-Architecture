SET NOCOUNT ON;

DECLARE
 @CurrentDate DATE = '1970-01-01';
DECLARE
 @EndDate DATE = '2050-12-31';

WHILE @CurrentDate < @EndDate
    BEGIN
  INSERT INTO [dbo].[Dim_Date]([DateKeySK],
 [DATE],
 day,
 [DaySuffix],
 [Weekday],
 [WeekDayName],
 [WeekDayName_Short],
 [WeekDayName_FirstLetter],
 [DOWInMonth],
 [DayOfYear],
 [WeekOfMonth],
 [WeekOfYear],
 [month],
 monthname,
 [MonthName_Short],
 [MonthName_FirstLetter],
 quarter,
 [QuarterName],
 [year],
 [MMYYYY],
 [MonthYear],
 [IsWeekend],
 [IsHoliday],
 [FirstDateofYear],
 [LastDateofYear],
 [FirstDateofQuater],
 [LastDateofQuater],
 [FirstDateofMonth],
 [LastDateofMonth],
 [FirstDateofWeek],
 [LastDateofWeek])
  SELECT [DateKeySK] = REPLACE(@CurrentDate, '-', ''),
DATE = @CurrentDate,
Day = DAY(@CurrentDate),
[DaySuffix] = CASE
  WHEN DAY(@CurrentDate) = 1
  OR DAY(@CurrentDate) = 21
  OR DAY(@CurrentDate) = 31
  THEN 'st'
  WHEN DAY(@CurrentDate) = 2
  OR DAY(@CurrentDate) = 22
  THEN 'nd'
  WHEN DAY(@CurrentDate) = 3
  OR DAY(@CurrentDate) = 23
  THEN 'rd'
  ELSE 'th'
   END,
[WEEKDAY] = DATEPART([dw], @CurrentDate),
[WeekDayName] = DATENAME([dw], @CurrentDate),
[WeekDayName_Short] = UPPER(LEFT(DATENAME([dw], @CurrentDate), 3)),
[WeekDayName_FirstLetter] = LEFT(DATENAME([dw], @CurrentDate), 1),
[DOWInMonth] = DAY(@CurrentDate),
[DayOfYear] = DATENAME([dy], @CurrentDate),
[WeekOfMonth] = DATEPART(WEEK, @CurrentDate) - DATEPART(WEEK, DATEADD([MM], DATEDIFF([MM], 0, @CurrentDate), 0)) + 1,
[WeekOfYear] = DATEPART([wk], @CurrentDate),
[Month] = MONTH(@CurrentDate),
MonthName = DATENAME([mm], @CurrentDate),
[MonthName_Short] = UPPER(LEFT(DATENAME([mm], @CurrentDate), 3)),
[MonthName_FirstLetter] = LEFT(DATENAME([mm], @CurrentDate), 1),
Quarter = DATEPART([q], @CurrentDate),
[QuarterName] = CASE
WHEN DATENAME([qq], @CurrentDate) = 1
THEN 'First'
WHEN DATENAME([qq], @CurrentDate) = 2
THEN 'second'
WHEN DATENAME([qq], @CurrentDate) = 3
THEN 'third'
WHEN DATENAME([qq], @CurrentDate) = 4
THEN 'fourth'
END,
[Year] = YEAR(@CurrentDate),
[MMYYYY] = RIGHT('0' + CAST(MONTH(@CurrentDate) AS VARCHAR(2)), 2) + CAST(YEAR(@CurrentDate) AS VARCHAR(4)),
[MonthYear] = CAST(CONCAT(UPPER(LEFT(DATENAME([mm], @CurrentDate), 3)), YEAR(@CurrentDate)) AS VARCHAR(4)),
[IsWeekend] = CASE
  WHEN DATENAME([dw], @CurrentDate) = 'Sunday'
  OR DATENAME([dw], @CurrentDate) = 'Saturday'
  THEN 1
  ELSE 0
   END,
[IsHoliday] = 0,
[FirstDateofYear] = CAST(CAST(YEAR(@CurrentDate) AS VARCHAR(4)) + '-01-01' AS DATE),
[LastDateofYear] = CAST(CAST(YEAR(@CurrentDate) AS VARCHAR(4)) + '-12-31' AS DATE),
[FirstDateofQuater] = DATEADD([qq], DATEDIFF([qq], 0, GETDATE()), 0),
[LastDateofQuater] = DATEADD([dd], -1, DATEADD([qq], DATEDIFF([qq], 0, GETDATE()) + 1, 0)),
[FirstDateofMonth] = CAST(CAST(YEAR(@CurrentDate) AS VARCHAR(4)) + '-' + CAST(MONTH(@CurrentDate) AS VARCHAR(2)) + '-01' AS DATE),
[LastDateofMonth] = EOMONTH(@CurrentDate),
[FirstDateofWeek] = DATEADD([dd], -(DATEPART([dw], @CurrentDate) - 1), @CurrentDate),
[LastDateofWeek] = DATEADD([dd], 7 - DATEPART([dw], @CurrentDate), @CurrentDate);

  SET @CurrentDate = DATEADD([DD], 1, @CurrentDate);
    END;

--Update Holiday information
UPDATE     [Dim_Date]
 SET [IsHoliday] = 1,
[HolidayName] = 'Christmas'
WHERE      [month] = 12
AND day = 25;

UPDATE     [Dim_Date]
 SET [SpecialDays] = 'Valentines Day'
WHERE      [month] = 2
AND day = 14;

--Update current date information
UPDATE     [Dim_Date]
 SET [CurrentYear] = DATEDIFF([yy], GETDATE(), DATE),
[CurrentQuater] = DATEDIFF([q], GETDATE(), DATE),
[CurrentMonth] = DATEDIFF([M], GETDATE(), DATE),
[CurrentWeek] = DATEDIFF([ww], GETDATE(), DATE),
[CurrentDay] = DATEDIFF([dd], GETDATE(), DATE);

UPDATE     [Dim_Date]
 SET [FinancialMonth] = CASE [Month]
   WHEN 6
   THEN 1
   WHEN 7
   THEN 2
   WHEN 8
   THEN 3
   WHEN 9
   THEN 4
   WHEN 10
   THEN 5
   WHEN 11
   THEN 6
   WHEN 12
   THEN 7
   WHEN 1
   THEN 8
   WHEN 2
   THEN 9
   WHEN 3
   THEN 10
   WHEN 4
   THEN 11
   WHEN 5
   THEN 12
END,
[FinancialQuarter] = CASE [Month]
WHEN 6
THEN 1
WHEN 7
THEN 1
WHEN 8
THEN 1
WHEN 9
THEN 2
WHEN 10
THEN 2
WHEN 11
THEN 2
WHEN 12
THEN 3
WHEN 1
THEN 3
WHEN 2
THEN 3
WHEN 3
THEN 4
WHEN 4
THEN 4
WHEN 5
THEN 4
 END,
[FinancialYear] = IIF([Month] >= 6
  AND Day >= 1, [Year] + 1, [Year]);

INSERT INTO           [dim_Date]([DateKeySK],
  [Date])
VALUES                (
 -1, '2050-12-31');
GO

DECLARE
 @StartTime TIME= CONVERT(TIME, '00:00:00');
DECLARE
 @EndTime TIME= CONVERT(TIME, '23:59:59');

WITH [timestamps]
AS (SELECT REPLACE(CAST([ts].[Time] AS TIME(0)), ':', '') AS [TimeKeySK],
CAST([ts].[Time] AS TIME(0)) AS                   [Time],
DATEPART(HOUR, [ts].[Time]) AS                    [Hour24],
DATEPART(MINUTE, [ts].[Time]) AS                  [MinuteOfHour],
DATEPART([SECOND], [ts].[Time]) AS                [SecondOfMinute]
   FROM   (SELECT DATEADD([SECOND], [x].[rn] - 1, @StartTime) AS [Time]
 FROM      (SELECT TOP (DATEDIFF([SECOND], @StartTime, @EndTime)) ROW_NUMBER() OVER(
 ORDER BY [s1].object_id) AS [rn]
  FROM                                                  [sys].[all_objects] AS [s1]
 CROSS JOIN [sys].[all_objects] AS [s2]
  ORDER BY [s1].object_id) AS [x]
 UNION
 SELECT @EndTime) AS [ts])
INSERT INTO [dbo].[Dim_Time]([TimeKeySK],
   [Time],
   [Hour12],
   [Hour24],
   [MinuteOfHour],
   [SecondOfMinute],
   [ElapsedMinutes],
   [ElapsedSeconds],
   [AMPM],
   [HHMMSS])
SELECT [TimeKeySK],
 [ts].[Time],
 CASE
WHEN [ts].[Hour24] > 12
AND [ts].[Hour24] % 12 <> 0
THEN [ts].[Hour24] % 12
WHEN [ts].[Hour24] % 12 = 0
THEN 12
ELSE [ts].[Hour24]
 END AS                                                                           [Hour12],
 [ts].[Hour24],
 [ts].[MinuteOfHour],
 [ts].[SecondOfMinute],
 ([ts].[Hour24] - 1) * 60 + [ts].[MinuteOfHour] AS                                [ElapsedMinutes],
 (([ts].[Hour24] - 1) * 60 + [ts].[MinuteOfHour]) * 60 + [ts].[SecondOfMinute] AS [ElapsedSeconds],
 CASE
WHEN [ts].[Hour24] > 12
THEN 'PM'
ELSE 'AM'
 END AS                                                                           [AMPM],
 CONVERT(CHAR(8), [ts].[Time], 108) AS                                            [HHMMSS]
FROM   [timestamps] AS [ts]
ORDER BY [ts].[Time];

INSERT INTO [dbo].[dim_time]([TimeKeySK])
VALUES(
 -1);
GO
EXECUTE [mst].[AddDimensionMergeComponent] 'stg.dim_EmployeeStaff',
  'dbo.dim_EmployeeStaff',
  'SourceStaffID',
  'StaffName, Role, StartDate, EndDate, ManagerID',
  2;
EXECUTE [mst].[AddDimensionMergeComponent] 'stg.dim_ManagerStaff',
  'dbo.dim_ManagerStaff',
  'SourceStaffID',
  'StaffName, Role, StartDate, EndDate',
  2;

EXECUTE [mst].[AddDimensionMergeComponent] 'stg.dim_EmployeeStaff',
  'dbo.dim_EmployeeStaff',
  'SourceStaffID',
  'imageURL',
  1;
EXECUTE [mst].[AddDimensionMergeComponent] 'stg.dim_ManagerStaff',
  'dbo.dim_ManagerStaff',
  'SourceStaffID',
  'imageURL',
  1;

EXECUTE [mst].[AddDimensionMergeComponent] 'stg.dim_Status',
  'dbo.dim_Status',
  'SourceStatusID',
  'StatusName, StatusDescription',
  1;
EXECUTE [mst].[AddDimensionMergeComponent] 'stg.dim_Location',
  'dbo.dim_Location',
  'SourceLocationID',
  'OfficeName, City, County, OfficePostcode',
  2;
EXECUTE [mst].[AddDimensionMergeComponent] 'stg.dim_WorkItem',
  'dbo.dim_WorkItem',
  'SourceWorkItemID',
  'StartDate, TargetDate, CompletedDate, SLA, Priority',
  2;
EXECUTE [mst].[AddFactMergeComponent] '#forceOrder',
  'dbo.[fact_WorkItemStateHistory]',
  'StaffSK, WorkItemSK, SnapshotStartDateSK, SnapshotStartTimeSK, StatusSK',
  'StaffSK, LocationSK, WorkItemSK',
  '[StaffSK], [WorkItemSK]',
  '[StaffSK], [LocationSK], [StatusSK], [WorkItemSK], [SnapshotStartDateSK], [SnapshotStartTimeSK], [SnapshotEndDateSK], [SnapshotEndTimeSK], [SnapshotCurrent]';
GO

INSERT INTO [mst].[MergingFlow]
VALUES      (
 'POC_DW', 1, 'Populating staging dimension tables with data from raw tables, sourced by POC_DW db', 1, 'Populating stg.dim_WorkItem dimension table with data from raw table', 'Dimension', 'EXECUTE [mst].[RawToStaging_WorkItem];');
INSERT INTO [mst].[MergingFlow]
VALUES      (
 'POC_DW', 1, 'Populating staging dimension tables with data from raw tables, sourced by POC_DW db', 2, 'Populating stg.dim_EmployeeStaff and stg.dim_ManagerStaff dimension table with data from raw table', 'Dimension', 'EXECUTE [mst].[RawToStaging_Staff];');
INSERT INTO [mst].[MergingFlow]
VALUES      (
 'POC_DW', 1, 'Populating staging dimension tables with data from raw tables, sourced by POC_DW db', 3, 'Populating stg.dim_Location dimension table with data from raw table', 'Dimension', 'EXECUTE [mst].[RawToStaging_Location];');
INSERT INTO [mst].[MergingFlow]
VALUES      (
 'POC_DW', 1, 'Populating staging dimension tables with data from raw tables, sourced by POC_DW db', 4, 'Populating stg.dim_Status dimension table with data from raw table', 'Dimension', 'EXECUTE [mst].[RawToStaging_Status];');
INSERT INTO [mst].[MergingFlow]
VALUES      (
 'POC_DW', 1, 'Populating staging dimension tables with data from raw tables, sourced by POC_DW db', 5, 'Populating stg.dim_WorkItemType dimension table with data from raw table', 'Dimension', 'EXECUTE [mst].[RawToStaging_WorkItemType];');
INSERT INTO [mst].[MergingFlow]
VALUES      (
 'POC_DW', 2, 'Merging staging dimension tables with final dimension tables', 1, 'Merging all final dimension tables with their corresponding staging counterparts', 'Dimension', 'EXECUTE [mst].[loadStagingToFinal] ''dim'';');
INSERT INTO [mst].[MergingFlow]
VALUES      (
 'POC_DW', 3, 'Populating staging fact table with data from final dimension tables', 1, 'Merging all final fact tables with their corresponding staging counterparts', 'Fact', 'EXECUTE [mst].[PopulateStagingfactWorkItemStateHistory];');
INSERT INTO [mst].[MergingFlow]
VALUES      (
 'POC_DW', 4, 'Merging staging fact table with final fact table', 1, 'Merging final fact table with their its corresponding final counterpart', 'Fact', 'EXECUTE [mst].[loadStagingToFinal] ''fact'';');
INSERT INTO [mst].[MergingFlow]
VALUES      (
 'POC_DW', 5, 'Truncates all raw and staging tables in a serial fashion', 1, 'Iterates through every raw and staging table and executes a TRUNCATE command for it', 'Raw/Staging', 'EXECUTE [mst].[TruncateRawStaging];');
GO

INSERT INTO [mst].[MergeFlowMaster]([System],
[Locked])
VALUES(
 'POC_DW', 1);
GO

INSERT INTO [mst].[SystemRawTablesMapping]
VALUES      (
 'POC_DW', 'raw.CurrentStatus');
INSERT INTO [mst].[SystemRawTablesMapping]
VALUES      (
 'POC_DW', 'raw.EmployeeStaff');
INSERT INTO [mst].[SystemRawTablesMapping]
VALUES      (
 'POC_DW', 'raw.ManagerStaff');

INSERT INTO [mst].[SystemRawTablesMapping]
VALUES      (
 'POC_DW', 'raw.StaffRole');
INSERT INTO [mst].[SystemRawTablesMapping]
VALUES      (
 'POC_DW', 'raw.Priority');
INSERT INTO [mst].[SystemRawTablesMapping]
VALUES      (
 'POC_DW', 'raw.WorkItem');
INSERT INTO [mst].[SystemRawTablesMapping]
VALUES      (
 'POC_DW', 'raw.WorkItemStateHistory');