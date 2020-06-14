INSERT INTO POC_DW.raw.StaffRole
       SELECT ROW_NUMBER() OVER(
              ORDER BY jobtitle) RN, 
              jobtitle, 
              GETDATE(), 
              'Denis Stepan'
       FROM
       (
           SELECT DISTINCT 
                  jobtitle
           FROM AdventureWorks2017.HumanResources.employee
       ) x1;
GO
INSERT INTO [POC_DW].[raw].[EmployeeStaff]
       SELECT TOP 49 PERCENT p.[BusinessEntityID], 
                             [FirstName], 
                             [LastName], 
                             CONCAT(LEFT(TRIM(Firstname), 1), LEFT(TRIM(LastName), 1)) Initials, 
                             st.Role_ID, 
                             NULL imageurl, 
                             LEFT(CAST(p.BusinessEntityID AS VARCHAR(5)), 1) Location, 
                             e.HireDate StartDate, 
                             NULL EndDate
       FROM [AdventureWorks2017].[Person].[Person] p
            INNER JOIN AdventureWorks2017.HumanResources.employee e ON p.BusinessEntityID = e.BusinessEntityID
            INNER JOIN POC_DW.raw.StaffRole st ON e.JobTitle COLLATE SQL_Latin1_General_CP1_CI_AS = st.Role COLLATE SQL_Latin1_General_CP1_CI_AS
       ORDER BY BusinessEntityID;
GO
INSERT INTO [POC_DW].[raw].[ManagerStaff]
       SELECT TOP 49 PERCENT p.[BusinessEntityID], 
                             [FirstName], 
                             [LastName], 
                             CONCAT(LEFT(TRIM(Firstname), 1), LEFT(TRIM(LastName), 1)) Initials, 
                             st.Role_ID, 
                             NULL imageurl, 
                             LEFT(CAST(p.BusinessEntityID AS VARCHAR(5)), 1) Location, 
                             e.HireDate StartDate, 
                             NULL EndDate
       FROM [AdventureWorks2017].[Person].[Person] p
            INNER JOIN AdventureWorks2017.HumanResources.employee e ON p.BusinessEntityID = e.BusinessEntityID
            INNER JOIN POC_DW.raw.StaffRole st ON e.JobTitle COLLATE SQL_Latin1_General_CP1_CI_AS = st.Role COLLATE SQL_Latin1_General_CP1_CI_AS
       ORDER BY BusinessEntityID DESC;
GO
INSERT INTO POC_DW.raw.Location
VALUES
(1, 
 'Zurich', 
 'Switzerland', 
 'Zurich'
);
INSERT INTO POC_DW.raw.Location
VALUES
(2, 
 'Quebec', 
 'Canada', 
 'Quebec'
);
INSERT INTO POC_DW.raw.Location
VALUES
(3, 
 'Tokyo', 
 'Japan', 
 'Tokyo'
);
INSERT INTO POC_DW.raw.Location
VALUES
(4, 
 'Sydney', 
 'Australia', 
 'Sydney'
);
INSERT INTO POC_DW.raw.Location
VALUES
(5, 
 'London', 
 'UK', 
 'London'
);
INSERT INTO POC_DW.raw.Location
VALUES
(6, 
 'Washington', 
 'USA', 
 'Washington'
);
INSERT INTO POC_DW.raw.Location
VALUES
(7, 
 'Malmo', 
 'Sweden', 
 'Malmo'
);
INSERT INTO POC_DW.raw.Location
VALUES
(8, 
 'Copenhagen', 
 'Netherlands', 
 'Copenhagen'
);
INSERT INTO POC_DW.raw.Location
VALUES
(9, 
 'Oslo', 
 'Norway', 
 'Oslo'
);
INSERT INTO POC_DW.raw.Location
VALUES
(10, 
 'Frankfurt', 
 'Germany', 
 'Frankfurt'
);
GO
INSERT INTO POC_DW.raw.CurrentStatus
VALUES
(1, 
 'Not Started'
);
INSERT INTO POC_DW.raw.CurrentStatus
VALUES
(2, 
 'In Progress'
);
INSERT INTO POC_DW.raw.CurrentStatus
VALUES
(3, 
 'Completed'
);
INSERT INTO POC_DW.raw.CurrentStatus
VALUES
(4, 
 'Paused'
);
INSERT INTO POC_DW.raw.CurrentStatus
VALUES
(5, 
 'Cancelled'
);
GO
INSERT INTO POC_DW.raw.Priority
VALUES
(1, 
 'Low'
);
INSERT INTO POC_DW.raw.Priority
VALUES
(2, 
 'Medium'
);
INSERT INTO POC_DW.raw.Priority
VALUES
(3, 
 'High'
);
GO
INSERT INTO POC_DW.raw.WorkItem
       SELECT DISTINCT 
              [ProductModelID], 
              CONCAT('Review manuals for: ', [Name]) [Description], 
              LEFT(CAST(ProductModelID AS VARCHAR(3)), 1) SLA, 
              'Manual Review' Subject, 
              IIF(ProductModelID >= 50, (ProductModelID % 2) + 2, (ProductModelID % 2) + 1) PriorityID, 
              GETDATE() [StartDate], 
              DATEADD(D, CAST(LEFT(CAST(ProductModelID AS VARCHAR(3)), 1) AS INT), GETDATE()) [TargetDate], 
              NULL CompletedDate, 
              NULL DaysPausedFor, 
              NULL PausedUntilDate, 
              1 IsNew, 
              ProductModelID AssignedTo
       FROM [AdventureWorks2017].[Production].[ProductModel];
GO
INSERT INTO POC_DW.raw.WorkItemStateHistory
       SELECT ROW_NUMBER() OVER(
              ORDER BY WorkItemID) [WorkItemStateHistoryID], 
              WorkItemID, 
              LEFT(CAST(WorkITemID AS VARCHAR(3)), 1) [StatusID], 
              LEFT(CAST(WorkItemID AS VARCHAR(3)), 1) [LocationID], 
              GETDATE() [StartDate], 
              NULL [EndDate], 
              0 [ElapsedDays]
       FROM POC_DW.raw.WorkItem;
GO