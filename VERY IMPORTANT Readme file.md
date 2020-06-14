# Autonomous-Accumulating-Snapshot-Fact-Table-Architecture

### I SUGGEST VIEWING THIS IN RAW FORMAT, USING THE RAW BUTTON FROM THE HEADER ###


Author: Denis Stepan
First Uploaded: 14/06/2020
Last Edit: 14/06/2020

IMPORTANT: All data used in this project is fictitious, based on AdventureWorks2017 DB and random names that came to my mind.

Short Description:
This Timespan Accumulating Snapshot Fact Table architecture attempts to make the Data Warehouse ETL process somewhat autonomous, and help the developer by reducing the amount of thought and code that needs to go into designing bespoke staging to final merge scripts, or even building full Transform and Load processes for each system.
It does so by mapping common SCD Types patterns into code and then uses the available metadata to compute carefully put together scripts.

Find out more about Timespan Accumulating Snapshots here:
https://www.kimballgroup.com/2012/05/design-tip-145-time-stamping-accumulating-snapshot-fact-tables/
https://www.nuwavesolutions.com/accumulating-snapshot-fact-tables/

Repurposing:
Although this example pertains specifically to the Timespan Accumulating Snapshot Fact Table architecture, this can easily be repurposed for normal DW designs, in order to leverage the automatic creation of SCD scripts.

Note:
For the purposes of this example, we're not going to get into the Data Integration side of this work, therefore SSIS will not be a part of this project; rather, the data inserts will be done manually as the scope of this project is to demonstrate the functionality and architecture of this system.
Part of the data in the inserts will be coming from the public AdventureWorks2017 database.

Prerequisites:
Local instance of SQL Server
SQL Server Management Studio
AdventureWorks2017 database (can be found here: https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorks2017.bak)
POC_DW DDL (can be found in repository)
POC_DW DML (can be found in repository)
POC_DW Insert (can be found in repository)

--OPTIONAL-- POC_DW Dictionary (can be found in repository): Although this file is optional, you might want to download it as it contains comments and description for every stored procedure that's part of the main logic of the code.

RUN INSTRUCTIONS:
1. Run SQL Server Management Studio
2. Restore the backup for AdventureWorks2017
3. Execute the contents of POC_DW DDL
4. After switching connection to POC_DW, execute the contents of POC_DW DML
5. Execute the contents of POC_DW Insert
6. Execute mst.[CheckSystemsForProcess]

Premise and scope:
  When creating a Data Warehouse system, developers are working not only designing the architecture of the database, but they also build the Data Integration (Extract) side, the Data Transformation side and the Loading (Merging) side. This project will focus on lifting some of the workload (if not all) of the latter.
  
  When it comes to building the loading side of the project, developers have to carefully consider every container/table from the transformation/staging side, and meticulously map every bit of the metadata with the final containers/tables, where the polished form of the data will be stored. 
This often involves creating complex merge scripts, depending on different columns being assigned to different slowly changing dimension types, and having the code execute in a correct order so that no constraint will be violated. This work has to be bespoke for every table and has to be done for every table, which can take a considerable abmount of time. This is what this project will try to address.

  Based on the logic of this project, the developer will no longer need to spend time on thinking, testing and creating bespoke scripts for each table; rather, all the developer will need to do will be to populate some key tables in the master (mst) schema with metadata that pertains to how the code should behave when working with each table.
  
  For example, when designing a merge script from the staging to the final side for a table containing 15 columns, out of which 7 would be of SCD Type 1 and 6 other columns would be SCD Type 2, a developer would have to account not only for the insertion of new records, but also design code on how to behave around columns that are of different SCD Types.
  Using this system, rather than creating bespoke scripts, as described above, for each table, the developer will have to populate a few tables to specify when merging the dimension what the target, source, key column and SCD Type will be, and the system will take care of everything else. Using string manipulation within computed columns and stored procedures.
  
How it helps:
  Say you've got a dimension table such as the below, where imageURL is of SCD Type 1, while StaffName, Role, StartDate, EndDate, ManagerID are of SCD Type 2:
  
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

To merge the staging side with the final side of this dimension table, the developer would have to write something like this:

INSERT INTO ##x1
       SELECT [SourceStaffID], 
              [StaffName], 
              [Role], 
              [ImageURL], 
              [StartDate], 
              [EndDate], 
              [ValidFrom], 
              [ValidTo], 
              [CurrentFlag]
       FROM(
           MERGE dbo.[dim_ManagerStaff] AS target
           USING stg.[dim_ManagerStaff] AS source
           ON source.sourceStaffid = target.sourceStaffid
               WHEN MATCHED AND target.ValidTo IS NULL
                                AND EXISTS
           (
               SELECT source.StaffName, 
                      source.Role, 
                      source.StartDate, 
                      source.EndDate
               EXCEPT
               SELECT target.StaffName, 
                      target.Role, 
                      target.StartDate, 
                      target.EndDate
           )
               THEN UPDATE SET 
                               ValidTo = GETDATE(), 
                               CurrentFlag = 0
           OUTPUT $ACTION ActionOut, 
                  source.[StaffSK], 
                  source.[SourceStaffID], 
                  source.[StaffName], 
                  source.[Role], 
                  source.[ImageURL], 
                  source.[StartDate], 
                  source.[EndDate], 
                  source.[ValidFrom], 
                  source.[ValidTo], 
                  source.[CurrentFlag]) AS MergeOut
       WHERE MergeOut.ActionOut = 'UPDATE'
             AND [StaffSK] != -1;
INSERT INTO dbo.[dim_ManagerStaff]
([SourceStaffID], 
 [StaffName], 
 [Role], 
 [ImageURL], 
 [StartDate], 
 [EndDate], 
 [ValidFrom], 
 [ValidTo], 
 [CurrentFlag]
)
       SELECT [SourceStaffID], 
              [StaffName], 
              [Role], 
              [ImageURL], 
              [StartDate], 
              [EndDate], 
              GETDATE(), 
              [ValidTo], 
              [CurrentFlag]
       FROM ##x1;
       
Instead of thinking, building and testing every line of this code, the developer using this system will have to add a reference of the source table, target table, key column, SCD columns and SCD Type in one table:

  EXECUTE [mst].[AddDimensionMergeComponent] 'stg.dim_EmployeeStaff',
  'dbo.dim_EmployeeStaff',
  'SourceStaffID',
  'StaffName, Role, StartDate, EndDate, ManagerID',
  2;
  EXECUTE [mst].[AddDimensionMergeComponent] 'stg.dim_EmployeeStaff',
  'dbo.dim_EmployeeStaff',
  'SourceStaffID',
  'imageURL',
  1;
  
  Once this is done, the computed column and the stored procedures automatically build the above script without the developer ever having to touch the keyboard for it.
  

Methodology:
  We start off by creating POC_DW (Proof Of Concept_Data Warehouse) database. We then create all of the objects and insert metadata into the objects. We, then, populate the tables in the raw schema with raw data coming from source systems. 
  The raw schema contains tables that stores the data as it comes from the source systems, in its original form. Once all of the raw tables are populated (i.e. the daily extract and load job is finished), executing Stored Procedure mst.CheckSystemsForProcess will set in motion the following:
  
  1. It will iterate thorugh every raw table mapped to every DW system in mst.[SystemRawTablesMapping] and check whether all of the raw tables for either system have data in them. Once it's been established which systems have all their raw tables populated, this SP will change the BIT column (column is found in mst.MergeFlowMaster) from Locked to Unlocked, to reflect that the DW system is ready for processing, and call SP [mst].[ProcessUnlockedSystems]
  2. As the name suggests, [mst].[ProcessUnlockedSystems] will iterate through every Unlocked table mapped in mst.MergeFlowMaster, and trigger the stored procedure required to initiate the (re-)processing of the system; in our case, it will call mst.MergeFlow 'POC_DW'.
  3. The aforementioned SP will iterate through every row of mst.[MergingFlow], where we've mapped every step and sub-step (step component) that needs to be executed for each respective DW System; in our case, for POC_DW, this will trigger the following:
    I.   Populate staging dimension tables with data from the raw tables
    II.  Merge staging dimension with final dimension
    III. Populate staging fact table with data from final dimension tables
    IV.  Merge staging fact table with final fact table
    V.   Truncate all raw and staging tables, serially
  4. Once all the above is executed, the system is updated as being processed, in mst.MergeFlowMaster, and is locked again until another load has been made and the SP is called again.
