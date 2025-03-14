IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'UnitTest')
BEGIN
    CREATE SCHEMA UnitTest;
END
GO

CREATE TABLE UnitTest.TestDataResults (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    TestRunId UNIQUEIDENTIFIER NOT NULL,
    TestId NVARCHAR(50) NOT NULL,
    EntityName NVARCHAR(100) NOT NULL,
    EntityKey NVARCHAR(100) NOT NULL,
    PropertyName NVARCHAR(100) NOT NULL,
    PropertyValue NVARCHAR(MAX) NULL,
    PropertyType NVARCHAR(50) NOT NULL
);
GO 


CREATE TABLE UnitTest.TestMetadataResults (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    TestRunId UNIQUEIDENTIFIER NOT NULL,
    TestId NVARCHAR(50) NOT NULL,
    TestCategory NVARCHAR(50) NOT NULL,
    RuleFunction NVARCHAR(50) NOT NULL,
    ExecutionDateTime DATETIME NOT NULL DEFAULT GETDATE(),
    ExecutionStatus NVARCHAR(20) NOT NULL
);