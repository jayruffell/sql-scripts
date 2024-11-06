-- Not following best practice here of having sep directories for each table. For speed, and no need for long term maintenance.

USE HectreDW;
GO

-- Dimension Tables
CREATE TABLE Dim_Picker (
    PickerID NVARCHAR(50) PRIMARY KEY,
    Name NVARCHAR(100)
);

CREATE TABLE Dim_Bin (
    BinID NVARCHAR(50) PRIMARY KEY,
    Block NVARCHAR(50),
    Variety NVARCHAR(50)
);

CREATE TABLE Dim_Defect (
    DefectID INT PRIMARY KEY IDENTITY(1,1),
    Name NVARCHAR(50)
);

CREATE TABLE Dim_Date (
    DateID INT PRIMARY KEY,
    Date DATE,
    Year INT,
    Month INT,
    Day INT
);

-- Fact Tables (No Foreign Keys)
CREATE TABLE Fact_Picking (
    PickingID INT PRIMARY KEY IDENTITY(1,1),
    BinID NVARCHAR(50), 
    PickerID NVARCHAR(50),
    Block NVARCHAR(50),
    DateID INT
);

CREATE TABLE Fact_Sampling (
    SampleID NVARCHAR(50) PRIMARY KEY,
    BinID NVARCHAR(50),
    DefectID INT,
    [Percent] INT,
    DateID INT
);
