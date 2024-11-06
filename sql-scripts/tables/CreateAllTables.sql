-- Not following best practice here of having sep directories for each table. For speed, and no need for long term maintenance.

USE HectreDW;
GO

-- CREATE TABLE for DimPicker
CREATE TABLE [dbo].[DimPicker] (
    [PickerID] INT,
    [PickerName] NVARCHAR(MAX)
);

-- CREATE TABLE for DimDefect
CREATE TABLE [dbo].[DimDefect] (
    [DefectID] INT,
    [DefectType] NVARCHAR(MAX)
);

-- CREATE TABLE for BridgeSamplePicker
CREATE TABLE [dbo].[BridgeSamplePicker] (
    [SampleID] INT,
    [PickerID] INT
);

-- CREATE TABLE for BridgeSampleDefect
CREATE TABLE [dbo].[BridgeSampleDefect] (
    [SampleID] INT,
    [DefectID] INT,
    [DefectPercent] FLOAT
);

-- CREATE TABLE for BridgeBinPicker
CREATE TABLE [dbo].[BridgeBinPicker] (
    [BinID] INT,
    [PickerID] INT
);

-- CREATE TABLE for FactSample
CREATE TABLE [dbo].[FactSample] (
    [SampleID] INT,
    [BinID] INT,
    [TotalPickers] INT,
    [TotalDefectPercent] FLOAT
);

-- CREATE TABLE for FactBin
CREATE TABLE [dbo].[FactBin] (
    [BinID] INT,
    [Block] NVARCHAR(MAX),
    [Variety] NVARCHAR(MAX),
    [CreatedDate] DATETIME,
    [TotalPickers] INT
);
