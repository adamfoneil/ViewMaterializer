USE [master]
GO
CREATE DATABASE [MaterializerSample]
GO

USE [master]
GO
ALTER DATABASE [MaterializerSample] SET CHANGE_TRACKING = ON 
GO

USE [MaterializerSample]
GO

CREATE TABLE [dbo].[Region] (
	[Name] nvarchar(50) NOT NULL PRIMARY KEY,
	[Id] int identity(1,1),
	CONSTRAINT [U_Region_Id] UNIQUE ([Id])
)
GO

INSERT INTO [dbo].[Region] ([Name]) VALUES ('North'), ('South'), ('East'), ('West')
GO

CREATE TABLE [dbo].[Item] (
	[Name] nvarchar(50) NOT NULL PRIMARY KEY,
	[Cost] money NOT NULL,
	[Id] int identity(1,1),
	CONSTRAINT [U_Item_Id] UNIQUE ([Id])
)
GO

-- will create random items in app

CREATE TABLE [dbo].[Sales] (
	[RegionId] int NOT NULL,
	[ItemId] int NOT NULL,
	[Date] date NOT NULL,
	[Quantity] int NOT NULL,
	[Id] int identity(1,1) PRIMARY KEY
)
GO

ALTER TABLE [dbo].[Sales] ENABLE CHANGE_TRACKING 
GO

CREATE VIEW [dbo].[SalesPivot]
AS
SELECT
	[ItemName],
	[RegionName],
	[Year],
	[Q1], [Q2], [Q3], [Q4]
FROM (
	SELECT
		[i].[Name] AS [ItemName],
		[r].[Name] AS [RegionName],
		YEAR([s].[Date]) AS [Year],
		'Q' + CONVERT(varchar, DATEPART(q, [s].[Date])) AS [Quarter],
		SUM([s].[Quantity]) AS [TotalQuantity]
	FROM
		[dbo].[Sales] [s]
		INNER JOIN [dbo].[Region] [r] ON [s].[RegionId]=[r].[Id]
		INNER JOIN [dbo].[Item] [i] ON [s].[ItemId]=[i].[Id]
	GROUP BY
		[i].[Name],
		[r].[Name],
		YEAR([s].[Date]),
		DATEPART(q, [s].[Date])
) AS [source]
PIVOT (
	SUM([TotalQuantity]) FOR [Quarter] IN ([Q1], [Q2], [Q3], [Q4])
) AS [pivot]
GO

CREATE SCHEMA [rpt]
GO
SELECT * INTO [rpt].[SalesPivot] FROM [dbo].[SalesPivot]

ALTER TABLE [rpt].[SalesPivot] ALTER COLUMN [ItemName] nvarchar(50) NOT NULL
ALTER TABLE [rpt].[SalesPivot] ALTER COLUMN [RegionName] nvarchar(50) NOT NULL
ALTER TABLE [rpt].[SalesPivot] ALTER COLUMN [Year] int NOT NULL
ALTER TABLE [rpt].[SalesPivot] ADD CONSTRAINT [PK_rptSalesPivot] PRIMARY KEY ([ItemName], [RegionName], [Year])
GO

CREATE FUNCTION [dbo].[FnSalesChanges](@version bigint)
RETURNS @results TABLE (
	[ItemName] nvarchar(50) NOT NULL,
	[RegionName] nvarchar(50) NOT NULL,
	[Year] int NOT NULL
) AS
BEGIN
	INSERT INTO @results (
		[ItemName], [RegionName], [Year]
	) SELECT
		[i].[Name], [r].[Name], YEAR([s].[Date])
	FROM
		CHANGETABLE(CHANGES dbo.Sales, @version) as [changes]
		INNER JOIN [dbo].[Sales] [s] ON [changes].[Id]=[s].[Id]
		INNER JOIN [dbo].[Item] [i] ON [s].[ItemId]=[i].[Id]
		INNER JOIN [dbo].[Region] [r] ON [s].[RegionId]=[r].[Id]
	GROUP BY
		[i].[Name], [r].[Name], YEAR([s].[Date])
	RETURN
END

