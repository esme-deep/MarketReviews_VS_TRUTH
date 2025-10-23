-- Crée une nouvelle base de données si elle n'existe pas (Optionnel)
-- IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'Projet_Market_Staging')
-- BEGIN
--     CREATE DATABASE Projet_Market_Staging;
-- END
-- GO

USE Projet_Market_Staging;
GO


-- Objectif : Stocker la liste des produits à scraper.
IF OBJECT_ID('dbo.Staging_Scraping_Queue', 'U') IS NOT NULL
    DROP TABLE dbo.Staging_Scraping_Queue;
GO

CREATE TABLE Staging_Scraping_Queue (
    QueueID INT IDENTITY(1,1) PRIMARY KEY,
    ProductURL NVARCHAR(1024) NOT NULL,
    ProductID_SKU NVARCHAR(100) NULL,
    Status NVARCHAR(50) NOT NULL DEFAULT 'pending', -- 'pending', 'processed', 'failed'
    DiscoveredAt DATETIME2 DEFAULT GETDATE(),
    LastAttempt DATETIME2 NULL
);
GO


-- Objectif : Stocker le JSON brut de chaque page scrapée, à chaque fois.
IF OBJECT_ID('dbo.Staging_VDB_Product_Page', 'U') IS NOT NULL
    DROP TABLE dbo.Staging_VDB_Product_Page;
GO

CREATE TABLE Staging_VDB_Product_Page (
    StagingVDBKey BIGINT IDENTITY(1,1) PRIMARY KEY,
    ScrapeTimestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
    ProductID_SKU NVARCHAR(100) NOT NULL, -- ex: "7819145"
    Raw_JSON_LD NVARCHAR(MAX) NOT NULL,  -- Le JSON complet pour la robustesse
    QueueID_Ref INT NULL -- Pour lier à la tâche qui l'a générée
);
GO

