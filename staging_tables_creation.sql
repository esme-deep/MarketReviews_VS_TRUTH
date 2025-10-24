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

USE Projet_Market_Staging;
GO

IF OBJECT_ID('dbo.Staging_Category_Queue', 'U') IS NOT NULL
    DROP TABLE dbo.Staging_Category_Queue;
GO

CREATE TABLE Staging_Category_Queue (
    CategoryQueueID INT IDENTITY(1,1) PRIMARY KEY,
    
    CategoryName NVARCHAR(255) NOT NULL, -- ex: "Télévision"
    CategoryURL NVARCHAR(1024) NOT NULL,   -- ex: "https://.../tv-audio/television"
    
    -- "Univers" ou "Parent" pour garder la hiérarchie. 
    -- Pour ce premier niveau, il peut être NULL ou "TV et Audio"
    ParentCategoryName NVARCHAR(255) NULL, 
    
    -- Colonnes de gestion du pipeline
    Status NVARCHAR(50) NOT NULL DEFAULT 'pending', -- 'pending' (à scraper), 'processed' (scrapé)
    DiscoveredAt DATETIME2 DEFAULT GETDATE(),
    LastAttempt DATETIME2 NULL
);
GO

USE Projet_Market_Staging;
GO

/* =======================================================================
 Table 4 : La File d'Attente des SOUS-Catégories
 Objectif : Stocker la liste des sous-catégories (celles qui ont 
            un nombre d'articles) à scraper pour trouver les produits.
======================================================================= */

IF OBJECT_ID('dbo.Staging_SubCategory_Queue', 'U') IS NOT NULL
    DROP TABLE dbo.Staging_SubCategory_Queue;
GO

CREATE TABLE Staging_SubCategory_Queue (
    SubCategoryQueueID INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Clé étrangère vers la table des "thèmes" (Staging_Category_Queue)
    ParentCategoryQueueID INT NOT NULL, 
    
    SubCategoryName NVARCHAR(255) NOT NULL, -- ex: "Casques audio"
    SubCategoryURL NVARCHAR(1024) NOT NULL,  -- ex: ".../mp3-casque-ecouteurs/casque"
    ItemCount INT NULL,                      -- ex: 174
    
    -- Statut pour le pipeline de PRODUITS
    -- 'pending' -> On doit scraper cette URL pour trouver les produits
    -- 'processed' -> Tâche terminée
    Status NVARCHAR(50) NOT NULL DEFAULT 'pending',
    
    DiscoveredAt DATETIME2 DEFAULT GETDATE(),
    LastAttempt DATETIME2 NULL,
    
    -- Crée la liaison formelle entre les deux tables
    CONSTRAINT FK_SubCategory_ParentCategory 
        FOREIGN KEY (ParentCategoryQueueID) 
        REFERENCES Staging_Category_Queue(CategoryQueueID)
);
GO


