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

USE Projet_Market_Staging;
GO

/* =======================================================================
 Table 5 : La File d'Attente des PRODUITS (pour l'Étape 1)
 Objectif : Stocker la liste de TOUS les produits individuels
            que notre scraper final (Étape 2) doit visiter.
======================================================================= */

IF OBJECT_ID('dbo.Staging_Product_Queue', 'U') IS NOT NULL
    DROP TABLE dbo.Staging_Product_Queue;
GO

-- C'est votre table 'Staging_Scraping_Queue' originale
CREATE TABLE Staging_Product_Queue (
    ProductQueueID INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Clé étrangère vers la table des sous-catégories
    SubCategoryQueueID INT NOT NULL, 
    
    ProductID_SKU NVARCHAR(100) NOT NULL, -- ex: "7819145"
    ProductURL NVARCHAR(1024) NOT NULL,
    
    Status NVARCHAR(50) NOT NULL DEFAULT 'pending', -- 'pending', 'processed', 'failed'
    
    DiscoveredAt DATETIME2 DEFAULT GETDATE(),
    LastAttempt DATETIME2 NULL,
    
    CONSTRAINT FK_Product_SubCategory 
        FOREIGN KEY (SubCategoryQueueID) 
        REFERENCES Staging_SubCategory_Queue(SubCategoryQueueID)
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




/* =======================================================================
 Table 6 : Staging "Nettoyé" (Silver Layer)
 Objectif : Stocker les données parsées du JSON, prêtes à être
            chargées dans le DWH.
======================================================================= */

IF OBJECT_ID('dbo.Staging_Product_Cleansed', 'U') IS NOT NULL
    DROP TABLE dbo.Staging_Product_Cleansed;
GO

CREATE TABLE Staging_Product_Cleansed (
    CleansedProductID INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Clé de référence vers le log brut
    StagingVDBKey_Ref BIGINT NOT NULL,
    
    -- Données pour Dim_Product (Catalogue)
    SKU NVARCHAR(100) NOT NULL,
    ProductName NVARCHAR(500),
    Brand NVARCHAR(100),
    Category NVARCHAR(200),
    
    -- Données pour Fact_Marketplace_Snapshot (Performances)
    Price DECIMAL(10, 2),
    Average_Rating FLOAT,
    Review_Count INT,
    Availability NVARCHAR(255), -- "https://schema.org/InStock"
    
    -- Métadonnées
    ProcessedAt DATETIME2 DEFAULT GETDATE()
);
GO

-- On ajoute une colonne "Status" à la table brute pour savoir ce qu'on a traité
IF NOT EXISTS (SELECT 1 FROM sys.columns 
               WHERE Name = N'Status' 
               AND Object_ID = Object_ID(N'dbo.Staging_VDB_Product_Page'))
BEGIN
    ALTER TABLE Staging_VDB_Product_Page
    ADD Status NVARCHAR(50) NOT NULL DEFAULT 'pending';
    PRINT 'Colonne Status ajoutée à Staging_VDB_Product_Page.';
END
GO
