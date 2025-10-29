USE Projet_Market_Staging;
GO

/* =======================================================================
 Table 7 : Staging des Données Reddit (Brutes)
 Objectif : Stocker les posts/commentaires bruts trouvés par l'API
            avant l'analyse de sentiment.
======================================================================= */
USE Projet_Market_Staging;
GO

/* =======================================================================
 Table 7 : Staging des Données Reddit (Brutes) - VERSION 2
 Objectif : Stocker les posts bruts AVEC les métadonnées de recherche.
======================================================================= */

-- 1. Supprime l'ancienne table (si elle existe)
IF OBJECT_ID('dbo.Staging_Reddit_Posts', 'U') IS NOT NULL
    DROP TABLE dbo.Staging_Reddit_Posts;
GO

-- 2. Crée la nouvelle table
CREATE TABLE Staging_Reddit_Posts (
    StagingPostID BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Référence à notre DWH pour savoir de quel produit on parle
    ProductKey_Ref BIGINT NOT NULL, 
    
    -- NOUVELLES COLONNES DE TRAÇABILITÉ
    ProductName_Ref NVARCHAR(500) NULL, -- Le nom complet du produit VDB (ex: "JVC LED HD...")
    Search_Keyword_Used NVARCHAR(255) NULL, -- Le mot-clé généré (ex: "JVC HD LT-32FV140 32")
    
    -- Données brutes de l'API Reddit
    PostID NVARCHAR(50) NOT NULL,    
    PostTitle NVARCHAR(500),
    PostText NVARCHAR(MAX),          
    PostURL NVARCHAR(1024),
    PostDate DATETIME2,              
    AuthorName NVARCHAR(255),        
    
    -- Métadonnées de notre pipeline
    ScrapeTimestamp DATETIME2 DEFAULT GETDATE(),
    Status NVARCHAR(50) NOT NULL DEFAULT 'pending_sentiment' 
);
GO

-- Créer un index pour éviter de scraper le même post deux fois
CREATE UNIQUE INDEX UQ_Staging_Reddit_Posts_PostID ON Staging_Reddit_Posts(PostID);
GO

PRINT 'Table Staging_Reddit_Posts (Version 2) créée avec succès.';
GO

-- Vider la table de sentiment (car les StagingPostID_Ref ne sont plus valides)
TRUNCATE TABLE Staging_Reddit_Sentiment;
PRINT 'Table Staging_Reddit_Sentiment vidée.';
GO

/* =======================================================================
 Table 8 : Staging du Sentiment (Nettoyé)
 Objectif : Stocker le résultat de notre analyse de sentiment (VADER)
            prêt à être chargé dans le DWH.
======================================================================= */

IF OBJECT_ID('dbo.Staging_Reddit_Sentiment', 'U') IS NOT NULL
    DROP TABLE dbo.Staging_Reddit_Sentiment;
GO

CREATE TABLE Staging_Reddit_Sentiment (
    SentimentID BIGINT IDENTITY(1,1) PRIMARY KEY,
    StagingPostID_Ref BIGINT NOT NULL, 
    ProductKey_Ref BIGINT NOT NULL,
    AuthorName NVARCHAR(255),
    PostDate DATETIME2,
    Sentiment_Score FLOAT, 
    ProcessedAt DATETIME2 DEFAULT GETDATE(),
    Status NVARCHAR(50) NOT NULL DEFAULT 'pending_dwh' 
);
GO

PRINT 'Table Staging_Reddit_Sentiment créée avec succès.';
GO