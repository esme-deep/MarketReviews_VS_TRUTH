/* =======================================================================
   Script de Création du DATA WAREHOUSE 
   
   Objectif : Créer la structure de la partie VandenBorre(Dimensions et Faits)
   qui sera alimentée par la base de Staging.
======================================================================= */

-- 1. Créer la nouvelle base de données pour le DWH
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'Projet_Market_DWH')
BEGIN
    CREATE DATABASE Projet_Market_DWH;
END
GO


USE Projet_Market_DWH;
GO

/* =======================================================================
   CRÉATION DES TABLES DE DIMENSION
======================================================================= */

-- Dimension Date (sera pré-remplie)
CREATE TABLE Dim_Date (
    DateKey INT PRIMARY KEY, -- ex: 20251027
    FullDate DATE NOT NULL,
    DayOfWeekName NVARCHAR(10),
    MonthName NVARCHAR(20),
    [Month] INT,
    [Quarter] INT,
    [Year] INT
);
GO

-- Dimension Time (sera pré-remplie)
CREATE TABLE Dim_Time (
    TimeKey INT PRIMARY KEY, -- ex: 143000 (pour 14:30:00)
    [Hour] INT NOT NULL,
    [Minute] INT NOT NULL
);
GO

-- Dimension Produit (SCD Type 1 - Simple)
CREATE TABLE Dim_Product (
    ProductKey BIGINT IDENTITY(1,1) PRIMARY KEY, -- Clé "substituée"
    
    ProductID_SKU NVARCHAR(100) NOT NULL, -- Clé "métier" (ex: "7819145")
    ProductName NVARCHAR(500),
    Brand NVARCHAR(100),
    Category NVARCHAR(200)
);
GO

-- On ajoute un index unique sur le SKU (important pour les mises à jour SCD Type 1)
CREATE UNIQUE INDEX UQ_Dim_Product_SKU ON Dim_Product(ProductID_SKU);
GO

/* =======================================================================
   CRÉATION DE LA TABLE DE FAIT
======================================================================= */

-- Table de Fait (Snapshot)
CREATE TABLE Fact_Marketplace_Snapshot (
    SnapshotKey BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Clés étrangères
    DateKey INT NOT NULL,
    TimeKey INT NOT NULL,
    ProductKey BIGINT NOT NULL, -- Réfère à Dim_Product.ProductKey
    
    -- Mesures (acceptant les NULLs)
    Price DECIMAL(10, 2) NULL,
    Average_Rating FLOAT NULL, -- Accepte NULL si pas d'avis
    Review_Count INT NULL,     -- Accepte NULL si pas d'avis
    Is_Available BIT NULL,
    
    -- Liaisons
    CONSTRAINT FK_Fact_Snapshot_Dim_Date FOREIGN KEY (DateKey) REFERENCES Dim_Date(DateKey),
    CONSTRAINT FK_Fact_Snapshot_Dim_Time FOREIGN KEY (TimeKey) REFERENCES Dim_Time(TimeKey),
    CONSTRAINT FK_Fact_Snapshot_Dim_Product FOREIGN KEY (ProductKey) REFERENCES Dim_Product(ProductKey)
);
GO
