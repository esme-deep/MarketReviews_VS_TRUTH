/*
-- ÉTAPE 1 : Nettoie les doublons de JSON bruts
-- Conserve la dernière entrée (la plus récente) pour chaque SKU
*/
;WITH DedupeCTE AS (
    SELECT
        StagingVDBKey,
        ProductID_SKU,
        -- Numérote les lignes pour chaque SKU
        -- Les plus récentes (clé d'identité la plus élevée) obtiennent le N°1
        ROW_NUMBER() OVER (
            PARTITION BY ProductID_SKU
            ORDER BY StagingVDBKey DESC 
        ) AS RowNumber
    FROM
        [Projet_Market_Staging].[dbo].[Staging_VDB_Product_Page]
)
-- Supprime toutes les lignes qui ne sont PAS la plus récente
DELETE FROM DedupeCTE
WHERE RowNumber > 1;

PRINT 'Étape 1 : Nettoyage de Staging_VDB_Product_Page terminé.';



SELECT * FROM Staging_VDB_Product_Page


-- Exécutez ceci pour VOIR les doublons (s'il y en a)
;WITH DedupeCTE AS (
    SELECT
        StagingVDBKey,
        ProductID_SKU,
        ROW_NUMBER() OVER (
            PARTITION BY ProductID_SKU
            ORDER BY StagingVDBKey DESC 
        ) AS RowNumber
    FROM
        [Projet_Market_Staging].[dbo].[Staging_VDB_Product_Page]
)
-- Cette ligne filtre et ne garde QUE les doublons
SELECT *
FROM DedupeCTE
WHERE RowNumber > 1;


SELECT
    q.ProductQueueID,
    q.ProductID_SKU,
    q.ProductURL,
    q.LastAttempt
FROM
    [Projet_Market_Staging].[dbo].[Staging_Product_Queue] AS q
WHERE
    q.Status = 'processed'  -- La file d'attente dit que c'est "traité"
    AND NOT EXISTS (
        -- Mais il n'existe PAS d'entrée correspondante dans la table JSON
        SELECT 1
        FROM [Projet_Market_Staging].[dbo].[Staging_VDB_Product_Page] AS p
        WHERE p.ProductID_SKU = q.ProductID_SKU
    );

-- Vérifie le statut de toutes les sous-catégories
SELECT
    Status,
    COUNT(*) AS Nombre
FROM
    [Projet_Market_Staging].[dbo].[Staging_SubCategory_Queue]
GROUP BY
    Status;


UPDATE [Projet_Market_Staging].[dbo].[Staging_SubCategory_Queue]
SET
    Status = 'processed',
    LastAttempt = GETDATE()
WHERE
    Status = 'pending';


