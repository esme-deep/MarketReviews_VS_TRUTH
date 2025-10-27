/* =======================================================================
   Script de Pré-remplissage pour le DWH (Projet_Market_DWH)
   
   Objectif : Remplir les tables 'Dim_Date' et 'Dim_Time'.
   À exécuter une seule fois.
======================================================================= */
USE Projet_Market_DWH;
GO


-- ---------------------------------
-- Remplissage de Dim_Date (2024-2026)
-- ---------------------------------
IF (SELECT COUNT(1) FROM dbo.Dim_Date) > 0
BEGIN
    PRINT 'Dim_Date est déjà remplie. Le remplissage est ignoré.';
END
ELSE
BEGIN
    PRINT 'Remplissage de Dim_Date pour 2024-2026...';
    
    DECLARE @StartDate DATE = '2024-01-01';
    DECLARE @EndDate DATE = '2026-12-31';

    -- Utilise une CTE récursive pour générer toutes les dates
    ;WITH DateCTE AS (
        SELECT @StartDate AS FullDate
        UNION ALL
        SELECT DATEADD(DAY, 1, FullDate)
        FROM DateCTE
        WHERE FullDate < @EndDate
    )
    INSERT INTO dbo.Dim_Date (
        DateKey, 
        FullDate, 
        DayOfWeekName, 
        MonthName, 
        [Month], 
        [Quarter], 
        [Year]
    )
    SELECT
        CONVERT(INT, FORMAT(FullDate, 'yyyyMMdd')) AS DateKey,
        FullDate,
        FORMAT(FullDate, 'dddd', 'fr-FR') AS DayOfWeekName,
        FORMAT(FullDate, 'MMMM', 'fr-FR') AS MonthName,
        DATEPART(MONTH, FullDate) AS [Month],
        DATEPART(QUARTER, FullDate) AS [Quarter],
        DATEPART(YEAR, FullDate) AS [Year]
    FROM DateCTE
    OPTION (MAXRECURSION 0); -- Nécessaire pour générer 1096 jours

END
GO

-- ---------------------------------
-- Remplissage de Dim_Time
-- ---------------------------------
IF (SELECT COUNT(1) FROM dbo.Dim_Time) > 0
BEGIN
    PRINT 'Dim_Time est déjà remplie. Le remplissage est ignoré.';
END
ELSE
BEGIN
    PRINT 'Remplissage de Dim_Time (1440 minutes)...';
    
    ;WITH TimeCTE AS (
        SELECT CAST('00:00:00' AS TIME) AS FullTime
        UNION ALL
        SELECT DATEADD(MINUTE, 1, FullTime)
        FROM TimeCTE
        WHERE FullTime < '23:59:00'
    )
    INSERT INTO dbo.Dim_Time (
        TimeKey, 
        [Hour], 
        [Minute]
    )
    SELECT
        (DATEPART(HOUR, FullTime) * 10000) + (DATEPART(MINUTE, FullTime) * 100) AS TimeKey, -- Format HHMMSS
        DATEPART(HOUR, FullTime) AS [Hour],
        DATEPART(MINUTE, FullTime) AS [Minute]
    FROM TimeCTE
    OPTION (MAXRECURSION 0); -- Nécessaire pour 1440 minutes
    
    PRINT '-> Dim_Time remplie avec succès.';
END
GO

PRINT '--- Pré-remplissage des dimensions terminé ---';
GO