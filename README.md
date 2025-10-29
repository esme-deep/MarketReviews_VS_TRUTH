# Projet : MarketReviews VS TRUTH (Analyse de Sentiment)

Ce projet est un pipeline de Data Engineering complet conçu pour extraire, transformer et charger (ETL) des données de deux sources distinctes afin d'analyser la corrélation entre les avis d'un site e-commerce (Vanden Borre) et le sentiment du public sur un forum (Reddit).

L'objectif final est de répondre à la question : "Est-ce que les notes d'un produit sur le site de vente reflètent ce que le public en pense vraiment ?"

* **Source 1 (Market Reviews) :** Vanden Borre (Scraping Web)
* **Source 2 (Public TRUTH) :** API Reddit
* **Staging :** Base de données `Projet_Market_Staging` (SQL Server)
* **Data Warehouse (DWH) :** Base de données `Projet_Market_DWH` (SQL Server)

---

## Architecture des Données

Le projet utilise deux bases de données pour séparer les données brutes/intermédiaires (Staging) des données finales prêtes pour l'analyse (DWH).

### 1. Base de Staging (`Projet_Market_Staging`)

Cette base sert de zone de "travail" et de file d'attente (queue) pour les différents pipelines.

* **Tables Pipeline VDB :**
    * `Staging_Category_Queue` : Stocke les "thèmes" (ex: "Télévision").
    * `Staging_SubCategory_Queue` : Stocke les sous-catégories (ex: "TV OLED (42)").
    * `Staging_Product_Queue` : Stocke les URL des produits à scraper (1773 produits trouvés).
    * `Staging_VDB_Product_Page` : Stocke le JSON-LD brut de chaque page produit.
    * `Staging_Product_Cleansed` : Stocke le JSON parsé en colonnes (Prix, Note, SKU, etc.).

* **Tables Pipeline Reddit :**
    * `Staging_Reddit_Posts` : Stocke les posts bruts (titre, texte, auteur, date) avec des colonnes de traçabilité (`ProductKey_Ref`, `Search_Keyword_Used`) pour garantir la pertinence.
    * `Staging_Reddit_Sentiment` : Stocke le résultat de l'analyse de sentiment (le score) pour chaque post.

### 2. Data Warehouse (`Projet_Market_DWH`)

C'est la source finale pour l'analyse (Power BI). Il utilise un **schéma en constellation** : deux tables de faits partagent des dimensions communes.

* **Dimensions :**
    * `Dim_Product` : Le "pont" central (dimension conformée). Contient la liste des produits uniques (1772 produits). Gérée en **SCD Type 1** (les modifications écrasent les anciennes données).
    * `Dim_Date` : Table pré-remplie (2024-2026).
    * `Dim_Time` : Table pré-remplie (1440 minutes).
    * `Dim_Reddit_User` (Future) : Stockera les auteurs Reddit (probablement hachés/anonymisés).

* **Tables de Faits :**
    * `Fact_Marketplace_Snapshot` : Stocke les "photos" des métriques de Vanden Borre (Prix, Note, Nb Avis). Les colonnes de rating **acceptent les `NULL`** pour les produits sans avis.
    * `Fact_Public_Sentiment` (Future) : Stockera les scores de sentiment de Reddit.


---

## Structure du Code (Python)

Le projet est divisé en "Moteurs" (`.py`) et "Pilotes" (`.ipynb`) pour faciliter l'automatisation et la maintenance.

### Fichiers Moteur (Logique)

* `vdb_pipeline_engine.py` : Contient toutes les fonctions de scraping (Requests/BeautifulSoup) et de chargement pour le pipeline Vanden Borre (Étapes 0a, 0b, 1, 2).
* `etl_transformer.py` : Contient la logique de transformation (Étape 3) pour parser le JSON brut de VDB et le charger dans `Staging_Product_Cleansed`.
* `etl_dwh_loader.py` : Contient la logique ETL (Étape 4) pour migrer les données de `Staging_Product_Cleansed` vers le DWH (`Dim_Product` et `Fact_Marketplace_Snapshot`).
* `reddit_pipeline_engine.py` : Contient la logique (Étape 6) pour :
    1.  Lire `Dim_Product` (en filtrant les produits déjà traités).
    2.  Générer des mots-clés de recherche propres (ex: "LG C5 OLED... (2025)" -> "LG C5").
    3.  Mapper les catégories VDB à des subreddits spécifiques (ex: "TV OLED" -> "r/OLED_Gaming").
    4.  Appeler l'API Reddit (avec une pause de 7 secondes intégrée pour la sécurité).
    5.  Charger les résultats bruts dans `Staging_Reddit_Posts`.

### Fichiers Pilote (Exécution)

* `run_staging_pipeline.ipynb` : Notebook pour exécuter les étapes 0 à 3 (le pipeline VDB complet, du scraping à la transformation).
* `run_dwh_migration.ipynb` : Notebook pour exécuter l'étape 4 (le chargement Staging VDB -> DWH).
* `run_reddit_pipeline.ipynb` : Notebook pour exécuter l'étape 6 (le pipeline d'extraction Reddit).

---

## État Actuel du Pipeline

### Pipeline 1 : Vanden Borre (Terminé)

1.  **Étape 0a (Thèmes)** : `Staging_Category_Queue` remplie.
2.  **Étape 0b (Sous-catégories)** : `Staging_SubCategory_Queue` remplie.
3.  **Étape 1 (Produits)** : `Staging_Product_Queue` remplie (1773 URL).
4.  **Étape 2 (Scraping JSON)** : `Staging_VDB_Product_Page` remplie (1773 JSON bruts).
5.  **Étape 3 (Transformation)** : `Staging_Product_Cleansed` remplie (1773 lignes nettoyées).
6.  **Étape 4 (Chargement DWH)** :
    * `Dim_Product` remplie (1772 produits uniques).
    * `Fact_Marketplace_Snapshot` remplie (1773 snapshots).

### Pipeline 2 : Reddit (En cours)

1.  **Étape 5 (Modélisation)** : Tables `Staging_Reddit_Posts` et `Staging_Reddit_Sentiment` créées.
2.  **Étape 6 (Extraction Staging Reddit)** : En cours. Le script est robuste :
    * Il lit `Dim_Product` et ignore les produits déjà présents dans `Staging_Reddit_Posts` (il peut être relancé).
    * Il utilise un mapping de catégories pour chercher dans les bons subreddits.
    * Il respecte une pause de 7 secondes pour éviter le bannissement de l'API.
3.  **Étape 7 (Transformation Sentiment)** : (Future)
    * Lire `Staging_Reddit_Posts`.
    * Appliquer une analyse de sentiment (VADER/TextBlob).
    * Charger `Staging_Reddit_Sentiment`.
4.  **Étape 8 (Chargement DWH Reddit)** : (Future)
    * Lire `Staging_Reddit_Sentiment`.
    * Gérer la `Dim_Reddit_User` (SCD Type 1 avec hashage).
    * Charger les faits dans `Fact_Public_Sentiment