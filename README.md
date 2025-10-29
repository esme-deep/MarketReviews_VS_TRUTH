# Projet : MarketReviews VS TRUTH

Ce projet est un pipeline de Data Engineering complet conçu pour extraire, transformer et charger (ETL) des données de produits afin d'analyser la corrélation entre les avis d'un site e-commerce (Vanden Borre) et le sentiment du public sur un forum (Reddit).

L'objectif final est de répondre à la question : "Est-ce que les notes d'un produit sur le site de vente reflètent ce que le public en pense vraiment ?"

* **Source 1 (Market Reviews) :** Vanden Borre (Scraping Web)
* **Source 2 (Public TRUTH) :** API Reddit (r/headphones) (à corriger)
* **Staging :** Base de données `Projet_Market_Staging` (SQL Server)
* **Data Warehouse (DWH) :** Base de données `Projet_Market_DWH` (SQL Server)

---

## Architecture des Données

Ce projet utilise deux bases de données distinctes pour séparer les données brutes/intermédiaires (Staging) des données finales prêtes pour l'analyse (DWH).

### 1. Base de Staging (`Projet_Market_Staging`)

Cette base sert de zone de "travail". Elle contient toutes les files d'attente (queues) et les données brutes.

* **Tables Pipeline VDB :**
    * `Staging_Category_Queue` : Stocke les "thèmes" (ex: "Télévision").
    * `Staging_SubCategory_Queue` : Stocke les sous-catégories (ex: "TV OLED (42)").
    * `Staging_Product_Queue` : Stocke les 1773 URL de produits à scraper.
    * `Staging_VDB_Product_Page` : Stocke le JSON-LD brut de chaque page produit.
    * `Staging_Product_Cleansed` : Stocke le JSON parsé en colonnes (Prix, Note, SKU, etc.).

* **Tables Pipeline Reddit :**
    * `Staging_Reddit_Posts` : Stocke les posts bruts (titre, texte, auteur, date) avec des colonnes de traçabilité (`ProductKey_Ref`, `Search_Keyword_Used`) pour garantir la pertinence.
    * `Staging_Reddit_Sentiment` : Stocke le résultat de l'analyse de sentiment (le score) pour chaque post.

### 2. Data Warehouse (`Projet_Market_DWH`)

C'est la source finale pour l'analyse (Power BI). Il utilise un **schéma en constellation** où deux tables de faits partagent des dimensions communes.

* **Dimensions :**
    * `Dim_Product` : Le "pont" central. Contient la liste de tous les produits uniques (1772 produits). Gérée en **SCD Type 1** (les modifications écrasent les anciennes données).
    * `Dim_Date` : Table pré-remplie (2024-2026).
    * `Dim_Time` : Table pré-remplie (1440 minutes).
    * `Dim_Reddit_User` (Future) : Stockera les auteurs Reddit (probablement hachés/anonymisés).

* **Tables de Faits :**
    * `Fact_Marketplace_Snapshot` : Stocke les "photos" des métriques de Vanden Borre (Prix, Note, Nb Avis). Les colonnes de rating **acceptent les `NULL`** pour les produits sans avis.
    * `Fact_Public_Sentiment` (Future) : Stockera les scores de sentiment de Reddit.

![Schéma du DWH](https://i.imgur.com/v8tA7mP.png)

---

## Structure du Code (Python)

Le projet est divisé en "Moteurs" (`.py`) et "Pilotes" (`.ipynb`) pour faciliter l'automatisation et la maintenance.

### Fichiers Moteur (Logique)

* `vdb_pipeline_engine.py` : Contient toutes les fonctions de scraping (Requests/BeautifulSoup) et de chargement pour le pipeline Vanden Borre (Étapes 0a, 0b, 1, 2).
* `etl_transformer.py` : Contient la logique de transformation (Étape 3) pour parser le JSON brut de VDB et le charger dans `Staging_Product_Cleansed`.
* `etl_dwh_loader.py` : Contient la logique ETL (Étape 4) pour migrer les données de `Staging_Product_Cleansed` vers le DWH (`Dim_Product` et `Fact_Marketplace_Snapshot`).
* `reddit_pipeline_engine.py` : Contient la logique (Étape 6) pour lire `Dim_Product`, générer des mots-clés de recherche propres, appeler l'API Reddit et charger les résultats bruts dans `Staging_Reddit_Posts`.

### Fichiers Pilote (Exécution)

* `run_staging_pipeline.ipynb` : Notebook pour exécuter les étapes 0 à 3 (le pipeline VDB complet, du scraping à la transformation).
* `run_dwh_migration.ipynb` : Notebook pour exécuter l'étape 4 (le chargement Staging -> DWH).
* `run_reddit_pipeline.ipynb` : Notebook pour exécuter l'étape 6 (le pipeline Reddit).

---

## Logique de Pipeline (Étapes)

### Pipeline 1 : Vanden Borre (Terminé)

1.  **Étape 0a (Découverte Thèmes) :** Scrape la page "univers" (ex: `tv-audio`) pour trouver les thèmes (ex: "Télévision") -> `Staging_Category_Queue`.
2.  **Étape 0b (Découverte Sous-catégories) :** Scrape chaque page "thème" (ex: `.../television`) pour trouver les sous-catégories (ex: "TV OLED (42)") -> `Staging_SubCategory_Queue`.
3.  **Étape 1 (Découverte Produits) :** Scrape chaque page de sous-catégorie (en gérant la pagination) pour trouver toutes les URL de produits (1773 produits) -> `Staging_Product_Queue`.
4.  **Étape 2 (Scraping JSON) :** Visite chaque URL de produit, extrait le JSON-LD caché (contenant prix, note, etc.) -> `Staging_VDB_Product_Page`.
5.  **Étape 3 (Transformation Staging) :** Parse le JSON brut de `Staging_VDB_Product_Page` en colonnes (Prix, Note, SKU, etc.) -> `Staging_Product_Cleansed`.
6.  **Étape 4 (Chargement DWH) :** Migre les données de `Staging_Product_Cleansed` vers le DWH `Projet_Market_DWH` en utilisant la logique SCD Type 1 pour `Dim_Product` et en insérant dans `Fact_Marketplace_Snapshot`.

### Pipeline 2 : Reddit (En cours)

1.  **Étape 5 (Modélisation) :** Les tables `Staging_Reddit_Posts` et `Staging_Reddit_Sentiment` sont créées.
2.  **Étape 6 (Extraction Staging Reddit) :**
    * Lit `Dim_Product` depuis le DWH.
    * Génère un mot-clé de recherche propre (ex: "JVC LED HD LT-32FV140 32 POUCES (2023)" -> "JVC HD LT-32FV140 32").
    * Appelle l'API Reddit (avec authentification OAuth et une pause de 3 secondes) pour ce mot-clé.
    * Sauvegarde les résultats (Titre, Texte, Auteur, Date) dans `Staging_Reddit_Posts`.
3.  **Étape 7 (Transformation Sentiment) (Future) :**
    * Lire les posts "pending_sentiment" de `Staging_Reddit_Posts`.
    * Appliquer une analyse de sentiment (VADER/TextBlob) sur le texte.
    * Charger le score dans `Staging_Reddit_Sentiment`.
4.  **Étape 8 (Chargement DWH Reddit) (Future) :**
    * Lire `Staging_Reddit_Sentiment`.
    * Gérer la `Dim_Reddit_User` (SCD Type 1 avec hashage).
    * Charger les faits dans `Fact_Public_Sentiment`.