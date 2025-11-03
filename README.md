# Projet : MarketReviews VS TRUTH (Analyse de Sentiment)

Ce projet est un pipeline de Data Engineering complet conçu pour extraire, transformer et charger (ELT) des données de deux sources distinctes afin d'analyser la corrélation entre les avis d'un site e-commerce (Vanden Borre) et le sentiment du public sur un forum (Reddit).

L'objectif final est de répondre à la question : **"Est-ce que les notes d'un produit sur le site de vente reflètent ce que le public en pense vraiment ?"**

Le projet est entièrement orchestré par **Apache Airflow** et s'exécute dans un environnement conteneurisé **Docker**.

---

## 1. Architecture et Technologies

* **Orchestration :** Apache Airflow
* **Conteneurisation :** Docker, Docker Compose
* **Base de Données (Staging) :** `Projet_Market_Staging` (MS SQL Server)
* **Base de Données (DWH) :** `Projet_Market_DWH` (MS SQL Server)
* **Connecteur BDD Python :** `pyodbc`
* **Scraping Web (VDB) :** `requests`, `BeautifulSoup4`
* **API Reddit (Extraction) :** `requests` (pour la recherche `.json` publique)
* **API Reddit (Enrichissement) :** `praw` (pour les détails des utilisateurs via l'API authentifiée)
* **Analyse de Sentiment (ML) :** `transformers` (Hugging Face), `torch`, `scipy`
    * **Modèle :** `cardiffnlp/twitter-roberta-base-sentiment`

---

## 2. Architecture des Données

Le projet utilise une architecture ELT avec une base de Staging (zone de travail) et un Data Warehouse (source de vérité pour l'analyse).

### 2.1. Base de Staging (`Projet_Market_Staging`)

Cette base sert de file d'attente (queue) et de zone de transformation pour tous les pipelines.

* **Tables VDB :**
    * `Staging_Category_Queue`, `Staging_SubCategory_Queue`, `Staging_Product_Queue`: Gèrent la file d'attente du scraping VDB.
    * `Staging_VDB_Product_Page`: Stocke le JSON-LD brut de chaque page produit.
    * `Staging_Product_Cleansed`: Stocke les données VDB parsées (prix, note, SKU...), prêtes pour le DWH.

* **Tables Reddit :**
    * `Staging_Reddit_Posts`: Table centrale. Stocke les posts bruts (texte, auteur, etc.) ET les données d'enrichissement (Karma, RedditorID, statut de vérification...).
    * `Staging_Reddit_Sentiment`: Table de résultats. Stocke uniquement les scores de sentiment calculés par RoBERTa, avec le statut `pending_migration`.

### 2.2. Data Warehouse (`Projet_Market_DWH`)

Le DWH est modélisé en **schéma en constellation**. Les deux tables de faits (`Fact_Marketplace_Snapshot` et `Fact_Public_Sentiment`) partagent des dimensions communes (`Dim_Product`, `Dim_Date`).

* **Dimensions :**
    * `Dim_Product` : Dimension conformée centrale (SKU, Nom, Marque).
    * `Dim_Date` : Dimension de temps (Jour, Mois, Année).
    * `Dim_Time` : Dimension de temps (Heure, Minute).
    * **`Dim_Reddit_User` (Nouveau)** : Stocke les auteurs Reddit uniques. Gérée en **SCD Type 1** basée sur `RedditorID` (la clé naturelle).

* **Tables de Faits :**
    * `Fact_Marketplace_Snapshot` : Stocke les métriques de VDB (Prix, Note VDB, Nb Avis).
    * **`Fact_Public_Sentiment` (Nouveau)** : Stocke chaque score de sentiment Reddit, lié à un produit et un utilisateur.


## 3. Description des Pipelines (DAGs)

Le projet est composé de deux pipelines principaux qui tournent en parallèle.

### Pipeline 1 : Vanden Borre (Market Reviews)

Ce pipeline charge le côté "Market" du DWH.

* **1. `dag_vdb_staging.py` (Étapes 0-3)**
    * **Moteur :** `vdb_pipeline_engine.py`
    * **Déclenchement :** Manuel (avec paramètres `hub_url` et `hub_name`).
    * **Logique :** Exécute la chaîne de scraping complète :
        1.  `task_0a_discover_themes` : Trouve les catégories (ex: "TV").
        2.  `task_0b_discover_subcategories` : Trouve les sous-catégories (ex: "TV OLED").
        3.  `task_1_discover_products` : Trouve toutes les URL de produits.
        4.  `task_2_scrape_products` : Visite chaque URL et extrait le JSON-LD.
        5.  `task_3_transform_json` : Nettoie le JSON et le charge dans `Staging_Product_Cleansed`.

* **2. `dag_staging_to_dwh.py` (Étape 4)**
    * **Moteur :** `etl_dwh_loader.py`
    * **Déclenchement :** Manuel.
    * **Logique :** Lit `Staging_Product_Cleansed` (statut `pending`) et migre les données vers le DWH en gérant les dimensions et les faits (SCD Type 1 pour `Dim_Product`).

### Pipeline 2 : Reddit (Public TRUTH)

Ce pipeline charge le côté "Truth" du DWH en quatre étapes.

* **1. `dag_reddit_pipeline.py` (Étape 6 - Extraction)**
    * **Moteur :** `reddit_engine.py`
    * **Déclenchement :** Manuel ou planifié (`@daily`).
    * **Logique :**
        1.  Lit `Dim_Product` du DWH pour trouver les produits à rechercher.
        2.  Utilise `requests` (API `.json` publique) pour rechercher des posts sur Reddit.
        3.  Applique un filtre de pertinence (basé sur la marque) pour ignorer les posts hors sujet.
        4.  Charge les posts trouvés dans `Staging_Reddit_Posts` avec le statut `pending_sentiment`.

* **2. `dag_sentiment_analysis_roberta.py` (Étape 7 - Sentiment)**
    * **Moteur :** `sentiment_engine_roberta.py`
    * **Déclenchement :** Manuel.
    * **Logique :**
        1.  Charge le modèle RoBERTa (`cardiffnlp`) en mémoire.
        2.  Lit **tous** les posts de `Staging_Reddit_Posts` avec statut `pending_sentiment`.
        3.  **Gestion des limites :** Si un post > 512 tokens, il est marqué `skipped_too_long` et n'est pas analysé.
        4.  Calcule le score de sentiment pour les autres.
        5.  Charge le résultat dans `Staging_Reddit_Sentiment` (statut `pending_migration`) et met le post source à `processed`.

* **3. `dag_update_staging_user_dates.py` (Étape 7.5 - Enrichissement)**
    * **Moteur :** Logique intégrée au DAG.
    * **Déclenchement :** Manuel.
    * **Logique :**
        1.  Lit les auteurs de `Staging_Reddit_Posts` où `AuthorCheckStatus IS NULL`.
        2.  Utilise **PRAW** (API authentifiée) pour récupérer les détails de l'auteur (Karma, `RedditorID`, date de création, e-mail vérifié).
        3.  Met à jour en masse (`UPDATE`) les lignes correspondantes dans `Staging_Reddit_Posts` avec ces nouvelles informations.
        4.  **Commits par lots (batch)** de 50 pour gérer les timeouts et la charge.

* **4. `dag_reddit_staging_to_dwh.py` (Étape 8 - Migration Finale)**
    * **Moteur :** `reddit_dwh_loader.py`
    * **Déclenchement :** Manuel.
    * **Logique (2 tâches) :**
        1.  **`migrate_dim_reddit_user` :** Lit `Staging_Reddit_Posts` (statut `Processed`), trouve les auteurs uniques et fait un **UPSERT (SCD Type 1)** dans `Dim_Reddit_User` en se basant sur le `RedditorID`.
        2.  **`migrate_fact_public_sentiment` :** (Dépend de la Tâche 1) Lit `Staging_Reddit_Sentiment` (statut `pending_migration`), fait la jointure avec `Dim_Reddit_User` (via `RedditorID`) et `Dim_Date` pour insérer les faits dans `Fact_Public_Sentiment`.

