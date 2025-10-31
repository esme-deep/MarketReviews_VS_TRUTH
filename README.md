# Projet : MarketReviews VS TRUTH (Analyse de Sentiment)

Ce projet est un pipeline de Data Engineering complet conçu pour extraire, transformer et charger (ETL/ELT) des données de deux sources distinctes afin d'analyser la corrélation entre les avis d'un site e-commerce (Vanden Borre) et le sentiment du public sur un forum (Reddit).

L'objectif final est de répondre à la question : **"Est-ce que les notes d'un produit sur le site de vente reflètent ce que le public en pense vraiment ?"**

---

## 1. Architecture et Technologies

Ce projet est orchestré par **Apache Airflow** et s'exécute dans un environnement conteneurisé **Docker**.

* **Orchestration :** Apache Airflow
* **Conteneurisation :** Docker, Docker Compose
* **Source 1 (Market Reviews) :** Vanden Borre (Scraping Web via **Requests/BeautifulSoup**)
* **Source 2 (Public TRUTH) :** API Reddit (via PRAW)
* **Analyse de Sentiment (ML) :** `transformers` (Hugging Face), `torch`, `scipy`
    * **Modèle :** `cardiffnlp/twitter-roberta-base-sentiment`
* **Bases de données :** Microsoft SQL Server
    * `Projet_Market_Staging` (Zone de transit / Staging)
    * `Projet_Market_DWH` (Data Warehouse)
* **Connecteur BDD Python :** `pyodbc`

---

## 2. Architecture des Données

Le projet utilise deux bases de données pour séparer les données brutes/intermédiaires (Staging) des données finales prêtes pour l'analyse (DWH).

### 2.1. Base de Staging (`Projet_Market_Staging`)

Cette base sert de zone de "travail" et de file d'attente (queue) pour les différents pipelines.

* **Tables Pipeline VDB :**
    * `Staging_Category_Queue` : Stocke les "thèmes" (ex: "Télévision").
    * `Staging_SubCategory_Queue` : Stocke les sous-catégories (ex: "TV OLED (42)").
    * `Staging_Product_Queue` : Stocke les URL des produits à scraper.
    * `Staging_VDB_Product_Page` : Stocke le JSON-LD brut de chaque page produit.
    * `Staging_Product_Cleansed` : Stocke le JSON parsé en colonnes (Prix, Note, SKU, etc.).

* **Tables Pipeline Reddit :**
    * `Staging_Reddit_Posts` : Stocke les posts bruts (titre, texte, auteur, date) avec traçabilité (`ProductKey_Ref`). Possède une colonne `Status` cruciale pour l'orchestration (`pending_sentiment`, `processed`, `failed`, `skipped_too_long`).
    * `Staging_Reddit_Sentiment` : Stocke le résultat de l'analyse (le score) pour chaque post, avec le statut `pending_migration`.

### 2.2. Data Warehouse (`Projet_Market_DWH`)

C'est la source finale pour l'analyse (Power BI). Il utilise un **schéma en constellation** : deux tables de faits partagent des dimensions communes.

* **Dimensions :**
    * `Dim_Product` : Le "pont" central (dimension conformée). Contient la liste des produits uniques. Gérée en **SCD Type 1**.
    * `Dim_Date` : Table pré-remplie (2024-2026).
    * `Dim_Time` : Table pré-remplie (1440 minutes).
    * `Dim_Reddit_User` (Future) : Stockera les auteurs Reddit (probablement hachés/anonymisés).

* **Tables de Faits :**
    * `Fact_Marketplace_Snapshot` : Stocke les "photos" des métriques de Vanden Borre (Prix, Note, Nb Avis). Les colonnes de rating **acceptent les `NULL`**.
    * `Fact_Public_Sentiment` (Future) : Stockera les scores de sentiment de Reddit agrégés.

---

## 3. Structure du Projet (Airflow)

Le projet a migré d'une exécution par notebooks (`.ipynb`) à une orchestration complète via **Apache Airflow**. Les anciens "moteurs" sont maintenant des modules Python appelés par les DAGs.
/ ├── dags/ │ ├── dag_sentiment_analysis_roberta.py # DAG principal (Étape 7) │ └── ... (futurs dags pour les étapes 0-6) ├── scripts/ │ ├── sentiment_engine_roberta.py # Logique métier, connexion DB, modèle ML (pour Étape 7) │ ├── vdb_pipeline_engine.py # Logique de scraping VDB (pour Étapes 0-3) │ ├── etl_transformer.py # Logique de transformation JSON (pour Étape 3) │ ├── etl_dwh_loader.py # Logique de chargement DWH (pour Étape 4) │ └── reddit_pipeline_engine.py # Logique d'extraction Reddit (pour Étape 6) ├── plugins/ ├── docker-compose.yml # Lance Airflow, SQL Server, etc. ├── Dockerfile # Construit l'image Airflow (incluant torch, transformers, pyodbc) ├── requirements.txt └── README.md (Ce fichier)

---

## 4. État Actuel du Pipeline

### Pipeline 1 : Vanden Borre (Terminé)

* **Statut :** Terminé (exécuté via scripts initiaux, non encore migré vers Airflow).
* **Étape 0a (Thèmes)** : `Staging_Category_Queue` remplie.
* **Étape 0b (Sous-catégories)** : `Staging_SubCategory_Queue` remplie.
* **Étape 1 (Produits)** : `Staging_Product_Queue` remplie.
* **Étape 2 (Scraping JSON)** : `Staging_VDB_Product_Page` remplie.
* **Étape 3 (Transformation)** : `Staging_Product_Cleansed` remplie.
* **Étape 4 (Chargement DWH)** :
    * `Dim_Product` remplie.
    * `Fact_Marketplace_Snapshot` remplie.

### Pipeline 2 : Reddit (Analyse en cours)

* **Étape 5 (Modélisation)** : Tables `Staging_Reddit_Posts` et `Staging_Reddit_Sentiment` créées.
* **Étape 6 (Extraction Staging Reddit)** :
    * **Statut :** Implémenté (script `reddit_pipeline_engine.py`).
    * **Logique :** Le script lit `Dim_Product` (DWH), génère des mots-clés, interroge l'API Reddit via un mapping de subreddits, et charge les résultats dans `Staging_Reddit_Posts` avec le statut `pending_sentiment`.
* **Étape 7 (Transformation Sentiment)** :
    * **Statut :** **ACTIF / IMPLÉMENTÉ (DAG Airflow)**
    * **DAG :** `sentiment_analysis_roberta_pipeline`
    * **Tâche :** `run_sentiment_analysis_roberta`
    * **Logique (E - Extraction) :**
        * Charge le modèle RoBERTa (`cardiffnlp`) en mémoire une seule fois.
        * Récupère les posts de `Staging_Reddit_Posts` (statut `pending_sentiment`).
        * **FILTRE MÉTIER CLÉ :** Ne sélectionne que les posts liés à un produit du DWH ayant **3 avis ou plus** (`Fact_Marketplace_Snapshot.Review_Count >= 3`).
    * **Logique (T - Transformation) :**
        * Concatène `PostTitle` et `PostText`.
        * **GESTION DES LIMITES :** Vérifie la longueur du texte. Si **> 512 tokens**, le post est ignoré (ne sera pas analysé).
        * Si valide, le modèle RoBERTa génère un score (de -1.0 à +1.0).
    * **Logique (L - Chargement) :**
        * **Succès :** Le score est inséré dans `Staging_Reddit_Sentiment` (statut `pending_migration`) ET le post source `Staging_Reddit_Posts` est mis à jour à `processed`.
        * **Ignoré (Trop long) :** Le post source est mis à jour à `skipped_too_long`.
        * **Échec :** Le post source est mis à jour à `failed`.
* **Étape 8 (Chargement DWH Reddit)** :
    * **Statut :** (Future)
    * **Logique :** Un futur DAG lira `Staging_Reddit_Sentiment` (statut `pending_migration`), gérera la `Dim_Reddit_User`, et chargera les faits agrégés dans `Fact_Public_Sentiment`.