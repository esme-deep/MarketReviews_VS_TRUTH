import sys
import os
from datetime import datetime
import importlib

# --- 1. Configuration du chemin ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# --- 2. Imports Airflow ---
from airflow.decorators import dag, task

# --- 3. Import du Moteur de Migration ---
try:
    from scripts import reddit_dwh_loader as rdl
except ImportError:
    print("ERREUR : Impossible d'importer 'reddit_dwh_loader' depuis 'scripts/'.")
    raise

# --- 4. Définition des Tâches ---

@task(task_id="migrate_dim_reddit_user")
def task_migrate_dim_user():
    """
    Étape 8a : Migre les auteurs de Staging_Reddit_Posts vers Dim_Reddit_User.
    Utilise une logique SCD Type 1 (UPSERT) basée sur RedditorID.
    """
    print("--- DÉBUT : Tâche 8a - Migration Dim_Reddit_User ---")
    importlib.reload(rdl)
    
    conn_staging, cursor_staging = None, None
    conn_dwh, cursor_dwh = None, None
    
    try:
        # (E) Extraire les auteurs de Staging
        conn_staging = rdl.get_staging_connection()
        cursor_staging = conn_staging.cursor()
        authors_to_migrate = rdl.get_processed_authors_from_staging(cursor_staging)
        
        if not authors_to_migrate:
            print("   -> Fin : Aucun nouvel auteur 'Processed' à migrer.")
            return

        # (L) Lire les utilisateurs existants du DWH pour le lookup
        conn_dwh = rdl.get_dwh_connection()
        cursor_dwh = conn_dwh.cursor()
        dwh_user_map = rdl.get_existing_users_from_dwh(cursor_dwh)
        
        # (T+L) Boucle d'UPSERT
        print(f"   -> Lancement de l'UPSERT pour {len(authors_to_migrate)} auteurs...")
        for author in authors_to_migrate:
            existing_key = dwh_user_map.get(author['RedditorID'])
            rdl.upsert_dim_user(cursor_dwh, author, existing_key)
            
        conn_dwh.commit()
        print("   -> ✅ Migration Dim_Reddit_User terminée avec succès.")

    except Exception as e:
        print(f"   -> ❌ ERREUR (Dim_Reddit_User) : {e}")
        if conn_dwh: conn_dwh.rollback()
        raise
    finally:
        if cursor_staging: cursor_staging.close()
        if conn_staging: conn_staging.close()
        if cursor_dwh: cursor_dwh.close()
        if conn_dwh: conn_dwh.close()

@task(task_id="migrate_fact_public_sentiment")
def task_migrate_fact_sentiment():
    """
    Étape 8b : Migre les scores de Staging_Reddit_Sentiment vers Fact_Public_Sentiment.
    Met à jour le statut dans Staging à 'migrated' pour l'idempotence.
    """
    print("--- DÉBUT : Tâche 8b - Migration Fact_Public_Sentiment ---")
    importlib.reload(rdl)
    
    conn_staging, cursor_staging = None, None
    conn_dwh, cursor_dwh = None, None
    
    try:
        # --- Connexions (les 2 sont en mode transaction) ---
        conn_staging = rdl.get_staging_connection()
        cursor_staging = conn_staging.cursor()
        conn_dwh = rdl.get_dwh_connection()
        cursor_dwh = conn_dwh.cursor()

        # (L) Étape cruciale : On doit relire Dim_Reddit_User pour le mapping
        # On le fait APRÈS la Tâche 1 pour avoir les dernières infos
        dwh_user_map = rdl.get_existing_users_from_dwh(cursor_dwh)
        if not dwh_user_map:
            raise Exception("La table Dim_Reddit_User est vide. La Tâche 1 a-t-elle échoué ?")

        # (E) Extraire les faits de sentiment (en joignant pour avoir le RedditorID)
        sentiments_to_migrate = rdl.get_pending_sentiments(cursor_staging)
        
        if not sentiments_to_migrate:
            print("   -> Fin : Aucun sentiment 'pending_migration' à migrer.")
            return

        # (T+L) Boucle de migration
        print(f"   -> Lancement de la migration pour {len(sentiments_to_migrate)} faits...")
        migrated_count = 0
        failed_count = 0
        
        for sentiment in sentiments_to_migrate:
            sentiment_id = sentiment['SentimentID']
            redditor_id = sentiment['RedditorID']
            
            # (T) Trouver les clés étrangères
            user_key = dwh_user_map.get(redditor_id)
            date_key = int(sentiment['PostDate'].strftime('%Y%m%d'))
            
            if user_key is None:
                # L'auteur du post n'existe pas dans la dim (ex: [deleted])
                print(f"   -> ⚠️ Fait ignoré (ID: {sentiment_id}) : Auteur {redditor_id} non trouvé dans Dim_Reddit_User.")
                rdl.update_sentiment_status(cursor_staging, sentiment_id, 'failed_migration (no user)')
                failed_count += 1
                continue

            # (L) Insérer le Fait dans le DWH
            rdl.insert_fact_sentiment(cursor_dwh, sentiment, user_key, date_key)
            
            # (L) Mettre à jour le statut Staging (Idempotence)
            rdl.update_sentiment_status(cursor_staging, sentiment_id, 'migrated')
            migrated_count += 1

        # Commit des deux BDD en même temps
        print(f"   -> Commit DWH ({migrated_count} faits insérés)...")
        conn_dwh.commit()
        print(f"   -> Commit Staging ({migrated_count} statuts mis à 'migrated')...")
        conn_staging.commit()
        
        if failed_count > 0:
            print(f"   -> ⚠️ {failed_count} faits ont été marqués comme 'failed_migration'.")

    except Exception as e:
        print(f"   -> ❌ ERREUR (Fact_Public_Sentiment) : {e}")
        if conn_dwh: conn_dwh.rollback()
        if conn_staging: conn_staging.rollback()
        raise
    finally:
        if cursor_staging: cursor_staging.close()
        if conn_staging: conn_staging.close()
        if cursor_dwh: cursor_dwh.close()
        if conn_dwh: conn_dwh.close()

# --- 5. Définition du DAG ---

@dag(
    dag_id="reddit_staging_to_dwh_migration",
    start_date=datetime(2025, 10, 29),
    schedule=None, # Manuel pour l'instant
    catchup=False,
    tags=['reddit_posts_sentiment_author_migration_to _DWH']
)
def reddit_to_dwh_dag():
    """
    Orchestre la migration finale (Étape 8) des données Reddit
    de Staging vers le DWH (Dim_Reddit_User et Fact_Public_Sentiment).
    """
    
    # On définit les tâches
    task_a_users = task_migrate_dim_user()
    task_b_facts = task_migrate_fact_sentiment()

    # On définit la dépendance : Les faits DÉPENDENT des utilisateurs
    task_a_users >> task_b_facts

# --- 6. Instanciation du DAG ---
reddit_to_dwh_dag()