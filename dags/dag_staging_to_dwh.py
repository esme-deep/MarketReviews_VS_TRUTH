import sys
import os
from datetime import datetime
import importlib

# --- 1. Configuration du chemin ---
# Ajoute le dossier racine (MARKETREVIEWS_VS_TRUTH) au chemin
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# --- 2. Imports Airflow ---
from airflow.decorators import dag, task

# --- 3. Import de votre MOTEUR (version 'scripts') ---
try:
    # Importe le moteur de chargement depuis /scripts
    from scripts import etl_dwh_loader as etl_loader
except ImportError:
    print("ERREUR : Impossible d'importer 'etl_dwh_loader' depuis 'scripts/'.")
    print("Veuillez vérifier que le fichier existe et que la connexion est configurée pour Docker.")
    raise

# --- 4. Définition de la Tâche de Migration ---

@task(task_id="run_dwh_migration")
def task_run_dwh_migration():
    """
    Tâche monolithique qui exécute l'Étape 4 (Migration Staging -> DWH).
    Elle reproduit la logique transactionnelle du notebook 'run_dwh_migration.ipynb'.
    """
    print("--- DÉBUT : Pipeline Staging -> DWH (Étape 4) ---")
    importlib.reload(etl_loader) # Force le rechargement du moteur
    
    conn_staging_read, cursor_staging_read = None, None
    tasks = []

    # --- 1. Extraire la liste des tâches (comme la cellule 2 du notebook) ---
    try:
        conn_staging_read = etl_loader.get_staging_connection()
        cursor_staging_read = conn_staging_read.cursor()
        
        # (E) Extraire les tâches
        tasks = etl_loader.get_pending_cleansed_data(cursor_staging_read)
        
    except Exception as e_init:
        print(f"--- ❌ ERREUR MAJEURE lors de la lecture de la file d'attente : {e_init} ---")
        raise # Si on ne peut pas lire la file, la tâche doit échouer
    finally:
        if cursor_staging_read: cursor_staging_read.close()
        if conn_staging_read: conn_staging_read.close()
    
    if not tasks:
        print("\n--- ✅ Fin : Rien à charger dans le DWH. Tâche terminée. ---")
        return
        
    print(f"\n--- {len(tasks)} produits à charger trouvés. Lancement de la boucle... ---")

    # --- 2. Boucle de Transformation (T) et Chargement (L) ---
    # On traite chaque produit comme sa propre transaction
    for task in tasks:
        cleansed_id = task['CleansedProductID']
        sku = task['SKU']
        
        conn_dwh, cursor_dwh = None, None
        conn_staging_write, cursor_staging_write = None, None
        
        try:
            print(f"\n--- Traitement du SKU {sku} (CleansedID: {cleansed_id}) ---")
            
            # --- Transaction DWH ---
            conn_dwh = etl_loader.get_dwh_connection()
            cursor_dwh = conn_dwh.cursor()
            
            # 3a. Gérer la Dimension (SCD Type 1)
            product_key = etl_loader.load_dim_product_scd1(cursor_dwh, task)
            
            # 3b. Gérer les Faits
            date_key, time_key = etl_loader.get_date_time_keys(task['ScrapeTimestamp'])
            etl_loader.load_fact_snapshot(cursor_dwh, task, product_key, date_key, time_key)
            
            conn_dwh.commit() # Si tout s'est bien passé dans le DWH, on valide
            print(f"   -> ✅ SKU {sku} chargé dans le DWH.")

            # --- Transaction Staging (si DWH OK) ---
            conn_staging_write = etl_loader.get_staging_connection()
            cursor_staging_write = conn_staging_write.cursor()
            etl_loader.update_staging_status(cursor_staging_write, cleansed_id, 'processed')
            conn_staging_write.commit() # Valide le statut 'processed'

        except Exception as e:
            # Gérer une erreur pour ce produit spécifique
            print(f"   -> ❌ ERREUR sur SKU {sku}: {e}.")
            if conn_dwh: conn_dwh.rollback()     # Annuler les changements dans le DWH
            if conn_staging_write: conn_staging_write.rollback()
            
            # Marquer comme 'failed' (dans une transaction séparée)
            conn_fail, cursor_fail = None, None
            try:
                conn_fail = etl_loader.get_staging_connection()
                cursor_fail = conn_fail.cursor()
                etl_loader.update_staging_status(cursor_fail, cleansed_id, 'failed')
                conn_fail.commit()
            finally:
                if cursor_fail: cursor_fail.close()
                if conn_fail: conn_fail.close()
            # On ne 'raise' pas pour que la boucle continue

        finally:
            if cursor_staging_write: cursor_staging_write.close()
            if conn_staging_write: conn_staging_write.close()
            if cursor_dwh: cursor_dwh.close()
            if conn_dwh: conn_dwh.close()

    print("\n--- ✅ Boucle de migration DWH terminée. ---")

# --- 5. Définition du DAG ---
@dag(
    dag_id="vdb_staging_to_dwh_migration",
    start_date=datetime(2025, 10, 29),
    schedule=None, # Se déclenche uniquement manuellement
    catchup=False,
    tags=['vandenborre_migration_DWH']
)
def staging_to_dwh_dag():
    """
    Orchestre la migration des données de Staging (Cleansed) 
    vers le DWH (Dim_Product & Fact_Marketplace_Snapshot).
    Reproduit 'run_dwh_migration.ipynb'.
    """
    
    # Appelle la tâche unique
    task_run_dwh_migration()

# --- 6. Instanciation du DAG ---
staging_to_dwh_dag()