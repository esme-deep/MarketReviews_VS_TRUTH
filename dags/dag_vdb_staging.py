import sys
import os
from datetime import datetime
import importlib
import time

# --- 1. Configuration du chemin ---
# Ajoute le dossier racine (MARKETREVIEWS_VS_TRUTH) au chemin
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# --- 2. Imports Airflow ---
from airflow.decorators import dag, task
from airflow.models.param import Param

# --- 3. Import de votre MOTEUR (version 'scripts') ---
# Cet import ne fonctionnera que si le chemin (ci-dessus) est correct
try:
    from scripts import vdb_pipeline_engine as vpe
except ImportError:
    print("ERREUR : Impossible d'importer 'vdb_pipeline_engine' depuis le dossier 'scripts/'.")
    print(f"Chemin de recherche (sys.path) : {sys.path}")
    # Si le DAG plante ici, c'est un problème de chemin
    raise

# --- 4. Fonction Utilitaire (Gestion des erreurs) ---
# Nous avons besoin de cette fonction pour gérer les échecs dans les boucles
def _update_queue_status_on_fail(queue_table, queue_id_column, queue_id):
    """
    Fonction utilitaire pour marquer une tâche comme 'failed' dans 
    n'importe quelle table de file d'attente.
    Crée sa propre connexion pour garantir le commit même en cas d'échec.
    """
    conn = None
    cursor = None
    try:
        importlib.reload(vpe) # Assure la bonne connexion
        conn = vpe.get_staging_connection() 
        cursor = conn.cursor()
        
        valid_tables = {
            "Staging_Category_Queue": "CategoryQueueID",
            "Staging_SubCategory_Queue": "SubCategoryQueueID",
            "Staging_Product_Queue": "ProductQueueID",
            "Staging_VDB_Product_Page": "StagingVDBKey"
        }
        
        if queue_table not in valid_tables or valid_tables[queue_table] != queue_id_column:
            raise ValueError("Nom de table ou de colonne non valide.")
            
        sql = f"UPDATE {queue_table} SET Status = 'failed', LastAttempt = GETDATE() WHERE {queue_id_column} = ?"
        
        cursor.execute(sql, (queue_id,))
        conn.commit()
        print(f"   -> Statut de {queue_table} (ID: {queue_id}) mis à 'failed'.")
    except Exception as e:
        print(f"   -> ERREUR CRITIQUE (update_queue_status_on_fail) : {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# --- 5. Définition des Tâches (basées sur votre notebook) ---

@task(task_id="Etape_0a_Discover_Themes")
def task_0a_discover_themes(**kwargs):
    """
    Étape 0a : Découvre les thèmes à partir d'une URL "hub".
    Prend l'URL et le Nom en paramètre du DAG.
    """
    importlib.reload(vpe) 
    params = kwargs['params']
    URL_UNIVERS = params['hub_url']
    NOM_UNIVERS = params['hub_name']
    
    print(f"--- DÉBUT : Étape 0a (Découverte Thèmes) ---")
    print(f"Paramètres reçus -> URL: {URL_UNIVERS}, Nom: {NOM_UNIVERS}")
    
    conn, cursor = None, None
    try:
        categories_data = vpe.discover_all_categories(URL_UNIVERS)
        if categories_data:
            conn = vpe.get_staging_connection()
            cursor = conn.cursor()
            vpe.save_categories_to_staging(cursor, categories_data, univers_name=NOM_UNIVERS)
            conn.commit()
            print("   ->  Étape 0a terminée avec succès.")
        else:
            print("   ->  Étape 0a : Aucun thème trouvé.")
    except Exception as e:
        print(f"   ->  ERREUR (Étape 0a) : {e}")
        if conn: conn.rollback()
        raise
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@task(task_id="Etape_0b_Discover_SubCategories")
def task_0b_discover_subcategories():
    """
    Étape 0b : Lit Staging_Category_Queue pour trouver
    les sous-catégories de tous les thèmes 'pending'.
    """
    importlib.reload(vpe) 
    print("\n--- DÉBUT : Étape 0b (Découverte Sous-Catégories) ---")
    
    conn_init, cursor_init = vpe.get_staging_connection(), None
    try:
        cursor_init = conn_init.cursor()
        cursor_init.execute("SELECT CategoryQueueID, CategoryName, CategoryURL FROM Staging_Category_Queue WHERE Status = 'pending' ORDER BY CategoryQueueID")
        tasks_to_process = list(cursor_init.fetchall())
    finally:
        if cursor_init: cursor_init.close()
        if conn_init: conn_init.close()

    if not tasks_to_process:
        print("   ->  Fin : Aucun thème 'pending' à traiter.")
        return
        
    print(f"   -> {len(tasks_to_process)} thèmes à traiter trouvés. Lancement boucle...")
    
    for task_tuple in tasks_to_process:
        task_id, task_name, task_url = task_tuple
        loop_conn, loop_cursor = None, None
        try:
            print(f"\n--- Traitement Thème {task_id} : '{task_name}' ---")
            subcategories_data = vpe.discover_all_subcategories(parent_category_name=task_name, hub_url=task_url)
            loop_conn = vpe.get_staging_connection()
            loop_cursor = loop_conn.cursor()

            if subcategories_data:
                vpe.save_subcategories_to_staging(loop_cursor, subcategories_data, parent_category_id=task_id)
                loop_cursor.execute("UPDATE Staging_Category_Queue SET Status = 'processed', LastAttempt = GETDATE() WHERE CategoryQueueID = ?", (task_id))
            else:
                loop_cursor.execute("UPDATE Staging_Category_Queue SET Status = 'processed', LastAttempt = GETDATE() WHERE CategoryQueueID = ?", (task_id))
            
            loop_conn.commit()
        except Exception as e:
            print(f"   ->  ERREUR (Tâche 0b - {task_id}) : {e}")
            if loop_conn: loop_conn.rollback()
            _update_queue_status_on_fail("Staging_Category_Queue", "CategoryQueueID", task_id)
        finally:
            if loop_cursor: loop_cursor.close()
            if loop_conn: loop_conn.close()
            print("   -> Pause de 3 secondes...")
            time.sleep(3)

@task(task_id="Etape_1_Discover_Products")
def task_1_discover_products():
    """
    Étape 1 : Lit Staging_SubCategory_Queue pour trouver
    les produits de toutes les sous-catégories 'pending'.
    """
    importlib.reload(vpe) 
    print("\n--- DÉBUT : Étape 1 (Découverte Produits) ---")
    
    conn_init, cursor_init = vpe.get_staging_connection(), None
    try:
        cursor_init = conn_init.cursor()
        cursor_init.execute("SELECT SubCategoryQueueID, SubCategoryName, SubCategoryURL, ItemCount FROM Staging_SubCategory_Queue WHERE Status = 'pending' AND ItemCount > 0 ORDER BY SubCategoryQueueID")
        tasks_to_process = list(cursor_init.fetchall())
    finally:
        if cursor_init: cursor_init.close()
        if conn_init: conn_init.close()

    if not tasks_to_process:
        print("   -> Fin : Aucune sous-catégorie 'pending' à traiter.")
        return
        
    print(f"   -> {len(tasks_to_process)} sous-catégories à traiter. Lancement boucle...")
    
    for task_tuple in tasks_to_process:
        task_id, task_name, task_url, task_item_count = task_tuple
        loop_conn, loop_cursor = None, None
        try:
            print(f"\n--- Traitement Sous-Catégorie {task_id} : '{task_name}' ({task_item_count} articles) ---")
            product_data = vpe.discover_all_products(task_name, task_url, task_item_count)
            loop_conn = vpe.get_staging_connection()
            loop_cursor = loop_conn.cursor()

            if product_data:
                vpe.save_products_to_staging(loop_cursor, product_data, sub_category_id=task_id)
                loop_cursor.execute("UPDATE Staging_SubCategory_Queue SET Status = 'processed', LastAttempt = GETDATE() WHERE SubCategoryQueueID = ?", (task_id))
            else:
                loop_cursor.execute("UPDATE Staging_SubCategory_Queue SET Status = 'failed', LastAttempt = GETDATE() WHERE SubCategoryQueueID = ?", (task_id))
            
            loop_conn.commit()
        except Exception as e:
            print(f"   ->  ERREUR (Tâche 1 - {task_id}) : {e}")
            if loop_conn: loop_conn.rollback()
            _update_queue_status_on_fail("Staging_SubCategory_Queue", "SubCategoryQueueID", task_id)
        finally:
            if loop_cursor: loop_cursor.close()
            if loop_conn: loop_conn.close()
            print("   -> Pause de 3 secondes...")
            time.sleep(3)

@task(task_id="Etape_2_Scrape_Products")
def task_2_scrape_products():
    """
    Étape 2 : Lit Staging_Product_Queue et scrape le JSON-LD
    de tous les produits 'pending'.
    """
    importlib.reload(vpe) 
    print("\n--- DÉBUT : Étape 2 (Scraping des Produits) ---")
    
    conn_init, cursor_init = vpe.get_staging_connection(), None
    try:
        cursor_init = conn_init.cursor()
        cursor_init.execute("SELECT ProductQueueID, ProductID_SKU, ProductURL FROM Staging_Product_Queue WHERE Status = 'pending' ORDER BY ProductQueueID")
        tasks_to_process = list(cursor_init.fetchall())
    finally:
        if cursor_init: cursor_init.close()
        if conn_init: conn_init.close()
    
    if not tasks_to_process:
        print("   ->  Fin : Aucun produit 'pending' à scraper.")
        return
        
    print(f"   -> {len(tasks_to_process)} produits à scraper. Lancement boucle...")
    
    total_tasks = len(tasks_to_process)
    for i, task_tuple in enumerate(tasks_to_process):
        task_id, task_sku, task_url = task_tuple
        loop_conn, loop_cursor = None, None
        print(f"\n--- Traitement Produit {i+1}/{total_tasks} (ID: {task_id}, SKU: {task_sku}) ---")
        try:
            product_json = vpe.scrape_product_page(task_url)
            loop_conn = vpe.get_staging_connection()
            loop_cursor = loop_conn.cursor()
            if product_json:
                vpe.save_product_json_to_staging(loop_cursor, task_sku, product_json, task_id)
                loop_cursor.execute("UPDATE Staging_Product_Queue SET Status = 'processed', LastAttempt = GETDATE() WHERE ProductQueueID = ?", (task_id))
            else:
                loop_cursor.execute("UPDATE Staging_Product_Queue SET Status = 'failed', LastAttempt = GETDATE() WHERE ProductQueueID = ?", (task_id))
            
            loop_conn.commit()
        except Exception as e:
            print(f"   ->  ERREUR (Tâche 2 - {task_id}) : {e}")
            if loop_conn: loop_conn.rollback()
            _update_queue_status_on_fail("Staging_Product_Queue", "ProductQueueID", task_id)
        finally:
            if loop_cursor: loop_cursor.close()
            if loop_conn: loop_conn.close()
            print("   -> Pause de 2 secondes...")
            time.sleep(2)

@task(task_id="Etape_3_Transform_JSON")
def task_3_transform_json():
    """
    Étape 3 : Lit Staging_VDB_Product_Page et transforme
    le JSON brut en colonnes propres dans Staging_Product_Cleansed.
    """
    importlib.reload(vpe) 
    print("\n--- DÉBUT : Étape 3 (Transformation JSON -> Colonnes) ---")
    
    conn_init, cursor_init = vpe.get_staging_connection(), None
    try:
        conn_init = vpe.get_staging_connection()
        cursor_init = conn_init.cursor()
        tasks_to_process = vpe.get_pending_raw_json(cursor_init)
    finally:
        if cursor_init: cursor_init.close()
        if conn_init: conn_init.close()

    if not tasks_to_process:
        print("---  Fin : Aucun JSON brut à transformer. ---")
        return

    for task in tasks_to_process:
        staging_key = task['StagingVDBKey']
        sku = task['ProductID_SKU']
        loop_conn, loop_cursor = None, None
        try:
            print(f"\n--- Transformation SKU {sku} (RawKey: {staging_key}) ---")
            cleaned_data = vpe.parse_and_clean_json(task['Raw_JSON_LD'])
            loop_conn = vpe.get_staging_connection()
            loop_cursor = loop_conn.cursor()
            vpe.save_cleansed_data(loop_cursor, staging_key, task['ScrapeTimestamp'], cleaned_data)
            vpe.update_raw_status(loop_cursor, staging_key, 'processed')
            loop_conn.commit()
            print(f"   ->  SKU {sku} transformé avec succès.")
        except Exception as e:
            print(f"   ->  ERREUR (Tâche 3 - {staging_key}) : {e}.")
            if loop_conn: loop_conn.rollback()
            _update_queue_status_on_fail("Staging_VDB_Product_Page", "StagingVDBKey", staging_key)
        finally:
            if loop_cursor: loop_cursor.close()
            if loop_conn: loop_conn.close()

# --- 6. Définition du DAG ---
@dag(
    dag_id="vdb_staging_pipeline",
    start_date=datetime(2025, 10, 29),
    schedule=None, # Se déclenche uniquement manuellement
    catchup=False,
    tags=['vandenborre_staging_new_category'],
    # Définition des paramètres pour le test
    params={
        "hub_url": Param(
            "https://www.vandenborre.be/fr/gros-electro",
            type="string",
            title="URL Hub (Parent)",
            description="L'URL de la catégorie parente à scraper (ex: /fr/gros-electro)"
        ),
        "hub_name": Param(
            "gros-electro",
            type="string",
            title="Nom Hub (Parent)",
            description="Le nom de cette catégorie parente (ex: 'Gros Electro')"
        )
    }
)
def vdb_staging_dag():
    """
    Orchestre le pipeline de Staging VDB (Étapes 0 à 3).
    Ce DAG reproduit le notebook 'run_staging_pipeline.ipynb'
    en utilisant les tables de Staging comme file d'attente.
    """
    
    # 1. Définir les tâches
    task_0a = task_0a_discover_themes()
    task_0b = task_0b_discover_subcategories()
    task_1 = task_1_discover_products()
    task_2 = task_2_scrape_products()
    task_3 = task_3_transform_json()

    # 2. Définir l'ordre d'exécution (en chaîne)
    task_0a >> task_0b >> task_1 >> task_2 >> task_3

# --- 7. Instanciation du DAG ---
vdb_staging_dag()