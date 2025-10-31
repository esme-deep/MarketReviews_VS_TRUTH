import sys
import os
from datetime import datetime
import importlib

# --- Configuration du chemin ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# --- Imports Airflow ---
from airflow.decorators import dag, task

# --- Import de votre moteur ---
# (Nous importons depuis le dossier 'scripts/' comme convenu)
try:
    from scripts import reddit_engine as rpe 
except ImportError:
    print("ERREUR : Impossible d'importer 'reddit_engine' depuis le dossier 'scripts/'.")
    raise

# --- Définition de la TÂCHE UNIQUE ---

@task(task_id="run_full_reddit_pipeline")
def run_full_reddit_pipeline_task():
    """
    Tâche monolithique qui exécute l'intégralité de l'Étape 6.
    Elle gère l'erreur XCOM en ne retournant rien,
    et en faisant tout dans une seule session.
    """
    
    # --- Logique de la Tâche 1 (Get Products) ---
    print("--- Lancement de l'Extraction (Étape 6a) ---")
    conn_dwh = None
    cursor_dwh = None
    product_list_to_search = []
    
    try:
        importlib.reload(rpe) 
        conn_dwh = rpe.get_dwh_connection()
        cursor_dwh = conn_dwh.cursor()
        print("   -> ✅ Connecté au DWH.")

        # Appelle la fonction originale (récupère Nom, Marque, etc.)
        query = """
            SELECT p.ProductKey, p.ProductName, p.Brand, p.Category
            FROM Projet_Market_DWH.dbo.Dim_Product AS p
            WHERE NOT EXISTS (
                SELECT 1 
                FROM Projet_Market_Staging.dbo.Staging_Reddit_Posts AS r
                WHERE r.ProductKey_Ref = p.ProductKey
            ) AND p.ProductKey > 1213
            ORDER BY p.ProductKey;
        """
        cursor_dwh.execute(query)
        product_list_to_search = cursor_dwh.fetchall()
        
        if product_list_to_search:
            print(f"--- ✅ SUCCÈS : {len(product_list_to_search)} produits à rechercher extraits ---")
        else:
            print("--- ⚠️ AVERTISSEMENT : Aucun nouveau produit à rechercher trouvé. ---")

    except Exception as e:
        print(f"--- ❌ ERREUR (get_products) : {e} ---")
        raise
    finally:
        if cursor_dwh: cursor_dwh.close()
        if conn_dwh: conn_dwh.close()
        print("Connexion DWH fermée (phase 1).")

    # Si la liste est vide, on arrête la tâche
    if not product_list_to_search:
        print("Aucun produit à traiter. Tâche terminée.")
        return

    # --- Logique de la Tâche 2 (Boucle de Traitement) ---
    total_products = len(product_list_to_search)
    print(f"\n--- Lancement de la boucle de scraping pour {total_products} produits ---")

    for i, product in enumerate(product_list_to_search):
        product_key = product.ProductKey
        product_name = product.ProductName
        brand = product.Brand
        category = product.Category
        
        conn_staging = None
        cursor_staging = None
        
        print(f"\n-------------------------------------------------")
        print(f"--- Traitement Produit {i+1}/{total_products} (Key: {product_key}) ---")
        
        try:
            # 1. Transformer (T) - Générer le mot-clé
            search_keyword = rpe.generate_search_keyword(product_name, brand)
            print(f"   -> Mot-clé généré : '{search_keyword}'")
            
            if not search_keyword:
                print("   -> ⚠️ Mot-clé vide, impossible de chercher. On ignore.")
                continue

            # 2. Transformer (T) - Trouver le bon subreddit
            target_subreddit = rpe.get_subreddit_for_category(category)
            
            # 3. Extraire (E) - Scraper (inclut la pause de 7 sec)
            raw_posts = rpe.search_reddit_api(search_keyword, subreddit=target_subreddit)
            
            if raw_posts:
                # 4. Charger (L)
                conn_staging = rpe.get_staging_connection()
                cursor_staging = conn_staging.cursor()
                
                # --- CORRECTION APPLIQUÉE ICI ---
                # On passe maintenant 'brand' comme dernier argument
                # pour correspondre à la nouvelle fonction moteur.
                rpe.save_raw_posts_to_staging(
                    cursor_staging, 
                    raw_posts, 
                    product_key, 
                    product_name, 
                    search_keyword,
                    brand  # <-- L'ARGUMENT MANQUANT A ÉTÉ AJOUTÉ
                )
                # ---------------------------------
                
                conn_staging.commit()
                print(f"   -> ✅ Données pour '{search_keyword}' sauvegardées.")
            else:
                print(f"   -> ⚠️ Aucun post trouvé pour '{search_keyword}' dans r/{target_subreddit}.")

        except Exception as e:
            print(f"   -> ❌ ERREUR lors du traitement du produit {product_name} : {e}")
            if conn_staging: conn_staging.rollback()
            # On log l'erreur mais on continue la boucle
            print(f"   -> ERREUR IGNORÉE, passage au produit suivant.") 
        finally:
            if cursor_staging: cursor_staging.close()
            if conn_staging: conn_staging.close()

    print("\n--- ✅ Boucle de scraping Reddit terminée. ---")


# --- Définition du DAG ---

@dag(
    dag_id="reddit_extraction_pipeline",
    start_date=datetime(2025, 10, 29),
    schedule="@daily",
    catchup=False,
    tags=['reddit_posts_search'] # J'ai gardé votre tag
)
def reddit_pipeline_dag():
    """
    Orchestre l'extraction des données Reddit (Étape 6)
    en une seule tâche monolithique pour éviter les erreurs XCom.
    """
    
    # Tâche 1: Exécuter tout
    run_full_reddit_pipeline_task()

# --- Instanciation du DAG ---
reddit_pipeline_dag()