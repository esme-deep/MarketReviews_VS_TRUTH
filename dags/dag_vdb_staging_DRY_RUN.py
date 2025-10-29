import sys
import os
from datetime import datetime
import importlib
import json
import time

# --- 1. Configuration du chemin ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# --- 2. Imports Airflow ---
from airflow.decorators import dag, task
from airflow.models.param import Param

# --- 3. Import de votre MOTEUR (version 'scripts') ---
from scripts import vdb_pipeline_engine as vpe 

# --- 4. Définition de la Tâche de Test ---

@task(task_id="run_dry_run_discovery")
def task_dry_run_discovery(**kwargs):
    """
    Exécute un "Dry Run" (simulation) du pipeline VDB.
    
    Cette tâche appelle toutes les fonctions de 'discovery' et 'scraping'
    et AFFICHE leur résultat dans les logs SANS RIEN ÉCRIRE 
    en base de données.
    """
    importlib.reload(vpe) 
    
    params = kwargs['params']
    URL_UNIVERS = params['hub_url']
    NOM_UNIVERS = params['hub_name']
    
    print("="*40)
    print(f"--- DÉBUT DU DRY RUN (SIMULATION) ---")
    print(f"--- Cible : {NOM_UNIVERS} ({URL_UNIVERS}) ---")
    print("--- AUCUNE DONNÉE NE SERA ÉCRITE EN BDD ---")
    print("="*40)

    try:
        # --- Test Étape 0a ---
        print("\n--- Test Étape 0a (discover_all_categories)... ---")
        categories_data = vpe.discover_all_categories(URL_UNIVERS)
        if not categories_data:
            print("   -> 0a : ÉCHEC. Aucun thème trouvé. Arrêt.")
            return
        
        print(f"   -> 0a : SUCCÈS. {len(categories_data)} thèmes trouvés :")
        print(json.dumps(categories_data, indent=2, ensure_ascii=False))
        
        # Teste la première catégorie trouvée
        first_category = categories_data[0]
        time.sleep(2)

        # --- Test Étape 0b ---
        print("\n--- Test Étape 0b (discover_all_subcategories)... ---")
        print(f"   -> Test sur le premier thème : '{first_category['category_name']}'")
        subcategories_data = vpe.discover_all_subcategories(
            parent_category_name=first_category['category_name'], 
            hub_url=first_category['url']
        )
        if not subcategories_data:
            print("   -> 0b : ÉCHEC. Aucune sous-catégorie trouvée. Arrêt.")
            return
            
        print(f"   -> 0b : SUCCÈS. {len(subcategories_data)} sous-catégories trouvées :")
        print(json.dumps(subcategories_data, indent=2, ensure_ascii=False))

        # Teste la première sous-catégorie
        first_subcategory = subcategories_data[0]
        time.sleep(2)

        # --- Test Étape 1 ---
        print("\n--- Test Étape 1 (discover_all_products)... ---")
        print(f"   -> Test sur la première sous-catégorie : '{first_subcategory['category_name']}'")
        # On ne scrape qu'une page (en mettant expected_item_count à 10) pour ce test
        product_data = vpe.discover_all_products(
            subcategory_name=first_subcategory['category_name'], 
            category_url=first_subcategory['url'], 
            expected_item_count=10 # Limite à 10 pour le test
        )
        if not product_data:
            print("   -> 1 : ÉCHEC. Aucun produit trouvé. Arrêt.")
            return
        
        print(f"   -> 1 : SUCCÈS. {len(product_data)} produits trouvés (limite 10) :")
        print(json.dumps(product_data, indent=2, ensure_ascii=False))

        # Teste le premier produit
        first_product = product_data[0]
        time.sleep(2)

        # --- Test Étape 2 ---
        print("\n--- Test Étape 2 (scrape_product_page)... ---")
        print(f"   -> Test sur le premier produit : SKU {first_product['sku']}")
        product_json = vpe.scrape_product_page(first_product['url'])
        if not product_json:
            print("   -> 2 : ÉCHEC. JSON-LD non trouvé. Arrêt.")
            return
            
        print(f"   -> 2 : SUCCÈS. JSON-LD brut trouvé :")
        print(json.dumps(product_json, indent=2, ensure_ascii=False))

        # --- Test Étape 3 ---
        print("\n--- Test Étape 3 (parse_and_clean_json)... ---")
        cleaned_data = vpe.parse_and_clean_json(json.dumps(product_json)) # Re-serialise pour simuler
        
        print(f"   -> 3 : SUCCÈS. Données nettoyées :")
        print(json.dumps(cleaned_data, indent=2, ensure_ascii=False))
        
        print("\n" + "="*40)
        print("--- DRY RUN TERMINÉ AVEC SUCCÈS ---")
        print("="*40)

    except Exception as e:
        print(f"\n--- ❌ ERREUR PENDANT LE DRY RUN : {e} ---")
        raise

# --- 5. Définition du DAG ---
@dag(
    dag_id="vdb_staging_pipeline_DRY_RUN",
    start_date=datetime(2025, 10, 29),
    schedule=None, # Se déclenche uniquement manuellement
    catchup=False,
    tags=['vandenborre_staging_new_cat_DRY_RUN'],
    # Paramètres pour lancer le test
    params={
        "hub_url": Param(
            "https://www.vandenborre.be/fr/gros-electro",
            type="string",
            title="URL Hub (Parent) à Tester"
        ),
        "hub_name": Param(
            "gros-electro",
            type="string",
            title="Nom Hub (Parent) à Tester"
        )
    }
)
def vdb_staging_dry_run_dag():
    """
    Exécute un "Dry Run" (simulation) du pipeline VDB
    pour visualiser les données scrappées sans toucher à la BDD.
    """
    task_dry_run_discovery()

# --- 6. Instanciation du DAG ---
vdb_staging_dry_run_dag()