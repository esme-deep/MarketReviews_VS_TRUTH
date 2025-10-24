import requests
from bs4 import BeautifulSoup
import json
import re
import pyodbc 
from datetime import datetime
import math
import time

def discover_all_categories(main_hub_url):
    """
    Scrappe une page "hub" principale (comme /tv-audio) pour trouver
    toutes les catégories de niveau 2 (Télévision, Home cinéma, etc.).
    
    Args:
        main_hub_url (str): L'URL de la page "hub" principale.
                            (ex: "https://www.vandenborre.be/fr/tv-audio")
        
    Retourne:
        Une liste de dictionnaires (ta "variable" de catégories). Ex:
        [
            {'category_name': 'Télévision', 'url': 'https://.../tv-audio/television'},
            {'category_name': 'Home cinéma', 'url': 'https://.../tv-audio/home-cinema'},
            ...
        ]
    """
    BASE_URL = "https://www.vandenborre.be"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    
    categories_found = []
    
    print(f"--- Lancement de la Découverte des Catégories (Étape 0a) ---")
    print(f"Scraping de la page Hub : {main_hub_url}...")
    
    try:
        session = requests.Session()
        session.headers.update(headers)
        response = session.get(main_hub_url, timeout=15)
        response.raise_for_status() # Stoppe si erreur 403, 404, etc.
        
        soup = BeautifulSoup(response.content, 'html.parser')

        # --- Extraction HTML ---
        # Le conteneur principal pour les catégories sur cette page
        main_container = soup.find('div', {'class': 'rubric-families'})
        if not main_container:
            print("❌ ERREUR : Impossible de trouver le conteneur 'div.rubric-families'.")
            return []

        # Trouver tous les blocs de catégorie
        category_blocks = main_container.find_all('div', {'class': 'accordion-image-grid'})
        
        print(f"   -> {len(category_blocks)} blocs de catégories trouvés.")

        for block in category_blocks:
            # Le lien est dans un <h2> pour les écrans larges
            link_tag = block.find('h2', {'class': 'hidden-xs'}).find('a')
            
            if not link_tag:
                continue

            category_name = link_tag.get('title')
            relative_url = link_tag.get('href')

            if category_name and relative_url:
                # Reconstruire l'URL complète
                product_url = ""
                if relative_url.startswith('//'):
                    product_url = f"https:{relative_url}"
                elif relative_url.startswith('/'):
                    product_url = f"{BASE_URL}{relative_url}"
                else:
                    product_url = relative_url
                    
                categories_found.append({
                    "category_name": category_name,
                    "url": product_url
                })
        
        print(f"--- Découverte des catégories terminée. {len(categories_found)} catégories extraites. ---")
        return categories_found

    except requests.exceptions.HTTPError as err:
        print(f"\n--- ❌ ERREUR HTTP : {err} ---")
        return []
    except Exception as e:
        print(f"\n--- ❌ ERREUR INATTENDUE : {e} ---")
        return []
    

# --- 1. Configuration SQL Server (Authentification Windows) ---
# (Basée sur votre configuration)
DB_CONFIG = {
    'server': r'LAPTOP-VT8FTHG2\DATAENGINEER', 
    'database': 'Projet_Market_Staging',
    'driver': '{ODBC Driver 17 for SQL Server}' 
}

# Construit la chaîne de connexion
conn_str = (
    f"DRIVER={DB_CONFIG['driver']};"
    f"SERVER={DB_CONFIG['server']};"
    f"DATABASE={DB_CONFIG['database']};"
    "Trusted_Connection=yes;" # Authentification Windows
)

def save_categories_to_staging(categories_to_save, univers_name):
    """
    Se connecte à la BDD et enregistre la liste des catégories 
    dans la table Staging_Category_Queue.
    
    Args:
        categories_to_save (list): La liste de dicts (le résultat de 
                                   discover_all_categories).
        univers_name (str): Le nom de la catégorie parente (ex: "TV et Audio").
    """
    print(f"\n--- Lancement du Chargement des Catégories (Étape L) ---")
    print(f"Connexion à SQL Server : {DB_CONFIG['server']}...")
    conn = None
    cursor = None
    
    try:
        conn = pyodbc.connect(conn_str, autocommit=False) # autocommit=False pour gérer la transaction
        cursor = conn.cursor()
        print("   -> ✅ Connecté à SQL Server avec succès.")

        insert_count = 0
        update_count = 0
        
        for cat in categories_to_save:
            category_name = cat['category_name']
            category_url = cat['url']
            
            # Vérifie si la catégorie existe déjà (basé sur l'URL)
            cursor.execute("SELECT CategoryQueueID FROM Staging_Category_Queue WHERE CategoryURL = ?", (category_url))
            existing_task = cursor.fetchone()
            
            if existing_task is None:
                # Cas 1: Nouvelle catégorie. On l'ajoute.
                cursor.execute(
                    """
                    INSERT INTO Staging_Category_Queue 
                        (CategoryName, CategoryURL, ParentCategoryName, Status, DiscoveredAt) 
                    VALUES (?, ?, ?, 'pending', GETDATE())
                    """,
                    (category_name, category_url, univers_name)
                )
                insert_count += 1
            else:
                # Cas 2: Catégorie déjà vue. On la réactive (met à jour le nom et le statut)
                cursor.execute(
                    """
                    UPDATE Staging_Category_Queue 
                    SET Status = 'pending', LastAttempt = NULL, CategoryName = ?, ParentCategoryName = ?
                    WHERE CategoryQueueID = ?
                    """,
                    (category_name, univers_name, existing_task.CategoryQueueID)
                )
                update_count += 1
        
        conn.commit() # Valide toutes les insertions et mises à jour
        print(f"   -> ✅ Terminé : {insert_count} nouvelles catégories insérées.")
        print(f"   -> ✅           {update_count} catégories existantes réactivées.")
        
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"\n--- ❌ ERREUR SQL Server : {sqlstate} ---")
        print("   Vérifie tes 'DB_CONFIG': nom du serveur, nom de la base, et que le driver ODBC est installé.")
        print(f"   Chaîne de connexion tentée : {conn_str}")
        if 'conn' in locals() and conn: conn.rollback() # Annule la transaction en cas d'erreur
    except Exception as e:
        print(f"\n--- ❌ ERREUR INATTENDUE : {e} ---")
        if 'conn' in locals() and conn: conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("Connexion SQL Server fermée.")