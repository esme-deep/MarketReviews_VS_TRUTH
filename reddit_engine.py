import pyodbc
import requests
import time
import re
from datetime import datetime

# --- 1. Configuration & Connexion ---
# (Tes fonctions de connexion restent inchangées)
DWH_CONFIG = {
    'server': r'LAPTOP-VT8FTHG2\DATAENGINEER', 
    'database': 'Projet_Market_DWH',
    'driver': '{ODBC Driver 17 for SQL Server}' 
}
STAGING_CONFIG = {
    'server': r'LAPTOP-VT8FTHG2\DATAENGINEER', 
    'database': 'Projet_Market_Staging',
    'driver': '{ODBC Driver 17 for SQL Server}' 
}
def get_dwh_connection():
    conn_str = (f"DRIVER={DWH_CONFIG['driver']};SERVER={DWH_CONFIG['server']};"
                f"DATABASE={DWH_CONFIG['database']};Trusted_Connection=yes;")
    return pyodbc.connect(conn_str)
def get_staging_connection():
    conn_str = (f"DRIVER={STAGING_CONFIG['driver']};SERVER={STAGING_CONFIG['server']};"
                f"DATABASE={STAGING_CONFIG['database']};Trusted_Connection=yes;")
    return pyodbc.connect(conn_str, autocommit=False) 

# --- 2. Fonctions du Pipeline Reddit (Étape 6) ---
# (get_products_to_search, generate_search_keyword, search_reddit_api restent inchangées)
# ... (colle-les ici)

def get_products_to_search(dwh_cursor):
    """Récupère la liste des produits (Clé, Nom, Marque) depuis le DWH."""
    print("   -> (E) Extraction des produits depuis Dim_Product...")
    dwh_cursor.execute("SELECT ProductKey, ProductName, Brand FROM Dim_Product WHERE ProductKey > 476 ")
    products = dwh_cursor.fetchall()
    print(f"   -> {len(products)} produits trouvés à rechercher sur Reddit.")
    return products

def generate_search_keyword(product_name, brand):
    """Tente de nettoyer un nom de produit retail en un mot-clé de recherche."""
    if not product_name: return None
    if not brand: brand = ""
    try:
        clean_name = re.sub(f'^{re.escape(brand)}', '', product_name, flags=re.IGNORECASE).strip()
        stop_words_regex = r'\b(BLACK|NOIR|WHITE|BLANC|POUCES|CM|\(2025\)|\(2024\)|4K|QLED|OLED|LED|FULL HD|AMBILIGHT|EVO|NEO|THE ONE)\b'
        clean_name = re.sub(stop_words_regex, '', clean_name, flags=re.IGNORECASE)
        clean_name = re.sub(r'\s+', ' ', clean_name).strip()
        search_keyword = f"{brand} {clean_name}".strip()
        parts = search_keyword.split(' ')
        if len(parts) > 4:
            search_keyword = " ".join(parts[:4])
        return search_keyword
    except Exception as e:
        print(f"    -> ⚠️ Erreur (generate_search_keyword): {e}")
        return f"{brand} {product_name}"

def search_reddit_api(search_keyword, subreddit="headphones", limit=25):
    """Appelle l'API de recherche Reddit pour un mot-clé de recherche propre."""
    print(f"   -> (E) Appel API Reddit pour : '{search_keyword}' dans r/{subreddit}")
    url = f"https://www.reddit.com/r/{subreddit}/search.json"
    params = {'q': search_keyword, 'sort': 'new', 'restrict_sr': 'true', 't': 'year', 'limit': limit}
    headers = {'User-Agent': 'ProjetDataEngineering-Asmae-v1.0'}
    posts_found = []
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status() 
        data = response.json()
        posts = data['data']['children']
        print(f"   -> {len(posts)} posts bruts trouvés.")
        for post in posts:
            post_data = post['data']
            post_date = datetime.fromtimestamp(post_data.get('created_utc', 0))
            posts_found.append({
                "PostID": post_data.get('name'), "PostTitle": post_data.get('title'),
                "PostText": post_data.get('selftext'), "PostURL": post_data.get('permalink'),
                "PostDate": post_date, "AuthorName": post_data.get('author')
            })
        return posts_found
    except Exception as e:
        print(f"   -> ❌ ERREUR (search_reddit_api) : {e}")
        return []

# --- 3. Fonction de Chargement (MODIFIÉE) ---

def save_raw_posts_to_staging(staging_cursor, posts_to_save, product_key, product_name, search_keyword):
    """
    Enregistre les posts bruts dans la table Staging_Reddit_Posts.
    MAINTENANT : Inclut les nouvelles colonnes de traçabilité.
    """
    print(f"   -> (L) Chargement de {len(posts_to_save)} posts dans Staging_Reddit_Posts...")
    insert_count = 0
    skipped_count = 0
    BASE_REDDIT_URL = "https://www.reddit.com"

    for post in posts_to_save:
        try:
            staging_cursor.execute("SELECT 1 FROM Staging_Reddit_Posts WHERE PostID = ?", (post['PostID'])) #ca c'est bien pensé
            if staging_cursor.fetchone():
                skipped_count += 1
                continue 
            
            full_url = f"{BASE_REDDIT_URL}{post['PostURL']}"
            
            # --- REQUÊTE INSERT MODIFIÉE ---
            staging_cursor.execute(
                """
                INSERT INTO Staging_Reddit_Posts
                    (ProductKey_Ref, ProductName_Ref, Search_Keyword_Used, -- Colonnes ajoutées
                     PostID, PostTitle, PostText, PostURL, PostDate, AuthorName, 
                     Status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending_sentiment')
                """,
                (
                    product_key,
                    product_name,     # Nouvelle donnée
                    search_keyword,   # Nouvelle donnée
                    post['PostID'],
                    post['PostTitle'],
                    post['PostText'],
                    full_url,
                    post['PostDate'],
                    post['AuthorName']
                )
            )
            insert_count += 1
            
        except pyodbc.IntegrityError:
             skipped_count += 1
             staging_cursor.rollback() 
        except Exception as e:
            print(f"   -> ❌ ERREUR (save_raw_posts_to_staging) : {e}")
            raise 
            
    print(f"   -> {insert_count} nouveaux posts insérés.")
    print(f"   -> {skipped_count} posts ignorés (doublons).")