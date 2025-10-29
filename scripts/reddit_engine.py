import pyodbc
import requests
import time
import re
from datetime import datetime


# --- 1. Configuration & Connexion (CORRIGÉE) ---
# Nous utilisons la méthode d'authentification SQL et 
# le serveur 'host.docker.internal' qui fonctionnait.

def get_dwh_connection():
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"    
        "SERVER=host.docker.internal,1433;"       
        "DATABASE=Projet_Market_DWH;"             
        "UID=airflow;"                            
        "PWD=airflow;"                            
        "Encrypt=yes;"                            
        "TrustServerCertificate=yes;"             
    )
    return pyodbc.connect(conn_str)

def get_staging_connection():
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=host.docker.internal,1433;"       
        "DATABASE=Projet_Market_Staging;"         
        "UID=airflow;"                            
        "PWD=airflow;"                            
        "Encrypt=yes;"                            
        "TrustServerCertificate=yes;"             
    )
    return pyodbc.connect(conn_str, autocommit=False)
# --- 1. Configuration & Connexion ---
# (Tes fonctions de connexion restent inchangées)
#DWH_CONFIG = {
#    'server': r'ICT-210-02', 
 #   'database': 'Projet_Market_DWH',
#    'driver': '{ODBC Driver 18 for SQL Server}' 
#}
#STAGING_CONFIG = {
  #  'server': r'ICT-210-02', 
   # 'database': 'Projet_Market_Staging',
   # 'driver': '{ODBC Driver 18 for SQL Server}' 
#}
#def get_dwh_connection():
  #  conn_str = (f"DRIVER={DWH_CONFIG['driver']};SERVER={DWH_CONFIG['server']};"
  #              f"DATABASE={DWH_CONFIG['database']};Trusted_Connection=yes;")
  #  return pyodbc.connect(conn_str)
#def get_staging_connection():
  #  conn_str = (f"DRIVER={STAGING_CONFIG['driver']};SERVER={STAGING_CONFIG['server']};"
   #             f"DATABASE={STAGING_CONFIG['database']};Trusted_Connection=yes;")
  #  return pyodbc.connect(conn_str, autocommit=False) 

# --- 2. Fonctions du Pipeline Reddit (Étape 6) ---
# (get_products_to_search, generate_search_keyword, search_reddit_api restent inchangées)
# ... (colle-les ici)
# Dans reddit_engine.py

# ... (les fonctions de connexion restent les mêmes) ...

def get_products_to_search(dwh_cursor):
    """
    MODIFIÉ : Récupère UNIQUEMENT la liste des ProductKey à traiter.
    """
    print("   -> (E) Extraction des ProductKey depuis Dim_Product...")
    query = """
        SELECT 
            p.ProductKey
        FROM 
            Projet_Market_DWH.dbo.Dim_Product AS p
        WHERE NOT EXISTS (
            SELECT 1 
            FROM Projet_Market_Staging.dbo.Staging_Reddit_Posts AS r
            WHERE r.ProductKey_Ref = p.ProductKey
        )
        ORDER BY
            p.ProductKey;
    """
    dwh_cursor.execute(query)
    products = dwh_cursor.fetchall()
    print(f"   -> {len(products)} ProductKey trouvés à rechercher sur Reddit.")
    return products


def get_product_details(dwh_cursor, product_key):
    """
    NOUVEAU : Récupère les détails (Nom, Marque, Catégorie) 
    pour UN SEUL ProductKey.
    """
    query = """
        SELECT ProductName, Brand, Category
        FROM Projet_Market_DWH.dbo.Dim_Product
        WHERE ProductKey = ?;
    """
    dwh_cursor.execute(query, (product_key,))
    details = dwh_cursor.fetchone()
    if details:
        return details.ProductName, details.Brand, details.Category
    else:
        raise Exception(f"Aucun détail trouvé pour ProductKey {product_key}")

# ... (le reste de vos fonctions : generate_search_keyword, search_reddit_api, etc. restent les mêmes) ...
def get_products_to_search_v1(dwh_cursor):
    """Récupère la liste des produits (Clé, Nom, Marque) depuis le DWH. ELLE FONCTIONNE POUR une recherche via un jupyter"""
    print("   -> (E) Extraction des produits depuis Dim_Product...")
    query = """
        SELECT 
            p.ProductKey, 
            p.ProductName, 
            p.Brand,
            p.Category
        FROM 
            Projet_Market_DWH.dbo.Dim_Product AS p
        WHERE NOT EXISTS (
            SELECT 1 
            FROM Projet_Market_Staging.dbo.Staging_Reddit_Posts AS r
            WHERE r.ProductKey_Ref = p.ProductKey
        )
        ORDER BY
            p.ProductKey;
    """
    dwh_cursor.execute(query)
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
    
def get_subreddit_for_category(category_name):
    """
    Traduit un nom de catégorie VDB en un nom de subreddit pertinent.
    """
    if not category_name:
        return "all" # Sécurité
        
    # Nettoyer le nom de la catégorie (ex: "TV (243)" -> "TV")
    clean_category = re.sub(r"\(.*\)", "", category_name).strip()
    
    # Dictionnaire de Mapping (basé sur ta liste)
    SUBREDDIT_MAPPING = {
        'Casque audio': 'headphones',
        'Écouteurs': 'headphones',
        'Tous les casques': 'headphones',
        'Casques TV': 'headphones',
        'Casques microphone': 'headphones',
        'Enceinte PC': 'PCSound',
        'Casques gaming': 'gaming_headsets',
        
        'TV OLED': 'OLED_Gaming', # r/OLED_Gaming est très actif
        'TV': 'Televisions',
        'TV Petite Taille (-32 pouces)': 'Televisions',
        
        'Projecteur': 'projectors',
        'Projecteurs portables': 'projectors',
        'Projecteurs Xgimi': 'projectors',
        'Écran de projection': 'projectors',

        'Home cinéma': 'hometheater',
        'Barres de son': 'soundbars',
        'Ampli home cinéma': 'hometheater',
        'Ampli hifi': 'hometheater',

        'Radio et Hi-Fi': 'audiophile',
        'Chaîne Hi-Fi': 'audiophile',
        'Radios': 'radio',
        'Radios internet': 'radio',
        'Radio FM / DAB': 'radio',
        'Radio-réveil': 'radio',
        'Autoradio': 'CarAV', # r/CarAV
        'Radio CD': 'radio',

        'Tourne-disque': 'vinyl',
        'Accessoires platines disques': 'vinyl',
        'Lecteur CD': 'audiophile',

        'Enceinte sans fil': 'bluetooth_speakers',
        'Enceinte Wi-Fi': 'bluetooth_speakers',
        'Enceinte Bluetooth': 'bluetooth_speakers',
        'Enceinte de soirée': 'bluetooth_speakers',
        'Haut-parleur': 'audiophile',
        
        'Lecteur multimédia / Streaming': 'streaming',
        'Lecteurs DVD': 'dvdcollection',
        'Lecteur DVD portable': 'dvdcollection',
        'Disques CD / DVD / Blu-ray': 'dvdcollection',

        'Micro pour appareil photo': 'videography',
        'Télécommande': 'hometheater',
        'Support TV': 'hometheater',
        'Meuble TV': 'hometheater',
        
        # ...etc.
    }
    
    # Cherche le subreddit, s'il n'est pas trouvé, utilise 'all'
    target_subreddit = SUBREDDIT_MAPPING.get(clean_category, "all")
    return target_subreddit

def search_reddit_api(search_keyword, subreddit, limit=25):
    """Appelle l'API de recherche Reddit pour un mot-clé de recherche propre."""
    print(f"   -> (E) Appel API Reddit pour : '{search_keyword}' dans r/{subreddit}")
    url = f"https://www.reddit.com/r/{subreddit}/search.json"
    params = {'q': search_keyword, 'sort': 'new', 'restrict_sr': 'true', 't': 'year', 'limit': limit}
    # Si le subreddit est 'all' (parce qu'on n'a pas trouvé de mapping),
    # on ne restreint PAS la recherche au subreddit "all".
    if subreddit == "all":
        params['restrict_sr'] = 'false'
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
    finally:
        # --- PAUSE DE SÉCURITÉ INTÉGRÉE ---
        # Garantit que nous respectons la limite de 10 req/min (6s/req)
        print("   -> Pause de 7 secondes (respect des limites API)...")
        time.sleep(7)

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


