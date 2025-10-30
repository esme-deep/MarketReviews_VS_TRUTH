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
    """
    MODIFIÉ (v8) : Ajoute plus de polluants comme FHD, FULL, 
    et les années (ex: 2022).
    """
    if not product_name: return None
    if not brand: brand = ""
    
    try:
        clean_name = product_name
        
        if brand:
            clean_name = re.sub(f'^{re.escape(brand)}', '', clean_name, flags=re.IGNORECASE).strip()
            
        measurements_regex = r'\b\d+([.,]\d+)?\s*(POUCES|CM|"|\'|M|ML|GB|GBT|DB|GO|W)\b'
        clean_name = re.sub(measurements_regex, '', clean_name, flags=re.IGNORECASE)

        POLLUTANT_WORDS = [
            'ACCSUP', 'CLEANER', 'SCREENCLEANING', 'CABLE', 'CAB', 'HDMI', 'RCA', 
            'JACK', 'USB', 'USB-C', 'VGA', 'SCART', 'OPTIC', 'OPTIQUE', 'COAX', 
            'COAXIAL', 'DVI', 'LIGHTNING', 'ADAPT', 'ADAPTER', 'CONVERTER', 
            'SPLITTER', 'KIT', 'ANTENNA', 'ANTENNE', 'REMOTE', 'FICHES', 'MOUNT', 
            'WALLMOUNT', 'FLOORSTAND', 'STAND', 'SUPPORT', 'FIX', 'TILT', 
            'ORIENTABLE', 'MICRO', 'MICROPHONE', 'CASE', 'CHARGE', 'HD', 'FHD', 'FULL', 'UHD', 
            '4K', '8K', '2K', 'QLED', 'OLED', 'LED', 'LCD', 'MINI-LED', 'BT', 
            'BLUETOOTH', 'WIFI', 'WI-FI', 'ANC', 'DAB\+', 'DAB', 'FM', 'SMART', 
            'WIRELESS', 'SANS FIL', 'FILAIRE', 'INTERNET', 'STEREO', 'SURROUND', 
            'BLACK', 'BLK', 'NOIR', 'WHITE', 'WHT', 'WH', 'BLANC', 'BLUE', 'BLEU', 
            'RED', 'ROSE', 'PINK', 'GREY', 'GRAY', 'GRIS', 'SILVER', 'ARGENT', 
            'GOLD', 'OR', 'GREEN', 'VERT', 'ORANGE', 'PURPLE', 'CREAM', 'WOOD', 
            'COPPER', 'LILAC', 'CHARCOAL', 'SLATE', 'WALNUT', 'ASH', 'MALE', 'FEMALE',
            'GEN', 'GÉNÉRATION', 'GENERATION', 'MKII', 'V2', 'V3', 'V4', 'PACK', 
            'KIDS', 'ECO', 'WW', 'EU', 'PAIR', 'PAIRE', 'BUNDLE', 'X2', 'X5', 
            '3 IN 1', 'ULTRA', 'PRO', 'MINI', 'NANO', 'LITE', 'AIR', 'MAX', 'PLUS',
            'TV', 'AUDIO', 'HOME', 'SPEAKER', 'HEADPHONE', 'EARBUDS', 'TWS',
            '2021', '2022', '2023', '2024', '2025' # Ajout des années
        ]
        pollutants_regex = r'\b(' + '|'.join(POLLUTANT_WORDS) + r')\b'
        clean_name = re.sub(pollutants_regex, '', clean_name, flags=re.IGNORECASE)

        clean_name = re.sub(r'[\(\)/\\\-+_]', ' ', clean_name)
        clean_name = re.sub(r'[^a-zA-Z0-9\s]', '', clean_name)
        
        # Supprime les nombres "orphelins" (1-2 chiffres)
        clean_name = re.sub(r'\b\d{1,2}\b', '', clean_name)
        
        clean_name = re.sub(r'\s+', ' ', clean_name).strip()
        
        if len(clean_name) < 3: 
             print(f"   -> ⚠️ Mot-clé rejeté car trop court après nettoyage (Nom nettoyé: '{clean_name}').")
             return None 

        final_keyword = f"{brand} {clean_name}".strip()
        
        parts = final_keyword.split(' ')
        if len(parts) > 4:
            final_keyword = " ".join(parts[:4])
            
        return final_keyword

    except Exception as e:
        print(f"    -> ⚠️ Erreur (generate_search_keyword): {e}. Retour du nom original.")
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

#
# --- REMPLACEZ CETTE FONCTION (VERSION À DEUX PASSES) ---
#
def _execute_reddit_search(search_keyword, subreddit, limit):
    """Fonction helper pour exécuter un appel API."""
    print(f"   -> (E) Appel API Reddit pour : '{search_keyword}' dans r/{subreddit}")
    url = f"https://www.reddit.com/r/{subreddit}/search.json"
    params = {'q': search_keyword, 'sort': 'relevance', 'restrict_sr': 'true', 't': 'year', 'limit': limit}
    
    if subreddit == "all":
        params['restrict_sr'] = 'false'
        
    headers = {'User-Agent': 'ProjetDataEngineering-Asmae-v1.0'}
    
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status() 
        data = response.json()
        return data['data']['children']
    except Exception as e:
        print(f"   -> ❌ ERREUR (_execute_reddit_search) : {e}")
        return []
    finally:
        print("   -> Pause de 7 secondes (respect des limites API)...")
        time.sleep(7)

def search_reddit_api(search_keyword, subreddit, limit=25):
    """
    MODIFIÉ (v8 - Deux Passes) :
    Passe 1 : Tente une recherche spécifique.
    Passe 2 : Si échec, tente une recherche large (juste la marque).
    """
    
    # --- PASSE 1 : Recherche Spécifique ---
    posts_data = _execute_reddit_search(search_keyword, subreddit, limit)
    
    # --- PASSE 2 : Recherche Large (si la Passe 1 échoue) ---
    if not posts_data:
        print(f"   -> ⚠️ Passe 1 (spécifique) n'a rien donné pour '{search_keyword}'.")
        # On extrait la marque (le premier mot du mot-clé)
        brand = search_keyword.split(' ')[0]
        
        # On ne fait la passe 2 que si la marque est différente du mot-clé
        if brand != search_keyword and len(brand) > 2:
            print(f"   -> Lancement Passe 2 (large) avec juste la marque: '{brand}'")
            posts_data = _execute_reddit_search(brand, subreddit, limit)
        else:
            print(f"   -> Mot-clé trop générique, annulation de la Passe 2.")

    # --- Traitement des résultats (des deux passes) ---
    print(f"   -> {len(posts_data)} posts bruts trouvés au total.")
    posts_found = []
    
    for post in posts_data:
        post_data = post['data']
        post_date = datetime.fromtimestamp(post_data.get('created_utc', 0))
        
        author_cakeday_utc = post_data.get('author_cakeday') 
        author_creation_date = None
        if author_cakeday_utc:
            author_creation_date = datetime.fromtimestamp(author_cakeday_utc)

        posts_found.append({
            "PostID": post_data.get('name'), 
            "PostTitle": post_data.get('title'),
            "PostText": post_data.get('selftext'), 
            "PostURL": post_data.get('permalink'),
            "PostDate": post_date, 
            "AuthorName": post_data.get('author'),
            "AuthorCreationDate": author_creation_date
        })
    return posts_found

# --- 3. Fonction de Chargement (MODIFIÉE) ---

def save_raw_posts_to_staging(staging_cursor, posts_to_save, product_key, product_name, search_keyword, brand):
    """
    MODIFIÉ (Filtre de Pertinence) :
    Enregistre les posts bruts MAIS SEULEMENT SI le titre ou le texte
    contient la marque, pour filtrer les résultats non pertinents.
    """
    # 1. On ajoute un nouveau compteur
    print(f"   -> (L) {len(posts_to_save)} posts reçus. Application du filtre de pertinence (Marque: '{brand}')...")
    insert_count = 0
    skipped_count = 0
    filtered_out_count = 0 # Compteur pour les posts hors sujet
    BASE_REDDIT_URL = "https://www.reddit.com"

    for post in posts_to_save:
        try:
            # 2. Vérification des doublons (votre code, ne change pas)
            staging_cursor.execute("SELECT 1 FROM Staging_Reddit_Posts WHERE PostID = ?", (post['PostID']))
            if staging_cursor.fetchone():
                skipped_count += 1
                continue 
            
            # --- 3. FILTRE DE PERTINENCE (NOUVEAU) ---
            # On vérifie si la marque (ex: 'philips') est dans le texte
            # On gère le cas où la marque serait None
            if brand and brand.strip() != "":
                # On combine le titre et le texte pour la recherche
                title = post['PostTitle'] or ""
                text = post['PostText'] or ""
                full_text = f"{title} {text}".lower() # Tout en minuscules
                
                if brand.lower() not in full_text:
                    filtered_out_count += 1
                    continue # Le post est hors sujet, on le saute
            # --- FIN DU FILTRE ---

            full_url = f"{BASE_REDDIT_URL}{post['PostURL']}"
            
            # 4. Insertion (votre code, ne change pas)
            staging_cursor.execute(
                """
                INSERT INTO Staging_Reddit_Posts
                    (ProductKey_Ref, ProductName_Ref, Search_Keyword_Used,
                     PostID, PostTitle, PostText, PostURL, PostDate, AuthorName, 
                     AuthorCreationDate, Status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending_sentiment')
                """,
                (
                    product_key, product_name, search_keyword,
                    post['PostID'], post['PostTitle'], post['PostText'],
                    full_url, post['PostDate'], post['AuthorName'],
                    post['AuthorCreationDate']
                )
            )
            insert_count += 1
            
        except pyodbc.IntegrityError:
             skipped_count += 1
             staging_cursor.rollback() 
        except Exception as e:
            print(f"   -> ❌ ERREUR (save_raw_posts_to_staging) : {e}")
            raise 
            
    # 5. Affichage des 3 compteurs
    print(f"   -> {insert_count} nouveaux posts (pertinents) insérés.")
    print(f"   -> {filtered_out_count} posts filtrés (hors sujet).")
    print(f"   -> {skipped_count} posts ignorés (doublons).")