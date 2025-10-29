import requests
from bs4 import BeautifulSoup
import json
import re
import pyodbc
from datetime import datetime
import time
import math

# --- 1. Configuration & Connexion (Staging) ---
STAGING_CONFIG = {
    'server': r'LAPTOP-VT8FTHG2\DATAENGINEER', 
    'database': 'Projet_Market_Staging',
    'driver': '{ODBC Driver 17 for SQL Server}' 
}

def get_staging_connection():
    """Crée et retourne une connexion à la base de Staging."""
    conn_str = (
        f"DRIVER={STAGING_CONFIG['driver']};"
        f"SERVER={STAGING_CONFIG['server']};"
        f"DATABASE={STAGING_CONFIG['database']};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=False)

# --- 2. Fonctions de l'Étape 0a (Découverte des Thèmes) ---

def discover_all_categories(main_hub_url):
    """
    Scrappe une page "hub" principale (ex: /tv-audio) pour trouver
    toutes les catégories "thème" (Télévision, Home cinéma, etc.).
    """
    BASE_URL = "https://www.vandenborre.be"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    
    categories_found = []
    print(f"--- (0a) Scraping de la page Hub : {main_hub_url}...")
    
    try:
        session = requests.Session()
        session.headers.update(headers)
        response = session.get(main_hub_url, timeout=15)
        response.raise_for_status() 
        soup = BeautifulSoup(response.content, 'html.parser')
        
        main_container = soup.find('div', {'class': 'rubric-families'})
        if not main_container:
            print("   ->  ERREUR : Conteneur 'div.rubric-families' non trouvé.")
            return []

        category_blocks = main_container.find_all('div', {'class': 'accordion-image-grid'})
        
        for block in category_blocks:
            link_tag = block.find('h2', {'class': 'hidden-xs'}).find('a')
            if not link_tag: continue

            category_name = link_tag.get('title')
            relative_url = link_tag.get('href')

            if category_name and relative_url:
                url = ""
                if relative_url.startswith('//'): url = f"https:{relative_url}"
                elif relative_url.startswith('/'): url = f"{BASE_URL}{relative_url}"
                else: url = relative_url
                    
                categories_found.append({"category_name": category_name, "url": url})
        
        print(f"   -> {len(categories_found)} thèmes trouvés.")
        return categories_found

    except Exception as e:
        print(f"   -> ERREUR (discover_all_categories) : {e}")
        return []

def save_categories_to_staging(cursor, categories_to_save, univers_name):
    """
    Enregistre la liste des thèmes dans la table Staging_Category_Queue.
    """
    print(f"--- (0a) Chargement des thèmes dans Staging_Category_Queue ---")
    insert_count = 0
    update_count = 0
    
    for cat in categories_to_save:
        cursor.execute("SELECT CategoryQueueID FROM Staging_Category_Queue WHERE CategoryURL = ?", (cat['url']))
        existing_task = cursor.fetchone()
        
        if existing_task is None:
            cursor.execute(
                "INSERT INTO Staging_Category_Queue (CategoryName, CategoryURL, ParentCategoryName, Status) VALUES (?, ?, ?, 'pending')",
                (cat['category_name'], cat['url'], univers_name)
            )
            insert_count += 1
        else:
            cursor.execute(
                "UPDATE Staging_Category_Queue SET Status = 'pending', LastAttempt = NULL, CategoryName = ?, ParentCategoryName = ? WHERE CategoryQueueID = ?",
                (cat['category_name'], univers_name, existing_task.CategoryQueueID)
            )
            update_count += 1
            
    print(f"   -> {insert_count} nouveaux thèmes insérés.")
    print(f"   -> {update_count} thèmes réactivés.")

# --- 3. Fonctions de l'Étape 0b (Découverte des Sous-Catégories) ---

def discover_all_subcategories(parent_category_name, hub_url):
    """
    Scrappe une page "thème" (ex: /mp3-casque-ecouteurs) pour trouver
    UNIQUEMENT les sous-catégories "families" (ex: "Casques audio (174)").
    """
    BASE_URL = "https://www.vandenborre.be"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

    print(f"--- (0b) Scraping de : {hub_url} (Recherche : '{parent_category_name}')")
    
    subcategories_found = []
    
    try:
        session = requests.Session()
        session.headers.update(headers)
        response = session.get(hub_url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Cible UNIQUEMENT la section "families"
        main_container = soup.find('div', {'class': 'rubric-families'})
        if not main_container:
            print(f"   -> ⚠️ Aucun conteneur 'rubric-families' trouvé sur {hub_url}.")
            return []

        name_count_regex = re.compile(r"(.*)\((\d+)\)")
        category_blocks = main_container.find_all('div', {'class': 'accordion-image-grid'})
        
        for block in category_blocks:
            link_tag = block.find('a', {'class': 'rubric-link'})
            if not link_tag: continue

            relative_url = link_tag.get('href')
            title_tag = link_tag.find(['h2', 'h3'])
            if not title_tag or not relative_url: continue

            full_text = title_tag.text.strip().replace('\xa0', ' ')
            
            url = ""
            if relative_url.startswith('//'): url = f"https:{relative_url}"
            elif relative_url.startswith('/'): url = f"{BASE_URL}{relative_url}"
            else: url = relative_url
                
            category_name = "N/A"
            item_count = 0
            
            match = name_count_regex.search(full_text)
            if match:
                category_name = match.group(1).strip()
                item_count = int(match.group(2))
            else:
                category_name = full_text
            
            if category_name != "N/A":
                subcategories_found.append({
                    "parent_category": parent_category_name,
                    "category_name": category_name,
                    "item_count": item_count,
                    "url": url
                })

        print(f"   -> {len(subcategories_found)} sous-catégories 'families' trouvées.")
        return subcategories_found

    except Exception as e:
        print(f"   -> ERREUR (discover_all_subcategories) : {e}")
        return []

def save_subcategories_to_staging(cursor, subcategories_to_save, parent_category_id):
    """
    Enregistre la liste des sous-catégories dans la table Staging_SubCategory_Queue.
    Ne "vole" pas les catégories d'un autre parent.
    """
    print(f"--- (0b) Chargement des sous-catégories dans Staging_SubCategory_Queue ---")
    insert_count = 0
    update_count = 0
    ignored_count = 0

    for sub_cat in subcategories_to_save:
        cursor.execute("SELECT SubCategoryQueueID, ParentCategoryQueueID FROM Staging_SubCategory_Queue WHERE SubCategoryURL = ?", (sub_cat['url']))
        existing_sub_task = cursor.fetchone()
        
        if existing_sub_task is None:
            # Cas 1: NOUVELLE sous-catégorie. On l'insère.
            cursor.execute(
                "INSERT INTO Staging_SubCategory_Queue (ParentCategoryQueueID, SubCategoryName, SubCategoryURL, ItemCount, Status) VALUES (?, ?, ?, ?, 'pending')",
                (parent_category_id, sub_cat['category_name'], sub_cat['url'], sub_cat['item_count'])
            )
            insert_count += 1
        elif existing_sub_task.ParentCategoryQueueID == parent_category_id:
            # Cas 2: On est le bon parent -> Mettre à jour
            cursor.execute(
                "UPDATE Staging_SubCategory_Queue SET Status = 'pending', LastAttempt = NULL, SubCategoryName = ?, ItemCount = ? WHERE SubCategoryQueueID = ?",
                (sub_cat['category_name'], sub_cat['item_count'], existing_sub_task.SubCategoryQueueID)
            )
            update_count += 1
        else:
            # Cas 3: La sous-catégorie existe mais appartient à un AUTRE parent. On ignore.
            ignored_count += 1
            pass
            
    print(f"   -> {insert_count} nouvelles sous-catégories insérées.")
    print(f"   -> {update_count} sous-catégories mises à jour.")
    if ignored_count > 0:
        print(f"   -> {ignored_count} sous-catégories ignorées (appartiennent à un autre parent).")

# --- 4. Fonctions de l'Étape 1 (Découverte des Produits) ---

def discover_all_products(subcategory_name, category_url, expected_item_count):
    """
    Scrappe TOUTES les pages d'une sous-catégorie (ex: "Casques audio")
    en s'arrêtant lorsque le 'expected_item_count' est atteint.
    """
    BASE_URL = "https://www.vandenborre.be"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    all_products_found = []
    session = requests.Session()
    session.headers.update(headers)
    
    print(f"--- (1) Scraping de '{subcategory_name}' (Objectif: {expected_item_count} produits)")

    try:
        response_page_1 = session.get(category_url, timeout=15)
        response_page_1.raise_for_status()
        soup_page_1 = BeautifulSoup(response_page_1.content, 'html.parser')
        
        count_per_page_select = soup_page_1.find('select', {'name': 'COUNTPERPAGE'})
        count_per_page = 24
        if count_per_page_select:
             selected_option = count_per_page_select.find('option', {'selected': True})
             if selected_option:
                count_per_page = int(selected_option.get('value', 24))
        
        if expected_item_count == 0:
             print(f"   -> {subcategory_name} n'a aucun article. Arrêt.")
             return []

        total_pages = math.ceil(expected_item_count / count_per_page)
        print(f"   -> {expected_item_count} produits sur {total_pages} pages.")

    except Exception as e:
        print(f"   -> ERREUR (discover_all_products - Page 1) : {e}")
        return []

    try:
        for page_num in range(1, total_pages + 1):
            if len(all_products_found) >= expected_item_count:
                print(f"   -> Limite de {expected_item_count} produits atteinte. Arrêt.")
                break 
            
            print(f"   -> Scraping de la Page {page_num}/{total_pages}...")
            
            if page_num == 1:
                soup = soup_page_1 
            else:
                url_to_scrape = f"{category_url}?page={page_num}"
                response = session.get(url_to_scrape, timeout=15)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
            
            product_containers = soup.find_all('div', {'class': 'js-product-container'})
            if not product_containers:
                print(f"   ->  Page {page_num} vide. On continue...")
                continue

            products_on_this_page = 0
            for container in product_containers:
                if len(all_products_found) >= expected_item_count:
                    break 

                sku = container.get('data-productid')
                if not sku: continue 

                link_tag = container.find('a', {'class': 'js-product-click'})
                if not link_tag or not link_tag.get('href'): continue 

                relative_url = link_tag['href']
                url = ""
                if relative_url.startswith('//'): url = f"https:{relative_url}"
                elif relative_url.startswith('/'): url = f"{BASE_URL}{relative_url}"
                else: url = relative_url
                
                all_products_found.append({"sku": sku, "url": url})
                products_on_this_page += 1
            
            print(f"      -> {products_on_this_page} produits extraits.")
            
            if page_num < total_pages and len(all_products_found) < expected_item_count:
                 time.sleep(2) # Pause
        
        print(f"   -> Scraping terminé. {len(all_products_found)} produits découverts.")
        return all_products_found

    except Exception as e:
        print(f"   -> ERREUR (discover_all_products - Boucle) : {e}")
        return all_products_found 

def save_products_to_staging(cursor, products_to_save, sub_category_id):
    """
    Enregistre la liste des produits dans la table Staging_Product_Queue.
    """
    print(f"--- (1) Chargement des produits dans Staging_Product_Queue ---")
    insert_count = 0
    update_count = 0

    for prod in products_to_save:
        cursor.execute("SELECT ProductQueueID FROM Staging_Product_Queue WHERE ProductID_SKU = ?", (prod['sku']))
        existing_prod_task = cursor.fetchone()
        
        if existing_prod_task is None:
            cursor.execute(
                "INSERT INTO Staging_Product_Queue (SubCategoryQueueID, ProductID_SKU, ProductURL, Status) VALUES (?, ?, ?, 'pending')",
                (sub_category_id, prod['sku'], prod['url'])
            )
            insert_count += 1
        else:
            cursor.execute(
                "UPDATE Staging_Product_Queue SET Status = 'pending', LastAttempt = NULL, ProductURL = ?, SubCategoryQueueID = ? WHERE ProductQueueID = ?",
                (prod['url'], sub_category_id, existing_prod_task.ProductQueueID)
            )
            update_count += 1
            
    print(f"   -> {insert_count} nouveaux produits insérés.")
    print(f"   -> {update_count} produits existants réactivés.")

# --- 5. Fonctions de l'Étape 2 (Scraping des Produits) ---

def scrape_product_page(product_url):
    """
    Visite la page d'un produit spécifique et extrait son JSON-LD caché.
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    
    print(f"   -> (2) Scraping de : {product_url}...")
    try:
        session = requests.Session()
        session.headers.update(headers)
        response = session.get(product_url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        json_scripts = soup.find_all('script', {'type': 'application/ld+json'})
        
        for script in json_scripts:
            if not script.string: continue
            
            clean_json_string = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', script.string)
            data = json.loads(clean_json_string)
            
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("@type") == "Product":
                        return item # Trouvé
            elif isinstance(data, dict) and data.get("@type") == "Product":
                return data # Trouvé
                
        print(f"   -> ⚠️ JSON-LD '@type':'Product' non trouvé.")
        return None

    except Exception as e:
        print(f"   ->  ERREUR (scrape_product_page) : {e}")
        return None

def save_product_json_to_staging(cursor, sku, json_data, queue_id_ref):
    """
    Enregistre le JSON brut du produit dans la table Staging_VDB_Product_Page.
    """
    print(f"   -> (2) Chargement du JSON pour SKU {sku}...")
    try:
        raw_json_string = json.dumps(json_data, ensure_ascii=False)
        cursor.execute(
            "INSERT INTO Staging_VDB_Product_Page (ProductID_SKU, Raw_JSON_LD, QueueID_Ref) VALUES (?, ?, ?)",
            (sku, raw_json_string, queue_id_ref)
        )
    except Exception as e:
        print(f"   -> ERREUR (save_product_json_to_staging) : {e}")
        raise # Propage l'erreur pour forcer un rollback

# --- 6. Fonctions de l'Étape 3 (Transformation JSON -> Colonnes) ---

def get_pending_raw_json(cursor):
    """
    Récupère toutes les lignes "pending" de la table JSON brute.
    """
    print("   -> (3) Extraction des JSON bruts (Status='pending')...")
    cursor.execute("""
        SELECT StagingVDBKey, ProductID_SKU, Raw_JSON_LD, ScrapeTimestamp
        FROM Staging_VDB_Product_Page
        WHERE Status = 'pending'
    """)
    columns = [column[0] for column in cursor.description]
    tasks = [dict(zip(columns, row)) for row in cursor.fetchall()]
    print(f"   -> {len(tasks)} JSON bruts trouvés à transformer.")
    return tasks

def parse_and_clean_json(raw_json_string):
    """
    Prend le JSON brut et le transforme en un dictionnaire Python propre.
    """
    data = json.loads(raw_json_string)
    cleaned_data = {}
    cleaned_data['sku'] = data.get('sku')
    # --- MODIFICATION ICI ---
    product_name = data.get('name')
    if product_name:
        # Utilise regex pour supprimer " (YYYY)" ou "(YYYY)" à la fin du nom
        # Ex: "SONY ... (2021)" -> "SONY ..."
        product_name = re.sub(r'\s*\(\d{4}\)$', '', product_name).strip()
    
    cleaned_data['name'] = product_name
    # --- FIN DE LA MODIFICATION ---
    cleaned_data['brand'] = data.get('brand', {}).get('name')
    cleaned_data['category'] = data.get('category')
    
    offer = data.get('offers', {})
    rating = data.get('aggregateRating', {})
    
    cleaned_data['price'] = offer.get('price')
    cleaned_data['availability'] = offer.get('availability')
    cleaned_data['rating'] = rating.get('ratingValue')
    cleaned_data['review_count'] = rating.get('reviewCount')
    
    try:
        if cleaned_data['price'] is not None:
            cleaned_data['price'] = float(str(cleaned_data['price']).replace(',', '.'))
        if cleaned_data['rating'] is not None:
            cleaned_data['rating'] = float(str(cleaned_data['rating']).replace(',', '.'))
        if cleaned_data['review_count'] is not None:
            cleaned_data['review_count'] = int(cleaned_data['review_count'])
    except (ValueError, TypeError):
        sku = cleaned_data.get('sku', 'SKU inconnu')
        print(f"   ->  Erreur conversion type pour SKU {sku}. Valeurs mises à NULL.")
        cleaned_data['price'] = None 
        cleaned_data['rating'] = None
        cleaned_data['review_count'] = None
        
    return cleaned_data

def save_cleansed_data(cursor, staging_vdb_key, scrape_timestamp, cleaned_data):
    """
    Charge les données nettoyées dans la table Staging_Product_Cleansed.
    """
    print(f"   -> (3) Chargement des données nettoyées pour SKU {cleaned_data['sku']}...")
    cursor.execute(
        """
        INSERT INTO Staging_Product_Cleansed 
            (StagingVDBKey_Ref, SKU, ProductName, Brand, Category, 
             Price, Average_Rating, Review_Count, Availability, 
             ScrapeTimestamp, Status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
        """,
        (
            staging_vdb_key,
            cleaned_data['sku'],
            cleaned_data['name'],
            cleaned_data['brand'],
            cleaned_data['category'],
            cleaned_data['price'],
            cleaned_data['rating'],
            cleaned_data['review_count'],
            cleaned_data['availability'],
            scrape_timestamp
        )
    )

def update_raw_status(cursor, staging_vdb_key, status):
    """
    Met à jour le statut de la table brute Staging_VDB_Product_Page.
    """
    print(f"   -> (3) MàJ statut table brute (ID: {staging_vdb_key}) à '{status}'.")
    cursor.execute(
        "UPDATE Staging_VDB_Product_Page SET Status = ? WHERE StagingVDBKey = ?",
        (status, staging_vdb_key)
    )