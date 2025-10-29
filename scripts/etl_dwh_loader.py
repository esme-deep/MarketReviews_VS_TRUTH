import pyodbc
from datetime import datetime

# --- 1. Configuration & Connexion ---
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


# --- 2. Fonction d'Extraction (Depuis Staging) ---

def get_pending_cleansed_data(staging_cursor):
    """
    Récupère toutes les lignes "pending" de la table Staging nettoyée.
    """
    print("   -> (E) Extraction des données de Staging_Product_Cleansed (Status='pending')...")
    
    # On s'assure de prendre le ScrapeTimestamp, qui est vital
    staging_cursor.execute("""
        SELECT 
            CleansedProductID, SKU, ProductName, Brand, Category,
            Price, Average_Rating, Review_Count, Availability, ScrapeTimestamp
        FROM Staging_Product_Cleansed
        WHERE Status = 'pending'
    """)
    
    # Convertit les résultats en liste de dictionnaires pour une manipulation facile
    columns = [column[0] for column in staging_cursor.description]
    tasks = [dict(zip(columns, row)) for row in staging_cursor.fetchall()]
    print(f"   -> {len(tasks)} produits trouvés à migrer.")
    return tasks

# --- 3. Fonctions de Transformation & Chargement (Vers DWH) ---

def get_date_time_keys(scrape_timestamp):
    """Convertit un datetime en DateKey et TimeKey."""
    if scrape_timestamp is None:
        raise ValueError("ScrapeTimestamp ne peut pas être NULL pour charger les faits")
    
    date_key = int(scrape_timestamp.strftime('%Y%m%d'))
    # Clé au format HHMMSS, arrondie à la minute (correspond à Dim_Time)
    time_key = int(scrape_timestamp.strftime('%H%M') + '00') 
    return date_key, time_key

def load_dim_product_scd1(dwh_cursor, task_data):
    """
    Gère la logique "UPSERT" (SCD Type 1) pour la Dim_Product.
    - Si le produit n'existe pas, il l'insère.
    - S'il existe, il l'écrase avec les nouvelles valeurs.
    Retourne le ProductKey (nouveau ou existant).
    """
    sku = task_data['SKU']
    
    # 1. Vérifier si le produit existe
    dwh_cursor.execute("SELECT ProductKey FROM Dim_Product WHERE ProductID_SKU = ?", (sku))
    row = dwh_cursor.fetchone()
    
    if row:
        # Cas 1: UPDATE (Écrasement)
        product_key = row.ProductKey
        print(f"   -> (T) SKU {sku} existe (Key={product_key}). Mise à jour (SCD Type 1)...")
        dwh_cursor.execute(
            """
            UPDATE Dim_Product
            SET ProductName = ?, Brand = ?, Category = ?
            WHERE ProductKey = ?
            """,
            (task_data['ProductName'], task_data['Brand'], task_data['Category'], product_key)
        )
    else:
        # Cas 2: INSERT (Nouveau produit)
        print(f"   -> (T) SKU {sku} est nouveau. Insertion dans Dim_Product...")
        dwh_cursor.execute(
            """
            INSERT INTO Dim_Product (ProductID_SKU, ProductName, Brand, Category)
            VALUES (?, ?, ?, ?)
            """,
            (sku, task_data['ProductName'], task_data['Brand'], task_data['Category'])
        )
        # Récupérer la clé qu'on vient de créer
        product_key = dwh_cursor.execute("SELECT @@IDENTITY AS ID;").fetchone()[0]
        
    return product_key

def load_fact_snapshot(dwh_cursor, task_data, product_key, date_key, time_key):
    """
    Charge une nouvelle ligne "snapshot" dans la table de faits.
    Accepte les NULLs pour les notes.
    """
    print(f"   -> (L) Chargement dans Fact_Marketplace_Snapshot...")
    
    # Gérer la dispo (booléen)
    is_available = 1 if "InStock" in (task_data['Availability'] or "") else 0
    
    dwh_cursor.execute(
        """
        INSERT INTO Fact_Marketplace_Snapshot 
            (DateKey, TimeKey, ProductKey, Price, Average_Rating, Review_Count, Is_Available)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            date_key, 
            time_key, 
            product_key, 
            task_data['Price'], 
            task_data['Average_Rating'], # Insère le NULL tel quel
            task_data['Review_Count'],   # Insère le NULL tel quel
            is_available
        )
    )

# --- 4. Fonction de Mise à Jour (du Staging) ---

def update_staging_status(staging_cursor, cleansed_product_id, status):
    """
    Met à jour le statut de la ligne traitée dans Staging_Product_Cleansed.
    """
    print(f"   -> Mise à jour du statut Staging (ID: {cleansed_product_id}) à '{status}'.")
    staging_cursor.execute(
        "UPDATE Staging_Product_Cleansed SET Status = ? WHERE CleansedProductID = ?",
        (status, cleansed_product_id)
    )