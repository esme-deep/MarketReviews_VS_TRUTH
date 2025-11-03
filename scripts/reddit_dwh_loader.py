import pyodbc
from datetime import datetime

# --- 1. Connexions ---

def get_dwh_connection():
    """Crée une connexion au DWH (mode transaction)."""
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"    
        "SERVER=host.docker.internal,1433;"       
        "DATABASE=Projet_Market_DWH;"             
        "UID=airflow;" "PWD=airflow;"                            
        "Encrypt=yes;" "TrustServerCertificate=yes;"             
    )
    return pyodbc.connect(conn_str, autocommit=False)

def get_staging_connection():
    """Crée une connexion au Staging (mode transaction)."""
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=host.docker.internal,1433;"       
        "DATABASE=Projet_Market_Staging;"         
        "UID=airflow;" "PWD=airflow;"                            
        "Encrypt=yes;" "TrustServerCertificate=yes;"             
    )
    return pyodbc.connect(conn_str, autocommit=False)

# --- 2. Fonctions pour Dim_Reddit_User ---

def get_processed_authors_from_staging(staging_cursor):
    """
    (E) Récupère les auteurs UNIQUES et ENRICHIS depuis Staging_Reddit_Posts.
    On ne prend que ceux que PRAW a vérifiés.
    """
    print("   -> (E) Lecture des auteurs 'Processed' depuis Staging_Posts...")
    query = """
    SELECT DISTINCT 
        RedditorID, 
        AuthorName, 
        AuthorCreationDate, 
        CommentKarma, 
        PostKarma, 
        HasVerifiedEmail
    FROM 
        Projet_Market_Staging.dbo.Staging_Reddit_Posts
    WHERE 
        AuthorCheckStatus = 'Processed' 
        AND RedditorID IS NOT NULL
    """
    staging_cursor.execute(query)
    columns = [column[0] for column in staging_cursor.description]
    authors = [dict(zip(columns, row)) for row in staging_cursor.fetchall()]
    print(f"   -> {len(authors)} auteurs uniques (enrichis) trouvés.")
    return authors

def get_existing_users_from_dwh(dwh_cursor):
    """
    (L) Lit la Dim_Reddit_User pour créer un "lookup map" (RedditorID -> UserKey).
    Permet de savoir qui mettre à jour (UPDATE) et qui insérer (INSERT).
    """
    print("   -> (L) Lecture des utilisateurs existants dans Dim_Reddit_User...")
    dwh_cursor.execute("SELECT RedditorID, UserKey FROM dbo.Dim_Reddit_User")
    # Crée un dictionnaire : {'t2_abc': 1, 't2_xyz': 2, ...}
    user_map = {row.RedditorID: row.UserKey for row in dwh_cursor.fetchall()}
    print(f"   -> {len(user_map)} utilisateurs déjà présents dans le DWH.")
    return user_map

def upsert_dim_user(dwh_cursor, author_data, existing_user_key):
    """
    (T+L) Gère la logique SCD Type 1 (UPSERT).
    """
    author_id = author_data['RedditorID']
    
    if existing_user_key:
        # Cas 1: UPDATE (SCD Type 1 - Écrasement)
        print(f"   -> (T/L) UPDATE Utilisateur (SCD 1) : {author_id} (Key={existing_user_key})")
        sql = """
        UPDATE Dim_Reddit_User
        SET 
            AuthorName = ?, 
            AccountCreationDate = ?, 
            CommentKarma = ?, 
            PostKarma = ?, 
            HasVerifiedEmail = ?,
            LastUpdatedAt = GETDATE()
        WHERE 
            UserKey = ?
        """
        dwh_cursor.execute(sql, (
            author_data['AuthorName'],
            author_data['AuthorCreationDate'],
            author_data['CommentKarma'],
            author_data['PostKarma'],
            author_data['HasVerifiedEmail'],
            existing_user_key
        ))
    else:
        # Cas 2: INSERT (Nouvel utilisateur)
        print(f"   -> (T/L) INSERT Utilisateur : {author_id}")
        sql = """
        INSERT INTO Dim_Reddit_User
            (RedditorID, AuthorName, AccountCreationDate, CommentKarma, 
             PostKarma, HasVerifiedEmail, FirstSeenAt, LastUpdatedAt)
        VALUES (?, ?, ?, ?, ?, ?, GETDATE(), GETDATE())
        """
        dwh_cursor.execute(sql, (
            author_data['RedditorID'],
            author_data['AuthorName'],
            author_data['AuthorCreationDate'],
            author_data['CommentKarma'],
            author_data['PostKarma'],
            author_data['HasVerifiedEmail']
        ))

# --- 3. Fonctions pour Fact_Public_Sentiment ---

def get_pending_sentiments(staging_cursor):
    """
    (E) Récupère les scores 'pending_migration' ET leur 'RedditorID'.
    C'est la jointure cruciale entre les deux tables de staging.
    """
    print("   -> (E) Lecture des sentiments 'pending_migration' (JOIN Staging_Posts)...")
    query = """
    SELECT 
        s.SentimentID,
        s.StagingPostID_Ref,
        s.ProductKey_Ref,
        s.PostDate,
        s.Sentiment_Score,
        p.RedditorID -- La clé pour la jointure DWH !
    FROM 
        Projet_Market_Staging.dbo.Staging_Reddit_Sentiment AS s
    JOIN 
        Projet_Market_Staging.dbo.Staging_Reddit_Posts AS p 
        ON s.StagingPostID_Ref = p.StagingPostID
    WHERE 
        s.Status = 'pending_migration'
        AND p.RedditorID IS NOT NULL -- On ne migre que si on a un auteur
    """
    staging_cursor.execute(query)
    columns = [column[0] for column in staging_cursor.description]
    sentiments = [dict(zip(columns, row)) for row in staging_cursor.fetchall()]
    print(f"   -> {len(sentiments)} faits de sentiment trouvés à migrer.")
    return sentiments

def insert_fact_sentiment(dwh_cursor, sentiment_data, user_key, date_key):
    """
    (L) Charge un enregistrement unique dans la table de faits.
    """
    dwh_cursor.execute(
        """
        INSERT INTO Fact_Public_Sentiment
            (ProductKey, UserKey, DateKey, Sentiment_Score, StagingPostID_Ref)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            sentiment_data['ProductKey_Ref'],
            user_key,
            date_key,
            sentiment_data['Sentiment_Score'],
            sentiment_data['StagingPostID_Ref']
        )
    )

def update_sentiment_status(staging_cursor, sentiment_id, status):
    """
    (L) Met à jour le statut dans Staging_Reddit_Sentiment
    pour garantir l'idempotence.
    """
    staging_cursor.execute(
        "UPDATE Staging_Reddit_Sentiment SET Status = ? WHERE SentimentID = ?",
        (status, sentiment_id)
    )