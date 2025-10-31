import pyodbc
from datetime import datetime
from transformers import AutoModelForSequenceClassification, AutoTokenizer, AutoConfig
import numpy as np
from scipy.special import softmax

# --- 1. Configuration & Connexion (Staging) ---
def get_staging_connection():
    """Crée et retourne une connexion à la base de données Staging."""
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=host.docker.internal,1433;"
        "DATABASE=Projet_Market_Staging;"
        "UID=airflow;" "PWD=airflow;"
        "Encrypt=yes;" "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=False)

# --- 2. Fonctions du Moteur de Sentiment ---

def load_roberta_model():
    """
    Charge le modèle et le tokenizer.
    S'exécute une SEULE fois au début du DAG.
    """
    print("--- Chargement du modèle RoBERTa (cardiffnlp)... ---")
    MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment"
    # S'assure qu'il télécharge dans un dossier accessible par le worker
    cache_dir = "/tmp/huggingface_cache" 
    
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, cache_dir=cache_dir)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, cache_dir=cache_dir)
    print("--- Modèle chargé avec succès. ---")
    return tokenizer, model

def get_pending_posts(staging_cursor):
    """
    Récupère les posts 'pending' filtrés (liés aux produits VDB avec avis)
    """
    print("   -> (E) Extraction des posts 'pending_sentiment' (filtrés)...")
    
    query = """
        SELECT
            r.StagingPostID, r.PostTitle, r.PostText,
            r.ProductKey_Ref, r.AuthorName, r.PostDate
        FROM
            [Projet_Market_Staging].[dbo].[Staging_Reddit_Posts] AS r
        WHERE
            -- 1. Le post n'a pas encore été analysé
            r.Status = 'pending_sentiment'
            
            -- 2. ET il existe une entrée correspondante dans le DWH
            --    qui a suffisamment d'avis
            AND EXISTS (
                SELECT 1
                FROM
                    [Projet_Market_DWH].[dbo].[Dim_Product] AS p
                JOIN
                    [Projet_Market_DWH].[dbo].[Fact_Marketplace_Snapshot] AS f
                    ON p.ProductKey = f.ProductKey
                WHERE
                    p.ProductKey = r.ProductKey_Ref -- Le lien
                    AND f.Average_Rating IS NOT NULL
                    AND f.Review_Count >= 3 -- NOTRE SEUIL
            );
    """
    
    staging_cursor.execute(query)
    columns = [column[0] for column in staging_cursor.description]
    tasks = [dict(zip(columns, row)) for row in staging_cursor.fetchall()]
    print(f"   -> {len(tasks)} posts (filtrés) trouvés à analyser.")
    return tasks

def analyze_sentiment_roberta(text, tokenizer, model):
    """
    Analyse le texte avec RoBERTa et retourne le score (-1 à +1) et le nom.
    """
    if not text or text.strip() == "":
        return 0.0, 'Neutre'
        
    text_short = text[:512] 
    
    # <-- CORRIGÉ 1 : Ajout de max_length=512 pour supprimer le warning 'Asking to truncate'
    encoded_input = tokenizer(text_short, return_tensors='pt', truncation=True, max_length=512)
    output = model(**encoded_input)
    scores = output[0][0].detach().numpy()
    scores = softmax(scores)
    
    # score[0]=Négatif, score[1]=Neutre, score[2]=Positif
    compound_score = scores[2] - scores[0] 
    
    if compound_score > 0.1:
        sentiment_name = 'Positif'
    elif compound_score < -0.1:
        sentiment_name = 'Négatif'
    else:
        sentiment_name = 'Neutre'
        
    # <-- CORRIGÉ 2 (LE PLUS IMPORTANT) :
    # On convertit le numpy.float32 en float Python standard
    # que pyodbc peut comprendre.
    return float(compound_score), sentiment_name

def save_sentiment_to_staging(staging_cursor, task_data, sentiment_score):
    """
    Enregistre le résultat dans Staging_Reddit_Sentiment (votre schéma dénormalisé)
    """
    staging_cursor.execute(
        """
        INSERT INTO Staging_Reddit_Sentiment
            (StagingPostID_Ref, ProductKey_Ref, AuthorName, PostDate, 
             Sentiment_Score, ProcessedAt, Status)
        VALUES (?, ?, ?, ?, ?, GETDATE(), 'pending_migration')
        """,
        (
            task_data['StagingPostID'],
            task_data['ProductKey_Ref'],
            task_data['AuthorName'],
            task_data['PostDate'],
            sentiment_score # <-- Ce sera maintenant un 'float' standard
        )
    )

def update_post_status(staging_cursor, post_id, status):
    """Met à jour le statut du post source."""
    staging_cursor.execute(
        "UPDATE Staging_Reddit_Posts SET Status = ? WHERE StagingPostID = ?",
        (status, post_id)
    )