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
            -- 1. On prend tous les posts qui n'ont pas encore été analysés
            r.Status = 'pending_sentiment';
    """
    
    staging_cursor.execute(query)
    columns = [column[0] for column in staging_cursor.description]
    tasks = [dict(zip(columns, row)) for row in staging_cursor.fetchall()]
    print(f"   -> {len(tasks)} posts (filtrés) trouvés à analyser.")
    return tasks

def analyze_sentiment_roberta(text, tokenizer, model):
    """
    Analyse le texte avec RoBERTa.
    Retourne (score, nom) ou (None, 'Skipped_Too_Long') si le texte dépasse 512 tokens.
    """
    if not text or text.strip() == "":
        return 0.0, 'Neutre'
        
    # <-- CORRIGÉ : On vérifie la longueur SANS couper (truncation=False)
    # On ne garde que les 'input_ids' pour vérifier la longueur
    encoded_check = tokenizer(text, truncation=False, return_tensors=None)
    
    token_count = len(encoded_check['input_ids'])
    
    # <-- NOUVELLE LOGIQUE : Ignorer si trop long
    if token_count > 512:
        print(f"   -> AVERTISSEMENT: Texte trop long ({token_count} tokens). Ignoré.")
        return None, 'Skipped_Too_Long'
        
    # Si le texte est OK (<= 512), on l'encode pour le modèle
    encoded_input = tokenizer(text, return_tensors='pt', truncation=True, max_length=512)
    
    output = model(**encoded_input)
    scores = output[0][0].detach().numpy()
    scores = softmax(scores)
    
    compound_score = scores[2] - scores[0] 
    
    if compound_score > 0.1:
        sentiment_name = 'Positif'
    elif compound_score < -0.1:
        sentiment_name = 'Négatif'
    else:
        sentiment_name = 'Neutre'
        
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
            sentiment_score
        )
    )

def update_post_status(staging_cursor, post_id, status):
    """Met à jour le statut du post source."""
    staging_cursor.execute(
        "UPDATE Staging_Reddit_Posts SET Status = ? WHERE StagingPostID = ?",
        (status, post_id)
    )