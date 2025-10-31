import sys
import os
from datetime import datetime
import importlib

# --- 1. Configuration du chemin ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# --- 2. Imports Airflow ---
from airflow.decorators import dag, task

# --- 3. Import du Moteur de Sentiment ---
try:
    from scripts import sentiment_engine_roberta as se_roberta
except ImportError:
    print("ERREUR : Impossible d'importer 'sentiment_engine_roberta' depuis 'scripts/'.")
    print("Vérifiez que le fichier existe et que 'torch'/'transformers' sont dans le Dockerfile.")
    raise

# --- 4. Définition de la Tâche d'Analyse ---

@task(task_id="run_sentiment_analysis_roberta")
def task_run_sentiment_analysis_roberta():
    """
    Tâche monolithique (Étape 7) qui utilise RoBERTa.
    C'est une tâche LOURDE qui peut prendre du temps.
    """
    print("--- DÉBUT : Pipeline d'Analyse de Sentiment (Étape 7 - RoBERTa) ---")
    importlib.reload(se_roberta)
    
    conn_read, cursor_read = None, None
    tasks = []
    
    # 1. Charger le modèle (LOURD - se fait une seule fois)
    # Cela peut prendre plusieurs minutes au premier démarrage de la tâche
    tokenizer, model = se_roberta.load_roberta_model()
    
    # --- 2. Extraire la liste des tâches ---
    try:
        conn_read = se_roberta.get_staging_connection()
        cursor_read = conn_read.cursor()
        tasks = se_roberta.get_pending_posts(cursor_read)
    except Exception as e_init:
        print(f"--- ❌ ERREUR MAJEURE lors de la lecture des posts : {e_init} ---")
        raise
    finally:
        if cursor_read: cursor_read.close()
        if conn_read: conn_read.close()
    
    if not tasks:
        print("\n--- ✅ Fin : Aucun post (filtré) à analyser. Tâche terminée. ---")
        return
        
    print(f"\n--- {len(tasks)} posts à analyser. Lancement de la boucle... ---")

    # --- 3. Boucle de (T) et (L) (robuste) ---
    for i, task in enumerate(tasks):
        post_id = task['StagingPostID']
        conn_write, cursor_write = None, None
        
        try:
            print(f"\n--- Traitement Post {i+1}/{len(tasks)} (ID: {post_id}) ---")
            
            # (T) Concaténer et Analyser
            text_to_analyze = (task['PostTitle'] or "") + " " + (task['PostText'] or "")
            compound_score, sentiment_name = se_roberta.analyze_sentiment_roberta(text_to_analyze, tokenizer, model)
            print(f"   -> Score: {compound_score:.4f} ({sentiment_name})")
            
            # --- Transaction d'écriture ---
            conn_write = se_roberta.get_staging_connection()
            cursor_write = conn_write.cursor()
            
            # (L) Charger le résultat
            # <-- CORRIGÉ : On enlève 'sentiment_name' qui était le 4ème argument en trop
            se_roberta.save_sentiment_to_staging(cursor_write, task, compound_score)
            
            # (L) Mettre à jour le statut du post source
            se_roberta.update_post_status(cursor_write, post_id, 'processed')
            
            conn_write.commit()
            print(f"   -> ✅ Post {post_id} marqué comme 'processed'.")

        except Exception as e:
            print(f"   -> ❌ ERREUR sur Post {post_id}: {e}.")
            if conn_write: conn_write.rollback()
            
            # Marquer comme 'failed' (dans une transaction séparée)
            conn_fail, cursor_fail = None, None
            try:
                conn_fail = se_roberta.get_staging_connection()
                cursor_fail = conn_fail.cursor()
                se_roberta.update_post_status(cursor_fail, post_id, 'failed')
                conn_fail.commit()
            finally:
                if cursor_fail: cursor_fail.close()
                if conn_fail: conn_fail.close()
                
        finally:
            if cursor_write: cursor_write.close()
            if conn_write: conn_write.close()

    print("\n--- ✅ Boucle d'analyse de sentiment (RoBERTa) terminée. ---")


# --- 5. Définition du DAG ---
@dag(
    dag_id="sentiment_analysis_roberta_pipeline",
    start_date=datetime(2025, 10, 29),
    schedule=None, # Manuel pour l'instant
    catchup=False,
    tags=['sentimentanalysis']
)
def sentiment_analysis_dag_roberta():
    """
    Orchestre l'analyse de sentiment LOURDE (Étape 7)
    avec RoBERTa.
    """
    task_run_sentiment_analysis_roberta()

# --- 6. Instanciation du DAG ---
sentiment_analysis_dag_roberta()