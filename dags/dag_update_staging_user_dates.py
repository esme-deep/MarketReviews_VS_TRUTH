import sys
import os
from datetime import datetime
import time

# --- Imports pour le DAG (Airflow, BDD, API) ---
from airflow.decorators import dag, task
import pyodbc
import praw  # C'est l'API Reddit

# --- 1. Configuration du chemin (au cas où, bonne pratique) ---
# PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# if PROJECT_ROOT not in sys.path:
#     sys.path.append(PROJECT_ROOT)

# -------------------------------------------------------------------
# --- LOGIQUE "MOTEUR" (Intégrée directement dans le fichier DAG) ---
# -------------------------------------------------------------------

def get_staging_connection():
    """Crée une connexion à la base de données Staging (en mode transaction)."""
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=host.docker.internal,1433;"
        "DATABASE=Projet_Market_Staging;"
        "UID=airflow;" "PWD=airflow;"
        "Encrypt=yes;" "TrustServerCertificate=yes;"
    )
    # autocommit=False pour gérer les UPDATE dans une transaction
    return pyodbc.connect(conn_str, autocommit=False)

def get_reddit_api():
    """
    Initialise et retourne une instance PRAW (API Reddit).
    !! REMPLISSEZ VOS IDENTIFIANTS ICI !!
    """
    print("   -> Initialisation de l'API PRAW (Reddit)...")
    try:
        reddit = praw.Reddit(
            # ⚠️⚠️⚠️ MODIFIE CES LIGNES ⚠️⚠️⚠️
            client_id="",
            client_secret="",
            user_agent="Airflow:MarketReviewsVSTruth:v1.0 (by /u/)",
            # ⚠️⚠️⚠️ FIN DE LA ZONE À MODIFIER ⚠️⚠️⚠️
            read_only=True
        )
        reddit.user.me() # Teste les identifiants
        print("   -> Connexion PRAW réussie.")
        return reddit
    except Exception as e:
        print(f"   -> ❌ ERREUR PRAW : Impossible de se connecter. Vérifiez les identifiants. {e}")
        raise

def get_pending_authors(cursor):
    """
    (E) Récupère la liste des auteurs uniques qui n'ont pas encore été vérifiés
    (c'est-à-dire où AuthorCheckStatus IS NULL).
    """
    print("   -> (E) Lecture des auteurs à vérifier (AuthorCheckStatus IS NULL)...")
    query = """
    SELECT DISTINCT AuthorName 
    FROM Staging_Reddit_Posts 
    WHERE AuthorCheckStatus IS NULL 
      AND AuthorName IS NOT NULL 
      AND AuthorName != '[deleted]'
    """
    cursor.execute(query)
    authors = [row.AuthorName for row in cursor.fetchall()]
    print(f"   -> {len(authors)} auteurs uniques à vérifier.")
    return authors

def get_user_details(reddit_api, author_name):
    """(T) Appelle PRAW pour trouver les détails d'un utilisateur."""
    try:
        redditor = reddit_api.redditor(author_name)
        
        # Récupère toutes les infos en une fois
        details = {
            "creation_date": datetime.utcfromtimestamp(redditor.created_utc),
            "redditor_id": redditor.id,
            "comment_karma": int(redditor.comment_karma),
            "post_karma": int(redditor.link_karma),
            "has_verified_email": bool(redditor.has_verified_email)
        }
        return details
        
    except Exception as e:
        # Gère les utilisateurs suspendus, supprimés ou invalides
        print(f"   -> ⚠️ Erreur API pour '{author_name}' (ex: suspendu/supprimé): {e}")
        # Retourne un dictionnaire "vide" pour marquer comme "Failed"
        return {
            "creation_date": None,
            "redditor_id": None,
            "comment_karma": None,
            "post_karma": None,
            "has_verified_email": None
        }


def update_staging_posts_for_author(cursor, author_name, details):
    """
    (L) Met à jour TOUS les posts de cet auteur dans la table staging
    avec les détails et le nouveau statut.
    """
    # Si creation_date est None, c'est que l'API a échoué (ex: compte banni)
    status = 'Processed' if details["creation_date"] else 'Failed_API (Suspended/Deleted)'
    
    print(f"   -> (L) MàJ de '{author_name}' -> Status: {status}")
    
    sql = """
    UPDATE Staging_Reddit_Posts  
    SET 
        AuthorCreationDate = ?, 
        AuthorCheckStatus = ?,
        RedditorID = ?,
        CommentKarma = ?,
        PostKarma = ?,
        HasVerifiedEmail = ?
    WHERE 
        AuthorName = ?
    """
    cursor.execute(sql, (
        details["creation_date"],
        status,
        details["redditor_id"],
        details["comment_karma"],
        details["post_karma"],
        details["has_verified_email"],
        author_name
    ))

# -------------------------------------------------------------------
# --- DÉFINITION DE LA TÂCHE AIRFLOW ---
# -------------------------------------------------------------------

@task(task_id="run_user_date_update")
def task_run_user_date_update():
    """
    Tâche ELT qui enrichit la table Staging_Reddit_Posts.
    ...
    """
    print("--- DÉBUT : Pipeline d'enrichissement (Date Création Auteur) ---")
    
    conn, cursor = None, None
    authors_to_process = []
    
    # --- E (Lecture) ---
    try:
        conn = get_staging_connection() 
        cursor = conn.cursor()
        authors_to_process = get_pending_authors(cursor)
    except Exception as e_init:
        print(f"--- ❌ ERREUR MAJEURE lors de la lecture BDD : {e_init} ---")
        if conn: conn.rollback()
        raise
    finally:
        # On garde la connexion ouverte pour la boucle
        pass 
        
    if not authors_to_process:
        print("\n--- ✅ Fin : Aucun nouvel auteur à vérifier. Staging à jour. ---")
        if cursor: cursor.close()
        if conn: conn.close()
        return

    print(f"\n--- {len(authors_to_process)} nouveaux auteurs à traiter. Lancement de la boucle... ---")
    
    # --- T & L (Boucle de Traitement) ---
    try:
        reddit = get_reddit_api()

        for i, author in enumerate(authors_to_process):
            print(f"\n--- Traitement {i+1}/{len(authors_to_process)} (Auteur: {author}) ---")
            
            # (T) Appel API
            user_details = get_user_details(reddit, author)
            
            # (L) Écriture DWH
            update_staging_posts_for_author(cursor, author, user_details)
            
            # -------------------------------------------------
            # --- CORRECTION : COMMIT PAR LOTS (BATCH) ---
            # -------------------------------------------------
            # On sauvegarde la progression tous les 50 auteurs
            if (i + 1) % 50 == 0:
                print(f"   -> 💾 Commit du lot de 50 auteurs ({i+1}/{len(authors_to_process)})...")
                conn.commit()
            # -------------------------------------------------
            
            # Limiteur de débit
            time.sleep(7) 

        # Si tout s'est bien passé, on commit TOUT à la fin
        print("   -> ✅ Commit final de la transaction (pour les auteurs restants).")
        conn.commit()

    except Exception as e_loop:
        print(f"   -> ❌ ERREUR durant la boucle de traitement : {e_loop}.")
        print("   -> ROLLBACK : Annulation du lot en cours.")
        if conn: conn.rollback()
        raise
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

    print("\n--- ✅ Fin : Enrichissement des auteurs terminé. ---")

# -------------------------------------------------------------------
# --- DÉFINITION DU DAG AIRFLOW ---
# -------------------------------------------------------------------

@dag(
    dag_id="update_staging_user_dates",
    start_date=datetime(2025, 10, 29),
    schedule=None, # Manuel pour l'instant
    catchup=False,
    tags=['staging_user_reddit_search']
)
def update_staging_user_dates_dag():
    """
    Orchestre l'enrichissement de la table Staging_Reddit_Posts
    en ajoutant la date de création des comptes auteurs.
    """
    task_run_user_date_update() # Appelle la tâche définie ci-dessus

# --- Instanciation du DAG ---
update_staging_user_dates_dag()