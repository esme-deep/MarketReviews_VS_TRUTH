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
            user_agent="Airflow:MarketReviewsVSTruth:v1.0 (by /u/VOTRE_NOM_REDDIT)",
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

def get_user_creation_date(reddit_api, author_name):
    """(T) Appelle PRAW pour trouver la date de création d'un utilisateur."""
    try:
        redditor = reddit_api.redditor(author_name)
        # .created_utc est le timestamp (float) de la création
        utc_timestamp = redditor.created_utc 
        return datetime.utcfromtimestamp(utc_timestamp)
    except Exception as e:
        # Gère les utilisateurs suspendus, supprimés ou invalides
        print(f"   -> ⚠️ Erreur API pour '{author_name}' (ex: suspendu/supprimé): {e}")
        return None # On enregistrera 'None' (NULL) pour ne pas le revérifier

def update_staging_posts_for_author(cursor, author_name, creation_date):
    """
    (L) Met à jour TOUS les posts de cet auteur dans la table staging
    avec la date de création et le nouveau statut.
    """
    status = 'Processed' if creation_date else 'Failed_API (Suspended/Deleted)'
    
    print(f"   -> (L) MàJ de '{author_name}' -> Status: {status}")
    
    sql = """
    UPDATE Staging_Reddit_Posts 
    SET 
        AuthorCreationDate = ?, 
        AuthorCheckStatus = ?
    WHERE 
        AuthorName = ?
    """
    cursor.execute(sql, (creation_date, status, author_name))


# -------------------------------------------------------------------
# --- DÉFINITION DE LA TÂCHE AIRFLOW ---
# -------------------------------------------------------------------

@task(task_id="run_user_date_update")
def task_run_user_date_update():
    """
    Tâche ELT qui enrichit la table Staging_Reddit_Posts.
    E: Lit les auteurs uniques avec AuthorCheckStatus IS NULL.
    T: Appelle PRAW pour obtenir la date de création du compte.
    L: UPDATE les lignes correspondantes dans Staging_Reddit_Posts.
    """
    print("--- DÉBUT : Pipeline d'enrichissement (Date Création Auteur) ---")
    
    conn, cursor = None, None
    authors_to_process = []
    
    # --- E (Lecture) ---
    try:
        # Appelle les fonctions définies ci-dessus
        conn = get_staging_connection() 
        cursor = conn.cursor()
        authors_to_process = get_pending_authors(cursor)
    except Exception as e_init:
        print(f"--- ❌ ERREUR MAJEURE lors de la lecture BDD : {e_init} ---")
        if conn: conn.rollback() # Annule au cas où
        raise
    finally:
        # On ne ferme PAS la connexion ici, on la garde pour la boucle d'écriture
        pass 
        
    if not authors_to_process:
        print("\n--- ✅ Fin : Aucun nouvel auteur à vérifier. Staging à jour. ---")
        if cursor: cursor.close()
        if conn: conn.close()
        return

    print(f"\n--- {len(authors_to_process)} nouveaux auteurs à traiter. Lancement de la boucle... ---")
    
    # --- T & L (Boucle de Traitement) ---
    try:
        # Initialise l'API Reddit (UNE SEULE FOIS)
        reddit = get_reddit_api()

        for i, author in enumerate(authors_to_process):
            print(f"\n--- Traitement {i+1}/{len(authors_to_process)} (Auteur: {author}) ---")
            
            # (T) Appel API
            creation_date = get_user_creation_date(reddit, author)
            
            # (L) Écriture DWH
            update_staging_posts_for_author(cursor, author, creation_date)
            
            # !! IMPORTANT : Limiteur de débit !!
            # Respecte l'API Reddit pour ne pas être banni.
            time.sleep(2) # 1 appel toutes les 2 secondes = 30 appels/minute

        # Si tout s'est bien passé, on commit TOUT à la fin
        print("   -> Commit final de la transaction.")
        conn.commit()

    except Exception as e_loop:
        print(f"   -> ❌ ERREUR durant la boucle de traitement : {e_loop}.")
        print("   -> ROLLBACK : Annulation de toutes les modifications de ce run.")
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
    tags=['staging', 'enrichment', 'reddit', 'util']
)
def update_staging_user_dates_dag():
    """
    Orchestre l'enrichissement de la table Staging_Reddit_Posts
    en ajoutant la date de création des comptes auteurs.
    """
    task_run_user_date_update() # Appelle la tâche définie ci-dessus

# --- Instanciation du DAG ---
update_staging_user_dates_dag()