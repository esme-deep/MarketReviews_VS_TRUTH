import datetime
import sys
from airflow.decorators import dag, task

# 1. LES IMPORTS À TESTER
# Nous les mettons ici, dans une fonction de tâche.
def import_all_libraries():
    """
    Tâche simple pour essayer d'importer toutes les librairies installées.
    Si une seule échoue, la tâche plantera et le log montrera l'erreur.
    """
    print("--- Démarrage du test d'importation des librairies ---")
    print(f"Version de Python utilisée : {sys.version}")
    print(f"Chemin de l'interpréteur : {sys.executable}")
    
    # Test 1: pyodbc
    try:
        import pyodbc
        print(f"✅ [SUCCÈS] pyodbc importé. Version : {pyodbc.version}")
    except ImportError as e:
        print("❌ [ÉCHEC] Impossible d'importer pyodbc")
        raise e

    # Test 3: python-dotenv (on importe 'dotenv')
    try:
        from dotenv import load_dotenv
        print(f"✅ [SUCCÈS] dotenv (python-dotenv) importé.")
    except ImportError as e:
        print("❌ [ÉCHEC] Impossible d'importer python-dotenv")
        raise e

    # Test 4: requests
    try:
        import requests
        print(f"✅ [SUCCÈS] requests importé. Version : {requests.__version__}")
    except ImportError as e:
        print("❌ [ÉCHEC] Impossible d'importer requests")
        raise e

    # Test 5: beautifulsoup4 (on importe 'bs4')
    try:
        from bs4 import BeautifulSoup
        print(f"✅ [SUCCÈS] bs4 (beautifulsoup4) importé.")
    except ImportError as e:
        print("❌ [ÉCHEC] Impossible d'importer beautifulsoup4")
        raise e

    print("\n--- TEST TERMINÉ ---")
    print("Toutes les librairies ont été importées avec succès !")


# 2. DÉFINITION DU DAG
# C'est la structure de base d'Airflow
@dag(
    dag_id='test_installation_librairies',
    description='Un DAG pour vérifier que les librairies Docker sont installées.',
    start_date=datetime.datetime(2024, 1, 1), # Date de début dans le passé
    schedule=None,                          # Ne s'exécute pas automatiquement
    catchup=False,                          # N'essaie pas de rattraper les exécutions passées
    tags=['test', 'docker', 'dependencies'] # Pour filtrer facilement dans l'UI
)
def my_dependency_test_dag():
    """
    Ce DAG contient une seule tâche qui appelle notre fonction de test.
    """
    
    # 3. LA TÂCHE
    # On transforme notre fonction Python en tâche Airflow
    @task
    def check_libraries_task():
        import_all_libraries()

    # On appelle la fonction pour qu'elle soit dans le DAG
    check_libraries_task()


# 4. INSTANCIATION DU DAG
# Cette ligne est nécessaire pour qu'Airflow détecte le DAG
my_dependency_test_dag()