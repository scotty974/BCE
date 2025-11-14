from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import logging
import os
import sys

# Ajouter le dossier bce_utils au path pour importer les modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'bce_utils'))

from bce_utils.bce_extractor import BCEDataExtractor
from bce_utils.bce_neo4j_loader import BCENeo4jLoader

# Configuration
HDFS_URL = 'http://namenode_exo_bce:9870'
HDFS_USER = 'root'
HDFS_BASE_PATH = '../data'  # Chemin de base dans HDFS où sont les dossiers d'entreprises
NEO4J_URI = 'bolt://neo4j:7687'
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = 'password'
TEMP_JSON_DIR = '..data/bce_temp_json'
PROCESSED_FILES_PATH = '../data/bce_processed_files.txt'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_processed_files():
    """Récupère la liste des fichiers déjà traités"""
    if os.path.exists(PROCESSED_FILES_PATH):
        with open(PROCESSED_FILES_PATH, 'r') as f:
            return set(line.strip() for line in f.readlines())
    return set()

def save_processed_file(hdfs_path):
    """Sauvegarde un fichier comme traité"""
    with open(PROCESSED_FILES_PATH, 'a') as f:
        f.write(f"{hdfs_path}\n")

def get_new_html_files(**context):
    """
    Récupère la liste des nouveaux fichiers HTML depuis HDFS
    (fichiers non encore traités)
    """
    from hdfs import InsecureClient
    
    logger = logging.getLogger(__name__)
    hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)
    
    # Récupérer les fichiers déjà traités
    processed_files = get_processed_files()
    logger.info(f"Nombre de fichiers déjà traités: {len(processed_files)}")
    
    # Parcourir les dossiers d'entreprises dans HDFS
    new_files = []
    
    try:
        # Lister tous les dossiers dans HDFS_BASE_PATH
        company_dirs = hdfs_client.list(HDFS_BASE_PATH)
        
        for company_dir in company_dirs:
            company_path = f"{HDFS_BASE_PATH}/{company_dir}"
            
            # Vérifier si c'est un dossier (numéro d'entreprise)
            try:
                files_in_dir = hdfs_client.list(company_path)
                
                # Chercher les fichiers HTML
                for file in files_in_dir:
                    if file.endswith('.htm') or file.endswith('.html'):
                        full_path = f"{company_path}/{file}"
                        
                        # Vérifier si le fichier n'a pas déjà été traité
                        if full_path not in processed_files:
                            new_files.append(full_path)
                            logger.info(f"Nouveau fichier trouvé: {full_path}")
            except Exception as e:
                logger.warning(f"Erreur lors de la lecture de {company_path}: {e}")
                continue
        
        logger.info(f"Nombre de nouveaux fichiers à traiter: {len(new_files)}")
        
        # Passer la liste des fichiers à la prochaine tâche via XCom
        context['task_instance'].xcom_push(key='new_files', value=new_files)
        
        return len(new_files)
        
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des fichiers HDFS: {e}")
        raise

def extract_bce_data(**context):
    """Tâche d'extraction des données BCE depuis HDFS"""
    logger = logging.getLogger(__name__)
    
    # Récupérer la liste des nouveaux fichiers depuis XCom
    new_files = context['task_instance'].xcom_pull(
        task_ids='get_new_files',
        key='new_files'
    )
    
    if not new_files:
        logger.info("Aucun nouveau fichier à traiter")
        return 0
    
    logger.info(f"Extraction de {len(new_files)} fichiers")
    
    # Initialiser l'extracteur
    extractor = BCEDataExtractor(
        hdfs_url=HDFS_URL,
        hdfs_user=HDFS_USER,
        temp_output_dir=TEMP_JSON_DIR
    )
    
    # Traiter chaque fichier
    successfully_processed = []
    failed_files = []
    
    for hdfs_file in new_files:
        try:
            data, json_path = extractor.process_file(hdfs_file)
            successfully_processed.append((hdfs_file, json_path))
            logger.info(f"✓ Fichier {hdfs_file} extrait avec succès")
        except Exception as e:
            logger.error(f"✗ Erreur lors de l'extraction de {hdfs_file}: {e}")
            failed_files.append(hdfs_file)
            continue
    
    # Passer les chemins des JSON à la tâche suivante
    json_paths = [json_path for _, json_path in successfully_processed]
    hdfs_paths = [hdfs_path for hdfs_path, _ in successfully_processed]
    
    context['task_instance'].xcom_push(key='json_paths', value=json_paths)
    context['task_instance'].xcom_push(key='hdfs_paths', value=hdfs_paths)
    context['task_instance'].xcom_push(key='failed_files', value=failed_files)
    
    logger.info(f"Extraction terminée: {len(successfully_processed)} succès, {len(failed_files)} échecs")
    
    return len(successfully_processed)

def load_to_neo4j(**context):
    """Tâche de chargement des données dans Neo4j"""
    logger = logging.getLogger(__name__)
    
    # Récupérer les chemins des fichiers JSON depuis XCom
    json_paths = context['task_instance'].xcom_pull(
        task_ids='extract_data',
        key='json_paths'
    )
    
    hdfs_paths = context['task_instance'].xcom_pull(
        task_ids='extract_data',
        key='hdfs_paths'
    )
    
    if not json_paths:
        logger.info("Aucune donnée à charger dans Neo4j")
        return 0
    
    logger.info(f"Chargement de {len(json_paths)} fichiers dans Neo4j")
    
    # Initialiser le loader Neo4j
    with BCENeo4jLoader(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD) as loader:
        # Créer les contraintes et index (une seule fois)
        loader.create_constraints()
        loader.create_indexes()
        
        # Charger chaque fichier
        successfully_loaded = []
        failed_loads = []
        
        for idx, json_path in enumerate(json_paths):
            try:
                loader.load_bce_data(json_path)
                successfully_loaded.append(json_path)
                
                # Marquer le fichier HDFS comme traité
                hdfs_path = hdfs_paths[idx]
                save_processed_file(hdfs_path)
                
                logger.info(f"✓ Fichier {json_path} chargé dans Neo4j")
            except Exception as e:
                logger.error(f"✗ Erreur lors du chargement de {json_path}: {e}")
                failed_loads.append(json_path)
                continue
    
    logger.info(f"Chargement terminé: {len(successfully_loaded)} succès, {len(failed_loads)} échecs")
    
    return len(successfully_loaded)

def cleanup_temp_files(**context):
    """Tâche de nettoyage des fichiers JSON temporaires"""
    import shutil
    logger = logging.getLogger(__name__)
    
    if os.path.exists(TEMP_JSON_DIR):
        try:
            shutil.rmtree(TEMP_JSON_DIR)
            logger.info(f"Répertoire temporaire {TEMP_JSON_DIR} nettoyé")
        except Exception as e:
            logger.error(f"Erreur lors du nettoyage de {TEMP_JSON_DIR}: {e}")
    else:
        logger.info(f"Répertoire {TEMP_JSON_DIR} n'existe pas, rien à nettoyer")

# Définition du DAG
with DAG(
    'dag_bce_to_neo4j',
    default_args=default_args,
    description='Pipeline d\'extraction BCE depuis HDFS vers Neo4j',
    schedule_interval=None,  # Sera déclenché par un autre DAG
    catchup=False,
    tags=['bce', 'neo4j', 'hdfs', 'extraction'],
) as dag:
    
    # Sensor pour attendre la fin d'un autre DAG (à configurer selon votre DAG précédent)
    # wait_for_previous_dag = ExternalTaskSensor(
    #     task_id='wait_for_scraping_dag',
    #     external_dag_id='dag_scraping_html',  # Remplacer par l'ID de votre DAG de scraping
    #     external_task_id=None,  # None = attendre que tout le DAG soit terminé
    #     mode='poke',
    #     timeout=600,
    #     poke_interval=30,
    # )
    
    get_files_task = PythonOperator(
        task_id='get_new_files',
        python_callable=get_new_html_files,
        provide_context=True,
    )
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_bce_data,
        provide_context=True,
    )
    
    load_task = PythonOperator(
        task_id='load_to_neo4j',
        python_callable=load_to_neo4j,
        provide_context=True,
    )
    
    cleanup_task = PythonOperator(
        task_id='cleanup_temp_files',
        python_callable=cleanup_temp_files,
        provide_context=True,
        trigger_rule='all_done',  # S'exécute toujours, même en cas d'échec
    )
    
    # Définir l'ordre d'exécution
    # wait_for_previous_dag >> get_files_task >> extract_task >> load_task >> cleanup_task
    get_files_task >> extract_task >> load_task >> cleanup_task