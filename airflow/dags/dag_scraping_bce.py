"""
DAG 2 : Scraping des entreprises BCE avec gestion des proxies
Compatible Airflow 3 avec décorateurs @task

Ce DAG effectue :
1. Récupération et validation des proxies
2. Scraping des pages BCE avec gestion des contraintes
3. Enregistrement du HTML dans HDFS
4. Envoi des métadonnées dans Kafka
"""

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import requests
import json
import time
import logging
import pendulum
from kafka import KafkaProducer
from kafka.errors import KafkaError
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict

# Import du service de gestion des proxies
from bce_utils.proxy import ProxyService

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['kafka_exo_bce:9092']
KAFKA_TOPIC_METADATA = 'bce_scraping_metadata'
REDIS_HOST = 'redis'
REDIS_PORT = 6379
HDFS_NAMENODE_URL = 'http://namenode_exo_bce:9870'
BCE_BASE_URL = 'https://kbopub.economie.fgov.be'

# Contraintes de scraping
MAX_CONCURRENT_REQUESTS = 20
REQUEST_DELAY_PER_IP = 20  # secondes
IP_COOLDOWN_ON_FAILURE = 300  # 5 minutes en secondes
REQUEST_TIMEOUT = 30  # secondes

logger = logging.getLogger(__name__)


@dag(
    dag_id='dag_scraping_bce',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule='0 6 * * *',  # Tous les jours à 6h00
    catchup=False,
    tags=['bce', 'scraping', 'proxies', 'hdfs'],
    description='Scraping des entreprises BCE avec proxies et stockage HDFS'
)
def dag_scraping_bce():
    """
    DAG de scraping avec gestion avancée des proxies
    """
    
    @task
    def recuperer_et_valider_proxies():
        """
        Récupère et valide les proxies depuis toutes les sources
        """
        logger.info("🚀 Démarrage de la récupération et validation des proxies")
        
        try:
            # Initialiser le service de proxies
            proxy_service = ProxyService(redis_host=REDIS_HOST, redis_port=REDIS_PORT)
            
            # Récupérer tous les proxies
            all_proxies = proxy_service.recuperer_tous_proxies()
            
            if not all_proxies:
                logger.error("❌ Aucun proxy récupéré")
                return 0
            
            # Valider les proxies
            valid_proxies = proxy_service.valider_proxies(all_proxies, max_workers=10)
            
            if not valid_proxies:
                logger.error("❌ Aucun proxy valide")
                return 0
            
            # Initialiser Redis avec les proxies valides
            proxy_service.initialiser_proxies(valid_proxies)
            
            return len(valid_proxies)
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération/validation des proxies: {str(e)}")
            raise
    
    @task
    def scraper_entreprises(nb_proxies: int):
        """
        Scrape les entreprises avec gestion des proxies et contraintes
        """
        # Récupérer la liste des entreprises du DAG 1
        from airflow.models import Variable
        
        logger.info("🚀 Démarrage du scraping des entreprises")
        
        # Initialiser le service de proxies et charger depuis Redis
        proxy_service = ProxyService(redis_host=REDIS_HOST, redis_port=REDIS_PORT)
        proxy_service.charger_proxies_depuis_redis()
        
        logger.info(f"📋 {len(proxy_service.proxies)} proxies disponibles")
        
        # Liste factice pour test (normalement récupérée du DAG 1 via XCom ou Variable)
        # TODO: Connecter avec DAG 1
        entreprises_test = [
            {'entity_number': '0200.065.765', 'denomination': 'Test Company 1'},
            {'entity_number': '0200.068.636', 'denomination': 'Test Company 2'},
        ]
        
        # Initialiser Kafka Producer
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except Exception as e:
            logger.error(f"❌ Erreur connexion Kafka: {str(e)}")
            kafka_producer = None
        
        def scrape_entreprise(entreprise: Dict) -> Dict:
            """
            Scrape une entreprise individuelle
            """
            entity_number = entreprise['entity_number']
            max_retries = 3
            
            for attempt in range(max_retries):
                # Attendre qu'un proxy soit disponible
                proxy = proxy_service.attendre_proxy_disponible(timeout=300)
                if proxy is None:
                    logger.error(f"❌ Timeout - Aucun proxy disponible pour {entity_number}")
                    break
                
                try:
                    # Construire l'URL BCE
                    transformed_entity_number = entity_number.replace('.', '')
                    url = f"{BCE_BASE_URL}/kbopub/zoeknummerform.html?lang=fr&nummer={transformed_entity_number}"
                    print(url)
                    
                    # Obtenir la configuration du proxy
                    proxies_dict = proxy_service.get_proxy_dict(proxy)
                    
                    # Marquer le proxy comme utilisé
                    proxy_service.mark_used(proxy)
                    
                    logger.info(f"🌐 Scraping {entity_number} via {proxy} (tentative {attempt + 1})")
                    
                    # Faire la requête
                    response = requests.get(
                        url,
                        proxies=proxies_dict,
                        timeout=REQUEST_TIMEOUT,
                        headers={'User-Agent': 'Mozilla/5.0'}
                    )

                    print("response", response.status_code)
                    
                    # Gérer les codes d'erreur
                    if response.status_code == 404:
                        logger.warning(f"⚠️ 404 pour {entity_number} - Ignoré (ne pas stocker)")
                        return {'status': 'skipped', 'reason': '404', 'entity_number': entity_number}
                    
                    response.raise_for_status()
                    
                    # Succès !
                    html_content = response.text
                    print("html_content")
                    
                    # Sauvegarder dans HDFS
                    hdfs_path = save_to_hdfs(entity_number, html_content)
                    
                    # Envoyer métadonnées dans Kafka
                    metadata = {
                        'entity_number': entity_number,
                        'denomination': entreprise.get('denomination', ''),
                        'hdfs_path': hdfs_path,
                        'scraped_at': datetime.now().isoformat(),
                        'status': 'success',
                        'proxy_used': proxy
                    }
                    
                    if kafka_producer:
                        print("metadata", metadata)
                        kafka_producer.send("lalalalal", value=metadata)
                    
                    logger.info(f"✅ {entity_number} scraped et stocké dans HDFS")
                    
                    return {
                        'status': 'success',
                        'entity_number': entity_number,
                        'hdfs_path': hdfs_path
                    }
                    
                except requests.exceptions.Timeout:
                    logger.warning(f"⏱️ Timeout pour {entity_number} via {proxy}")
                    # Ne pas bloquer le proxy pour un timeout simple
                    continue
                    
                except requests.exceptions.ProxyError:
                    logger.error(f"🚫 Erreur proxy {proxy} pour {entity_number}")
                    proxy_service.mark_failed(proxy)
                    continue
                    
                except requests.exceptions.RequestException as e:
                    if 'banned' in str(e).lower() or 'forbidden' in str(e).lower():
                        logger.error(f"🚫 Proxy {proxy} bloqué par le site")
                        proxy_service.mark_failed(proxy)
                    continue
                    
                except Exception as e:
                    logger.error(f"❌ Erreur inattendue pour {entity_number}: {str(e)}")
                    continue
            
            # Max retries atteint
            logger.error(f"❌ Échec du scraping de {entity_number} après {max_retries} tentatives")
            return {'status': 'failed', 'entity_number': entity_number}
        
        # Scraping en parallèle (max 20 workers)
        results = []
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
            futures = [executor.submit(scrape_entreprise, ent) for ent in entreprises_test]
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
        
        # Statistiques finales
        success_count = sum(1 for r in results if r['status'] == 'success')
        failed_count = sum(1 for r in results if r['status'] == 'failed')
        skipped_count = sum(1 for r in results if r['status'] == 'skipped')
        
        logger.info("=" * 60)
        logger.info("📊 STATISTIQUES DE SCRAPING")
        logger.info("=" * 60)
        logger.info(f"Total entreprises  : {len(entreprises_test)}")
        logger.info(f"Succès            : {success_count}")
        logger.info(f"Échecs            : {failed_count}")
        logger.info(f"Ignorés (404)     : {skipped_count}")
        logger.info("=" * 60)
        
        # Fermer Kafka
        if kafka_producer:
            kafka_producer.flush()
            kafka_producer.close()
        
        return results
    
    # Fonction helper pour HDFS
    def save_to_hdfs(entity_number: str, html_content: str) -> str:
        """
        Sauvegarde le HTML dans HDFS via WebHDFS API
        """
        try:
            # Construire le chemin HDFS
            date_path = datetime.now().strftime('%Y/%m/%d')
            hdfs_path = f"/bce/html/{date_path}/{entity_number}.html"
            
            # URL WebHDFS pour créer le fichier
            url = f"{HDFS_NAMENODE_URL}/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true"
            
            # WebHDFS fonctionne en 2 étapes :
            # 1. GET pour obtenir l'URL de redirection
            # 2. PUT pour uploader le fichier
            
            response = requests.put(
                url,
                data=html_content.encode('utf-8'),
                headers={'Content-Type': 'text/html'},
                allow_redirects=True,
                timeout=30
            )
            
            response.raise_for_status()
            
            return hdfs_path
            
        except Exception as e:
            logger.error(f"❌ Erreur HDFS pour {entity_number}: {str(e)}")
            # Fallback : sauvegarder localement
            local_path = f"/opt/airflow/logs/html_fallback/{entity_number}.html"
            import os
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            return local_path
    
    # Définition du flux
    nb_proxies = recuperer_et_valider_proxies()
    resultats = scraper_entreprises(nb_proxies)


# Instancier le DAG
dag_scraping_bce()
