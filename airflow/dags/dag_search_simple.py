"""
DAG Simple : Scrape une entreprise spécifique avec retry sur proxies
Compatible Airflow 3.0
Usage: Déclencher via API avec le paramètre {"entity_number": "0123456789"}
"""

import random
import time
from datetime import datetime

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from bce_utils.proxy import ProxyService
from bce_utils.token import TokenService
from hdfs import InsecureClient

# CONFIGURATION
REDIS_HOST = "redis"
REDIS_PORT = 6379
NAMENODE_URL = "http://namenode_bce:9870"
BCE_BASE_URL = "https://kbopub.economie.fgov.be"


@dag(
    dag_id="dag_search_simple",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["scraping", "simple", "retry"],
    params={"entity_number": ""},  # Paramètre requis pour le numéro d'entreprise
)
def dag_search_simple():

    @task()
    def get_valid_proxies():
        """Récupère et initialise les proxies dans Redis"""
        proxy_service = ProxyService(redis_host=REDIS_HOST, redis_port=REDIS_PORT)

        try:
            print("🔍 Récupération des proxies...")
            all_proxies = proxy_service.recuperer_tous_proxies()

            if not all_proxies:
                raise ValueError("Aucun proxy récupéré depuis les sources")

            print(f"📋 {len(all_proxies)} proxies récupérés")

            print("🧪 Validation des proxies...")
            valid_proxies = proxy_service.valider_proxies(all_proxies, max_workers=10)

            if not valid_proxies:
                raise ValueError("Aucun proxy valide trouvé")

            print(f"✅ {len(valid_proxies)} proxies valides")

            # Initialiser Redis avec les proxies valides
            proxy_service.initialiser_proxies(valid_proxies)

            # Vérifier les stats
            stats = proxy_service.get_stats()
            print(
                f"📊 Stats Redis - Total: {stats['total']}, Disponibles: {stats['available']}"
            )

            return {
                "total_proxies": len(valid_proxies),
                "stats": stats,
                "proxies": valid_proxies,  # Ajout de la liste des proxies
            }

        except Exception as e:
            print(f"❌ Erreur lors de l'initialisation des proxies: {e}")
            raise

    @task()
    def extract_proxy_list(proxy_data: dict):
        """Extrait la liste des proxies disponibles"""
        proxies = proxy_data["proxies"]
        print(f"📋 {len(proxies)} proxies disponibles pour le scraping")
        return proxies

    @task()
    def get_token():
        token_service = TokenService()
        token = token_service.get_token()
        print(f"🔐 Token JWT obtenu: {token[:20]}...")
        return token

    @task()
    def validate_entity_number(**context):
        """Valide et récupère le numéro d'entreprise depuis les paramètres"""
        entity_number = context["params"].get("entity_number", "").strip()
        
        if not entity_number:
            raise AirflowException(
                "❌ Aucun numéro d'entreprise fourni. "
                "Utilisez: {\"entity_number\": \"0123456789\"}"
            )
        
        print(f"🎯 Entreprise à scraper: {entity_number}")
        return entity_number

    @task()
    def scrape_entreprise_with_retry(entity_number: str, proxy_list: list, token: str):
        """Scrape une entreprise en essayant différents proxies jusqu'à réussite"""
        
        if not proxy_list:
            raise AirflowException("❌ Aucun proxy disponible pour le scraping")
        
        print(f"\n🚀 Début du scraping pour l'entreprise: {entity_number}")
        print(f"📊 {len(proxy_list)} proxies disponibles")
        
        # Créer les services
        proxy_service = ProxyService(redis_host=REDIS_HOST, redis_port=REDIS_PORT)
        client = InsecureClient(NAMENODE_URL, user="root")
        
        max_retries = len(proxy_list)
        
        for attempt in range(max_retries):
            proxy = proxy_list[attempt]
            
            try:
                print(f"\n🔄 Tentative {attempt + 1}/{max_retries}")
                print(f"🌐 Utilisation du proxy: {proxy}")
                
                # Construire l'URL (même méthode que dag_exemple_scrappring)
                transformed_entity_number = entity_number.replace(".", "")
                url = f"{BCE_BASE_URL}/kbopub/zoeknummerform.html?lang=fr&nummer={transformed_entity_number}"
                
                # Faire la requête avec le proxy
                proxies_dict = proxy_service.get_proxy_dict(proxy)
                print(f"🌐 URL: {url}")
                
                response = requests.get(
                    url,
                    proxies=proxies_dict,
                    timeout=15,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                
                print(f"📡 Status code: {response.status_code}")
                
                # Gérer 404 - Ne pas stocker, mais ce n'est pas une erreur du proxy
                if response.status_code == 404:
                    print(f"⚠️ Entreprise {entity_number} non trouvée (404)")
                    return {
                        "status": "skipped",
                        "reason": "404",
                        "entity_number": entity_number,
                    }
                
                response.raise_for_status()
                html_content = response.text
                print(f"📄 HTML récupéré: {len(html_content)} caractères")
                
                # Sauvegarder dans HDFS
                hdfs_path = f"/hdfs-html-page/{entity_number}.html"
                with client.write(hdfs_path, overwrite=True) as writer:
                    writer.write(html_content.encode("utf-8"))
                
                print(f"✅ Entreprise {entity_number} scrapée avec succès")
                
                # Déclencher le DAG Neo4j pour traiter ce fichier
                try:
                    neo4j_dag_id = "dag_bce_to_neo4j"
                    api_url = f"http://airflow-apiserver:8080/api/v2/dags/{neo4j_dag_id}/dagRuns"
                    
                    neo4j_payload = {
                        "dag_run_id": f"neo4j_{entity_number.replace('.', '_')}_{int(time.time())}",
                        "logical_date": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "conf": {
                            "entity_number": entity_number,
                            "hdfs_path": hdfs_path,
                        },
                    }
                    
                    neo4j_response = requests.post(
                        api_url,
                        json=neo4j_payload,
                        headers={
                            "Content-Type": "application/json",
                            "Authorization": f"Bearer {token}",
                        },
                        timeout=10,
                    )
                    
                    if neo4j_response.status_code in [200, 201]:
                        print(f"✅ DAG Neo4j déclenché pour {entity_number}")
                    else:
                        print(f"⚠️ Erreur déclenchement Neo4j: {neo4j_response.text}")
                
                except Exception as e:
                    print(f"⚠️ Erreur lors du déclenchement Neo4j (non-bloquant): {e}")
                
                return {
                    "status": "success",
                    "entity_number": entity_number,
                    "proxy_used": proxy,
                    "attempt": attempt + 1,
                    "hdfs_path": hdfs_path,
                }
                
            except requests.exceptions.RequestException as e:
                print(f"❌ Erreur requête (tentative {attempt + 1}): {e}")
                
                if attempt == max_retries - 1:
                    return {
                        "status": "failed",
                        "reason": f"Échec après {max_retries} tentatives: {str(e)}",
                        "entity_number": entity_number,
                    }
                
                # Attendre un peu avant de réessayer
                wait_time = random.uniform(2, 5)
                print(f"⏳ Attente de {wait_time:.1f}s avant le prochain proxy...")
                time.sleep(wait_time)
                
            except Exception as e:
                print(f"❌ Erreur inattendue: {e}")
                import traceback
                traceback.print_exc()
                
                return {
                    "status": "failed",
                    "reason": str(e),
                    "entity_number": entity_number,
                    "proxy_used": proxy,
                }
        
        # Si on arrive ici, tous les proxies ont échoué
        return {
            "status": "failed",
            "reason": "Échec après toutes les tentatives",
            "entity_number": entity_number,
        }

    # Flux d'exécution
    entity_number = validate_entity_number()
    proxy_data = get_valid_proxies()
    proxy_list = extract_proxy_list(proxy_data)
    token = get_token()
    
    # Scraper l'entreprise avec retry automatique sur tous les proxies
    result = scrape_entreprise_with_retry(entity_number, proxy_list, token)


dag_search_simple()
