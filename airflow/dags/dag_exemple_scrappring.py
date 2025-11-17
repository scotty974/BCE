"""
DAG Worker : Scraping d'entreprises BCE
Ce DAG est triggered N fois en parallèle par l'orchestrateur
"""

from airflow.decorators import dag, task
import pendulum
import time
import requests
from hdfs import InsecureClient
from bce_utils.proxy import ProxyService

REDIS_HOST = "redis"
REDIS_PORT = 6379
NAMENODE_URL = "http://namenode_bce:9870"
BCE_BASE_URL = "https://kbopub.economie.fgov.be"

@dag(
    dag_id='dag_exemple_scrappring',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,  # Triggered par l'orchestrateur
    catchup=False,
    tags=['exemple', 'worker', 'scrappring']
    # Note: max_active_runs se configure dans airflow.cfg ou docker-compose.yaml
)
def dag_exemple_scrappring():

    @task()
    def scrape_entreprise(**context):
        """Scrape une entreprise en utilisant le proxy assigné"""
        # Récupérer les paramètres depuis la configuration du DAG run
        dag_run_conf = context.get('dag_run').conf
        
        proxy_recu = dag_run_conf.get('proxy')
        entity_number = dag_run_conf.get('entity_number')
        denomination = dag_run_conf.get('denomination', 'N/A')
        
        print(f"🏢 Scraping entreprise: {entity_number} - {denomination}")
        print(f"🌐 Proxy assigné: {proxy_recu}")
        
        # Créer une nouvelle instance pour cette tâche
        proxy_service = ProxyService(redis_host=REDIS_HOST, redis_port=REDIS_PORT)
        client = InsecureClient(NAMENODE_URL, user="root")

        # Nombre réduit de tentatives car on utilise un seul proxy
        max_retries = 10
        
        # Vérifier qu'on a bien reçu un proxy
        if not proxy_recu:
            print("❌ Aucun proxy assigné")
            return {
                "status": "failed",
                "reason": "Aucun proxy assigné",
                "entity_number": entity_number,
            }

        print(f"🔄 Max tentatives: {max_retries} (avec le proxy assigné uniquement)")
        
        for attempt in range(max_retries):
            try:
                print(f"🔄 Tentative {attempt + 1}/{max_retries}")
                print(f"🌐 Utilisation du proxy: {proxy_recu}")

                # Construire l'URL
                transformed_entity_number = entity_number.replace(".", "")
                url = f"{BCE_BASE_URL}/kbopub/zoeknummerform.html?lang=fr&nummer={transformed_entity_number}"

                # Faire la requête avec le proxy assigné
                proxies_dict = proxy_service.get_proxy_dict(proxy_recu)
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

                response.raise_for_status()
                html_content = response.text

                # Sauvegarder dans HDFS
                hdfs_path = f"/hdfs-html-page/{entity_number}.html"
                with client.write(hdfs_path, overwrite=True) as writer:
                    writer.write(html_content.encode("utf-8"))

                print(f"✅ Entreprise {entity_number} scrapée avec succès")

                return {
                    "status": "success",
                    "entity_number": entity_number,
                    "proxy_used": proxy_recu,
                }

            except requests.exceptions.RequestException as e:
                print(f"❌ Erreur requête (tentative {attempt + 1}): {e}")

                # # Marquer le proxy comme défaillant
                # if proxy_recu:
                #     proxy_service.mark_failed(proxy_recu)

                if attempt == max_retries - 1:
                    return {
                        "status": "failed",
                        "reason": f"Échec après {max_retries} tentatives: {str(e)}",
                        "entity_number": entity_number,
                    }

            except Exception as e:
                print(f"❌ Erreur inattendue: {e}")
                import traceback
                traceback.print_exc()
                return {
                    "status": "failed",
                    "reason": str(e),
                    "entity_number": entity_number,
                    "proxy_used": proxy_recu,
                }
                
        return {
            "status": "failed",
            "reason": "Échec après toutes les tentatives",
            "entity_number": entity_number,
            "proxy_used": proxy_recu,
        }
    
    # Appeler la tâche
    scrape_entreprise()



# Instancier le DAG
dag_exemple_scrappring()