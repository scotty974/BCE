import csv
import random
from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from bce_utils.proxy import ProxyService
from hdfs import InsecureClient

# CONFIGURATION
REDIS_HOST = "redis"
REDIS_PORT = 6379
NAMENODE_URL = "http://namenode_bce:9870"
BCE_BASE_URL = "https://kbopub.economie.fgov.be"
SAMPLE_SIZE = 30  # Nombre d'entreprises à sélectionner aléatoirement


@dag(
    dag_id="dag_scraping_html",
    start_date=datetime(2023, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
)
def dag_scraping_html():
<<<<<<< Updated upstream
    entreprises_test = [
        {"entity_number": "0475.960.588", "denomination": "Test Company 1"},
        {"entity_number": "0475.960.687", "denomination": "Test Company 2"},
    ]
=======
    @task()
    def load_entreprises():
        """Charge toutes les entreprises et sélectionne un échantillon aléatoire"""
        rows = []
        with open(
            "/opt/airflow/data/enterprise.csv", newline="", encoding="utf-8"
        ) as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append({"entity_number": r["EnterpriseNumber"]})

        # Sélection aléatoire
        selected = random.sample(rows, min(SAMPLE_SIZE, len(rows)))

        print(
            f"🎲 {len(selected)} entreprises sélectionnées aléatoirement sur {len(rows)}"
        )
        return selected
>>>>>>> Stashed changes

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

            proxy_service.initialiser_proxies(valid_proxies)

            stats = proxy_service.get_stats()
            print(
                f"📊 Stats Redis - Total: {stats['total']}, Disponibles: {stats['available']}"
            )

            return {"total_proxies": len(valid_proxies), "stats": stats}

        except Exception as e:
            print(f"❌ Erreur lors de l'initialisation des proxies: {e}")
            raise

    @task()
    def scrape_entreprise(entreprise, proxy_init_result):
        """Scrape une entreprise"""
        proxy_service = ProxyService(redis_host=REDIS_HOST, redis_port=REDIS_PORT)
        client = InsecureClient(NAMENODE_URL, user="root")
        entity_number = entreprise["entity_number"]

        try:
            proxy_service.charger_proxies_depuis_redis()
        except Exception as e:
            print(f"❌ Erreur chargement proxies pour {entity_number}: {e}")
            return {
                "entity_number": entity_number,
                "status": "failed",
                "reason": f"Erreur chargement proxies: {str(e)}",
            }

        client.makedirs("/hdfs-html-page/")

        max_retries = 3

        for attempt in range(max_retries):
            proxy = None
            try:
                proxy = proxy_service.attendre_proxy_disponible(timeout=60)

                if proxy is None:
                    print(f"⚠️ Aucun proxy disponible pour {entity_number}")
                    return {
                        "entity_number": entity_number,
                        "status": "failed",
                        "reason": "Aucun proxy disponible",
                    }

                proxy_service.mark_used(proxy)

                transformed_entity_number = entity_number.replace(".", "")
                url = f"{BCE_BASE_URL}/kbopub/zoeknummerform.html?lang=fr&nummer={transformed_entity_number}"

                proxies_dict = proxy_service.get_proxy_dict(proxy)
                response = requests.get(
                    url,
                    proxies=proxies_dict,
                    timeout=15,
                    headers={"User-Agent": "Mozilla/5.0"},
                )

                if response.status_code == 404:
                    print(f"⚠️ {entity_number} non trouvé (404)")
                    return {
                        "entity_number": entity_number,
                        "status": "skipped",
                        "reason": "404",
                    }

                response.raise_for_status()
                html_content = response.text

                hdfs_path = f"/hdfs-html-page/{entity_number}.html"
                with client.write(hdfs_path, overwrite=True) as writer:
                    writer.write(html_content.encode("utf-8"))

                print(f"✅ {entity_number} scraped avec succès")
                return {"entity_number": entity_number, "status": "success"}

            except requests.exceptions.RequestException as e:
                print(f"❌ Erreur {entity_number} (tentative {attempt + 1}): {e}")

                if proxy:
                    proxy_service.mark_failed(proxy)

                if attempt == max_retries - 1:
                    return {
                        "entity_number": entity_number,
                        "status": "failed",
                        "reason": str(e),
                    }

            except Exception as e:
                print(f"❌ Erreur inattendue {entity_number}: {e}")
                return {
                    "entity_number": entity_number,
                    "status": "failed",
                    "reason": str(e),
                }

    entreprises = load_entreprises()
    proxy_init = get_valid_proxies()

    scraping_tasks = scrape_entreprise.partial(proxy_init_result=proxy_init).expand(
        entreprise=entreprises
    )

    trigger_neo4j = TriggerDagRunOperator(
        task_id="trigger_neo4j_dag",
        trigger_dag_id="dag_bce_to_neo4j",
        wait_for_completion=False,
    )

    scraping_tasks >> trigger_neo4j


dag_scraping_html()
