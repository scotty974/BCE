from datetime import datetime

import requests
from airflow.decorators import dag, task
from bce_utils.proxy import ProxyService
from hdfs import InsecureClient

# CONFIGURATION
REDIS_HOST = "redis"
REDIS_PORT = 6379
NAMENODE_URL = "http://namenode_bce:9870"
BCE_BASE_URL = "https://kbopub.economie.fgov.be"


@dag(
    dag_id="dag_scraping_html",
    start_date=datetime(2023, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
)
def dag_scraping_html():
    entreprises_test = [
        {"entity_number": "0200.362.408", "denomination": "Test Company 1"},
        {"entity_number": "0200.420.410", "denomination": "Test Company 2"},
    ]

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

            return {"total_proxies": len(valid_proxies), "stats": stats}

        except Exception as e:
            print(f"❌ Erreur lors de l'initialisation des proxies: {e}")
            raise

    @task()
    def scrape_entreprise(entreprise, proxy_init_result):
        """Scrape une entreprise en utilisant les proxies de Redis"""
        # Créer une nouvelle instance pour cette tâche
        proxy_service = ProxyService(redis_host=REDIS_HOST, redis_port=REDIS_PORT)
        client = InsecureClient(NAMENODE_URL, user="root")

        entity_number = entreprise["entity_number"]
        max_retries = 30

        print(f"🏢 Scraping entreprise: {entity_number}")

        # Charger les proxies depuis Redis
        try:
            proxy_service.charger_proxies_depuis_redis()
            print(f"✅ {len(proxy_service.proxies)} proxies chargés depuis Redis")
        except Exception as e:
            print(f"❌ Erreur chargement proxies: {e}")
            return {
                "status": "failed",
                "reason": f"Erreur chargement proxies: {str(e)}",
                "entity_number": entity_number,
            }

        print(f"max_retries: {max_retries}")
        client.makedirs("/hdfs-html-page/")
        for attempt in range(max_retries):
            try:
                print(f"🔄 Tentative {attempt + 1}/{max_retries}")

                # Attendre un proxy disponible
                proxy = proxy_service.attendre_proxy_disponible(timeout=300)

                if proxy is None:
                    print("❌ Aucun proxy disponible")
                    if attempt == max_retries - 1:
                        return {
                            "status": "failed",
                            "reason": "Aucun proxy disponible après timeout",
                            "entity_number": entity_number,
                        }
                    continue

                print(f"🌐 Utilisation du proxy: {proxy}")

                # Marquer le proxy comme utilisé
                proxy_service.mark_used(proxy)

                # Construire l'URL
                transformed_entity_number = entity_number.replace(".", "")
                url = f"{BCE_BASE_URL}/kbopub/zoeknummerform.html?lang=fr&nummer={transformed_entity_number}"

                # Faire la requête
                proxies_dict = proxy_service.get_proxy_dict(proxy)
                print(f"🌐 URL: {url}")
                response = requests.get(
                    url,
                    proxies=proxies_dict,
                    timeout=15,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                print(f"🌐 Status code: {response.status_code}")
                # Gérer 404
                if response.status_code == 404:
                    print(f"⚠️ Entreprise {entity_number} non trouvée (404)")
                    return {
                        "status": "skipped",
                        "reason": "404",
                        "entity_number": entity_number,
                    }

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
                    "proxy_used": proxy,
                }

            except requests.exceptions.RequestException as e:
                print(f"❌ Erreur requête (tentative {attempt + 1}): {e}")

                # Marquer le proxy comme défaillant
                if proxy:
                    proxy_service.mark_failed(proxy)

                if attempt == max_retries - 1:
                    return {
                        "status": "failed",
                        "reason": f"Échec après {max_retries} tentatives: {str(e)}",
                        "entity_number": entity_number,
                    }

            except Exception as e:
                print(f"❌ Erreur inattendue: {e}")
                return {
                    "status": "failed",
                    "reason": str(e),
                    "entity_number": entity_number,
                }
        return {
            "status": "failed",
            "reason": "Échec après toutes les tentatives",
            "entity_number": entity_number,
        }

    # Définir les dépendances entre tâches
    proxy_init = get_valid_proxies()

    # scrape_entreprise dépend de get_valid_proxies
    scrape_entreprise.partial(proxy_init_result=proxy_init).expand(
        entreprise=entreprises_test
    )


dag_scraping_html()
