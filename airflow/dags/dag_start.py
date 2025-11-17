"""
DAG Orchestrateur : Trigger des DAG workers avec session authentifiée
Compatible Airflow 3.0
"""

import csv
import random
import time
from datetime import datetime

import pendulum
import requests
from airflow.decorators import dag, task
from bce_utils.proxy import ProxyService
from bce_utils.token import TokenService

# CONFIGURATION
REDIS_HOST = "redis"
REDIS_PORT = 6379
NAMENODE_URL = "http://namenode_bce:9870"
BCE_BASE_URL = "https://kbopub.economie.fgov.be"
SAMPLE_SIZE = 20  # Nombre d'entreprises à sélectionner aléatoirement


@dag(
    dag_id="dag_start",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="*/10 * * * *",  # se lancer toutes les 10 minutes
    catchup=False,
    tags=["exemple", "orchestrateur", "parallelisme"],
)
def dag_start():
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
        """Extrait la liste des proxies pour le mapping (limité à 10)"""
        proxies = proxy_data["proxies"][:20]  # Limite à 20 proxies
        print(f"📋 {len(proxies)} proxies disponibles pour le scraping (limité à 20)")
        return proxies

    @task()
    def create_proxy_entreprise_pairs(proxy_list: list, entreprises_list: list):
        """Crée les paires (proxy, entreprise) 1:1"""
        # Prendre le minimum entre le nombre de proxies et d'entreprises
        nb_pairs = min(len(proxy_list), len(entreprises_list))

        pairs = []
        for i in range(nb_pairs):
            pairs.append({"proxy": proxy_list[i], "entreprise": entreprises_list[i]})

        print(f"🔗 {len(pairs)} paires (proxy, entreprise) créées")
        for i, pair in enumerate(pairs):
            print(
                f"   [{i + 1}] {pair['proxy']} → {pair['entreprise']['entity_number']}"
            )

        return pairs

    @task()
    def get_token():
        token_service = TokenService()
        token = token_service.get_token()
        print(f"🔐 Token JWT obtenu: {token[:20]}...")
        return token

    @task()
    def trigger_scrapper(pair: dict, token: str):
        """Trigger un scraping pour une entreprise avec un proxy donné (1:1)"""

        proxy = pair["proxy"]
        entreprise = pair["entreprise"]
        entity_number = entreprise["entity_number"]

        print(f"   Proxy: {proxy}")

        dag_id = "dag_exemple_scrappring"
        api_url = f"http://airflow-apiserver:8080/api/v2/dags/{dag_id}/dagRuns"

        payload = {
            "dag_run_id": f"scrapper_{entity_number.replace('.', '_')}_{int(time.time())}",
            "logical_date": datetime.utcnow().isoformat() + "Z",
            "conf": {
                "proxy": proxy,
                "token": token,
                "entity_number": entity_number,
            },
        }

        try:
            response = requests.post(
                api_url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {token}",
                },
                timeout=10,
            )
            print(f"📡 Response status: {response.status_code}")

            if response.status_code in [200, 201]:
                print(f"✅ Scraping triggered pour {entity_number}")
                return {
                    "entity_number": entity_number,
                    "proxy": proxy,
                    "triggered": True,
                }
            else:
                print(f"⚠️ Erreur: {response.text}")
                return {
                    "entity_number": entity_number,
                    "proxy": proxy,
                    "triggered": False,
                }

        except Exception as e:
            print(f"❌ Erreur lors du trigger: {str(e)}")
            return {"entity_number": entity_number, "proxy": proxy, "triggered": False}

    # Flux
    entreprises = load_entreprises()
    proxy_data = get_valid_proxies()
    proxy_list = extract_proxy_list(proxy_data)

    # Créer les paires 1:1 (proxy, entreprise)
    pairs = create_proxy_entreprise_pairs(proxy_list, entreprises)

    token = get_token()

    # Trigger un scraping par paire (limité à 10 max)
    # Si 10 proxies → 10 scrapers en parallèle (1 proxy = 1 entreprise)
    trigger_scrapper.partial(token=token).expand(pair=pairs)


dag_start()
