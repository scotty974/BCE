"""
Module de gestion des proxies pour le scraping BCE
Gère la récupération, validation et rotation des proxies
"""

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import redis
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class ProxyService:
    """
    Service complet de gestion des proxies avec Redis pour tracking d'état
    """

    # Contraintes de scraping
    REQUEST_DELAY_PER_IP = 20  # secondes entre les requêtes par IP
    IP_COOLDOWN_ON_FAILURE = 300  # 5 minutes en secondes
    REQUEST_TIMEOUT = 30  # secondes

    def __init__(self, redis_host: str = "redis", redis_port: int = 6379):
        """
        Initialise le service de gestion des proxies

        Args:
            redis_host: Hôte Redis
            redis_port: Port Redis
        """
        self.redis_client = redis.Redis(
            host=redis_host, port=redis_port, decode_responses=False
        )
        self.proxies = []

    def recuperer_proxies_spys(self) -> List[str]:
        """
        Récupère les proxies depuis spys.me

        Returns:
            Liste des proxies au format IP:PORT
        """
        logger.info("🔍 Récupération des proxies depuis spys.me")

        try:
            response = requests.get("https://spys.me/proxy.txt", timeout=10)
            response.raise_for_status()

            # Regex pour extraire les IPs:ports
            regex = r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]+\b"
            proxies = re.findall(regex, response.text)

            logger.info(f"✅ {len(proxies)} proxies récupérés depuis spys.me")
            return proxies

        except Exception as e:
            logger.error(f"❌ Erreur spys.me: {str(e)}")
            return []

    def recuperer_proxies_free_proxy_list(self) -> List[str]:
        """
        Récupère les proxies depuis free-proxy-list.net

        Returns:
            Liste des proxies au format IP:PORT
        """
        logger.info("🔍 Récupération des proxies depuis free-proxy-list.net")

        try:
            response = requests.get("https://free-proxy-list.net/", timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            td_elements = soup.select(".fpl-list .table tbody tr td")

            proxies = []
            for i in range(0, len(td_elements), 8):
                if i + 1 < len(td_elements):
                    ip = td_elements[i].text.strip()
                    port = td_elements[i + 1].text.strip()
                    proxies.append(f"{ip}:{port}")

            logger.info(
                f"✅ {len(proxies)} proxies récupérés depuis free-proxy-list.net"
            )
            return proxies

        except Exception as e:
            logger.error(f"❌ Erreur free-proxy-list.net: {str(e)}")
            return []

    def recuperer_tous_proxies(self) -> List[str]:
        """
        Récupère les proxies depuis toutes les sources disponibles

        Returns:
            Liste dédupliquée des proxies
        """
        proxies_spys = self.recuperer_proxies_spys()
        proxies_free = self.recuperer_proxies_free_proxy_list()

        # Dédupliquer
        all_proxies = list(set(proxies_spys + proxies_free))
        logger.info(f"📋 Total {len(all_proxies)} proxies uniques récupérés")

        return all_proxies

    def valider_proxies(self, proxies: List[str], max_workers: int = 10) -> List[str]:
        """
        Valide les proxies en testant leur fonctionnement

        Args:
            proxies: Liste des proxies à valider
            max_workers: Nombre de workers pour les tests en parallèle

        Returns:
            Liste des proxies valides
        """
        logger.info(f"🧪 Validation de {len(proxies)} proxies...")

        valid_proxies = []
        test_url = "http://httpbin.org/ip"

        def test_proxy(proxy):
            try:
                proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
                response = requests.get(test_url, proxies=proxies_dict, timeout=5)
                if response.status_code == 200:
                    return proxy
            except:
                pass
            return None

        # Test en parallèle
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(test_proxy, proxy) for proxy in proxies]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    valid_proxies.append(result)

        logger.info(f"✅ {len(valid_proxies)}/{len(proxies)} proxies valides")
        return valid_proxies

    def initialiser_proxies(self, proxies: List[str]):
        """
        Initialise Redis avec la liste des proxies valides

        Args:
            proxies: Liste des proxies à initialiser
        """
        logger.info(f"🔧 Initialisation de Redis avec {len(proxies)} proxies")

        # Nettoyer les anciennes données
        self.redis_client.delete(
            "proxy_status", "proxy_last_used", "proxy_blocked_until"
        )

        # Ajouter tous les proxies
        self.proxies = proxies.copy()
        for proxy in proxies:
            self.redis_client.hset("proxy_status", proxy, "available")

        stats = self.get_stats()
        logger.info(f"✅ Redis initialisé - Proxies disponibles: {stats['available']}")

    def charger_proxies_depuis_redis(self):
        type_cle = self.redis_client.type("proxy_status").decode("utf-8")
        if type_cle == "hash":
            all_proxies = self.redis_client.hkeys("proxy_status")
            self.proxies = [p.decode("utf-8") for p in all_proxies]
        elif type_cle == "list":
            all_proxies = self.redis_client.lrange("proxy_status", 0, -1)
            self.proxies = [p.decode("utf-8") for p in all_proxies]
        else:
            raise TypeError(f"Type de clé non supporté : {type_cle}")

    def get_available_proxy(self) -> Optional[str]:
        current_time = time.time()

        status_type = self.redis_client.type("proxy_status").decode("utf-8")
        if status_type != "hash":
            raise TypeError(f"proxy_status doit être un hash, trouvé {status_type}")

        for proxy in self.proxies:
            status = self.redis_client.hget("proxy_status", proxy)
            if status == b"blocked":
                blocked_until = self.redis_client.hget("proxy_blocked_until", proxy)
                if blocked_until and float(blocked_until) > current_time:
                    continue
                else:
                    self.redis_client.hset("proxy_status", proxy, "available")

            last_used = self.redis_client.hget("proxy_last_used", proxy)
            if last_used:
                time_since_last_use = current_time - float(last_used)
                if time_since_last_use < self.REQUEST_DELAY_PER_IP:
                    continue
            print(proxy)
            return proxy

        return None

    def mark_used(self, proxy: str):
        """
        Marque un proxy comme utilisé maintenant

        Args:
            proxy: Proxy à marquer comme utilisé
        """
        self.redis_client.hset("proxy_last_used", proxy, str(time.time()))

    def mark_failed(self, proxy: str):
        """
        Marque un proxy comme bloqué pour 5 minutes

        Args:
            proxy: Proxy à marquer comme bloqué
        """
        blocked_until = time.time() + self.IP_COOLDOWN_ON_FAILURE
        self.redis_client.hset("proxy_status", proxy, "blocked")
        self.redis_client.hset("proxy_blocked_until", proxy, str(blocked_until))
        logger.warning(f"🚫 Proxy {proxy} bloqué pour {self.IP_COOLDOWN_ON_FAILURE}s")

    def get_stats(self) -> Dict:
        """
        Retourne les statistiques des proxies

        Returns:
            Dictionnaire avec les statistiques (total, available, blocked, in_use)
        """
        stats = {"total": len(self.proxies), "available": 0, "blocked": 0, "in_use": 0}

        current_time = time.time()
        for proxy in self.proxies:
            status = self.redis_client.hget("proxy_status", proxy)
            if status == b"blocked":
                blocked_until = self.redis_client.hget("proxy_blocked_until", proxy)
                if blocked_until and float(blocked_until) > current_time:
                    stats["blocked"] += 1
                else:
                    stats["available"] += 1
            else:
                last_used = self.redis_client.hget("proxy_last_used", proxy)
                if last_used:
                    time_since = current_time - float(last_used)
                    if time_since < self.REQUEST_DELAY_PER_IP:
                        stats["in_use"] += 1
                    else:
                        stats["available"] += 1
                else:
                    stats["available"] += 1

        return stats

    def attendre_proxy_disponible(self, timeout: int = 300) -> Optional[str]:
        """
        Attend qu'un proxy devienne disponible

        Args:
            timeout: Temps maximum d'attente en secondes

        Returns:
            Proxy disponible ou None si timeout
        """
        start_time = time.time()

        while (time.time() - start_time) < timeout:
            proxy = self.get_available_proxy()
            print(proxy)
            if proxy is not None:
                return proxy

            logger.warning("⏳ Aucun proxy disponible, attente 5s...")
            time.sleep(5)
            stats = self.get_stats()
            logger.info(
                f"📊 Proxies - Dispo: {stats['available']}, Bloqués: {stats['blocked']}"
            )

        logger.error(f"❌ Timeout après {timeout}s - Aucun proxy disponible")
        return None

    def get_proxy_dict(self, proxy: str) -> Dict[str, str]:
        """
        Convertit un proxy au format requis par requests

        Args:
            proxy: Proxy au format IP:PORT

        Returns:
            Dictionnaire de configuration proxy pour requests
        """
        return {"http": f"http://{proxy}", "https": f"http://{proxy}"}
