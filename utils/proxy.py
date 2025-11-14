import time

import requests
from bs4 import BeautifulSoup


class Proxy:
    def __init__(self):
        self.url_list_proxies = "https://api.proxyscrape.com/v4/free-proxy-list/get?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=elite,anonymous&skip=0&limit=2000"
        self.test_url = "https://something.com/"
        self.size_proxies = 2
        self.list_proxies = []

    def get_proxies(self):
        c = requests.get(self.url_list_proxies)
        proxies = [p.strip() for p in c.text.split("\n") if p.strip()]
        print(f"Nombre de proxies récupérés: {len(proxies)}")
        return proxies

    def test_proxies(self, proxies):
        for proxy in proxies:
            # Arrêter si on a trouvé assez de proxies
            if len(self.list_proxies) >= self.size_proxies:
                break

            try:
                response = requests.get(
                    self.test_url,
                    proxies={"http": f"http://{proxy}", "https": f"https://{proxy}"},
                    timeout=5,  # Timeout de 5 secondes
                )
                if response.status_code == 200:
                    print(f"✓ Proxy {proxy} is working")
                    self.list_proxies.append(proxy)
                else:
                    print(
                        f"✗ Proxy {proxy} is not working (status: {response.status_code})"
                    )
            except Exception as e:
                print(f"✗ Proxy {proxy} is not working ({type(e).__name__})")

            # Délai entre chaque test de proxy
            time.sleep(0.3)


proxy = Proxy()

while len(proxy.list_proxies) < proxy.size_proxies:
    result = proxy.get_proxies()
    proxy.test_proxies(result)

    # Si on n'a pas trouvé assez de proxies, attendre avant de refaire un appel
    if len(proxy.list_proxies) < proxy.size_proxies:
        print(
            f"\nSeulement {len(proxy.list_proxies)}/{proxy.size_proxies} proxy(s) trouvé(s)."
        )
        print("Attente de 15 secondes avant de relancer...\n")
        time.sleep(15)

print(f"\n✓ Proxies fonctionnels trouvés: {proxy.list_proxies}")
