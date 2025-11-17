"""
Module de gestion des tokens JWT pour l'authentification Airflow API
"""

import logging
import requests
from typing import Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class TokenService:
    """
    Service de gestion des tokens JWT pour l'authentification Airflow API
    """

    def __init__(
        self,
        username: str = "airflow",
        password: str = "airflow",
        api_url: str = "http://airflow-apiserver:8080",
        timeout: int = 10
    ):
        """
        Initialise le service de gestion des tokens
        
        Args:
            username: Nom d'utilisateur Airflow (défaut: "airflow")
            password: Mot de passe Airflow (défaut: "airflow")
            api_url: URL de base de l'API Airflow (défaut: "http://airflow-apiserver:8080")
            timeout: Timeout des requêtes en secondes (défaut: 10)
        """
        self.username = username
        self.password = password
        self.api_url = api_url
        self.timeout = timeout
        self._token = None
        self._token_expiry = None

    def get_token(self) -> Optional[str]:
        """
        Obtient un JWT token pour authentifier les appels API Airflow 3
        Utilise le token en cache s'il est encore valide
        
        Returns:
            Token JWT ou None en cas d'erreur
        """
        # Retourner le token en cache s'il est encore valide
        if self._token and self._token_expiry and datetime.now() < self._token_expiry:
            logger.info("🔐 Utilisation du token JWT en cache")
            return self._token

        # Sinon, obtenir un nouveau token
        logger.info("🔄 Obtention d'un nouveau token JWT")
        token = self._fetch_new_token()
        
        if token:
            self._token = token
            # Cache le token pour 50 minutes (les tokens expirent généralement après 1h)
            self._token_expiry = datetime.now() + timedelta(minutes=50)
            logger.info(f"🔐 Token JWT obtenu avec succès (expire dans 50 min)")
        
        return token

    def _fetch_new_token(self) -> Optional[str]:
        """
        Récupère un nouveau token depuis l'API Airflow
        
        Returns:
            Token JWT ou None en cas d'erreur
        """
        token_url = f"{self.api_url}/auth/token"
        
        payload = {
            "username": self.username,
            "password": self.password
        }

        try:
            response = requests.post(
                token_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=self.timeout
            )

            if response.status_code in [200, 201]:
                data = response.json()
                
                # Airflow 3 peut renvoyer "token" ou "access_token"
                token = data.get("access_token") or data.get("token")
                
                if token:
                    return token
                else:
                    logger.warning("⚠️ Réponse valide mais aucun token trouvé")
                    return None
            
            logger.warning(f"⚠️ Erreur token ({response.status_code}): {response.text}")
            return None

        except Exception as e:
            logger.error(f"❌ Erreur obtention token: {e}")
            return None

    def refresh_token(self) -> Optional[str]:
        """
        Force le rafraîchissement du token
        
        Returns:
            Nouveau token JWT ou None en cas d'erreur
        """
        logger.info("🔄 Rafraîchissement forcé du token")
        self._token = None
        self._token_expiry = None
        return self.get_token()

    def invalidate_token(self):
        """
        Invalide le token en cache
        """
        logger.info("🗑️ Invalidation du token en cache")
        self._token = None
        self._token_expiry = None

    def is_token_valid(self) -> bool:
        """
        Vérifie si le token en cache est encore valide
        
        Returns:
            True si le token est valide, False sinon
        """
        return (
            self._token is not None
            and self._token_expiry is not None
            and datetime.now() < self._token_expiry
        )


# Fonction helper pour la rétrocompatibilité
def get_jwt_token(
    username: str = "airflow",
    password: str = "airflow",
    api_url: str = "http://airflow-apiserver:8080"
) -> Optional[str]:
    """
    Fonction helper pour obtenir un token JWT (rétrocompatibilité)
    
    Args:
        username: Nom d'utilisateur Airflow
        password: Mot de passe Airflow
        api_url: URL de base de l'API Airflow
    
    Returns:
        Token JWT ou None en cas d'erreur
    """
    service = TokenService(username=username, password=password, api_url=api_url)
    return service.get_token()

