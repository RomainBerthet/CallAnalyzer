import asyncio

import pandas as pd
import requests
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

def execute_sync(coro):
    return asyncio.get_event_loop().run_until_complete(coro)

def to_dataframe(response: dict) -> pd.DataFrame:
    # Transforme récursivement les données pour convenir à un DataFrame (à adapter selon ton schéma GQL)
    return pd.json_normalize(response)


class GqlConnector:
    """
    Classe responsable de la connexion à l'API GraphQL de l'IPBX
    """

    def __init__(self, hostname: str, client_id: str, client_secret: str, scope: str = None):
        self.hostname = self._sanitize_hostname(hostname)
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.token_url = f"http://{self.hostname}/admin/api/api/token"
        self.api_url = f"http://{self.hostname}/admin/api/api/gql"
        self.token = None

    def _sanitize_hostname(self, hostname: str) -> str:
        """Nettoie l'URL fournie pour ne garder que le hostname"""
        return hostname.replace("http://", "").replace("https://", "").strip().split("/")[0]

    def _request_token(self) -> str:
        """Effectue une requête pour obtenir un token OAuth"""
        data = {'grant_type': 'client_credentials'}
        if self.scope:
            data['scope'] = self.scope

        response = requests.post(self.token_url, data=data, auth=(self.client_id, self.client_secret))
        if response.status_code != 200:
            raise ConnectionError("Impossible d'obtenir le token OAuth. Vérifiez les identifiants et l'URL de l'IPBX.")

        token_type = response.json().get("token_type")
        access_token = response.json().get("access_token")
        if token_type and access_token:
            return f"{token_type} {access_token}"

        raise ValueError("Token ou type de token manquant dans la réponse de l'IPBX.")

    def connect(self) -> Client:
        """
        Connecte au service GraphQL de l'IPBX et retourne un client GQL.
        :return: Instance `Client` prête à être utilisée pour les requêtes GraphQL.
        """
        try:
            self.token = self._request_token()
            headers = {"Authorization": self.token}
            transport = AIOHTTPTransport(url=self.api_url, headers=headers)
            return Client(transport=transport, fetch_schema_from_transport=False)
        except Exception as e:
            raise ConnectionError(f"Erreur lors de la connexion à l'API GraphQL : {e}")

    async def execute_query_async(self, query: str) -> pd.DataFrame:
        """
        Exécute une requête GraphQL de manière asynchrone avec fermeture propre du client.
        :param query: Requête GraphQL à exécuter.
        :return: DataFrame contenant les résultats.
        """
        self.token = self._request_token()
        transport = AIOHTTPTransport(url=self.api_url, headers={"Authorization": self.token})

        async with Client(transport=transport, fetch_schema_from_transport=False) as session:
            try:
                response = await session.execute(gql(query))
                return to_dataframe(response)
            except Exception as e:
                raise RuntimeError(f"Erreur lors de l'exécution de la requête GraphQL : {e}")

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Version synchrone qui encapsule la logique async.
        :param query: Requête GraphQL.
        :return: DataFrame.
        """
        return execute_sync(self.execute_query_async(query))
