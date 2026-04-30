import requests
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport


class GqlConnector:
    """Connexion synchrone à l'API GraphQL de l'IPBX."""

    def __init__(self, hostname: str, client_id: str, client_secret: str, scope: str = None):
        self.hostname = self._sanitize_hostname(hostname)
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.token_url = f"http://{self.hostname}/admin/api/api/token"
        self.api_url = f"http://{self.hostname}/admin/api/api/gql"
        self._client: Client = None

    def _sanitize_hostname(self, hostname: str) -> str:
        return hostname.replace("http://", "").replace("https://", "").strip().split("/")[0]

    def _request_token(self) -> str:
        data = {'grant_type': 'client_credentials'}
        if self.scope:
            data['scope'] = self.scope
        response = requests.post(self.token_url, data=data, auth=(self.client_id, self.client_secret), timeout=10)
        if response.status_code != 200:
            raise ConnectionError("Impossible d'obtenir le token OAuth. Vérifiez les identifiants et l'URL de l'IPBX.")
        payload = response.json()
        token_type = payload.get("token_type")
        access_token = payload.get("access_token")
        if token_type and access_token:
            return f"{token_type} {access_token}"
        raise ValueError("Token ou type de token manquant dans la réponse de l'IPBX.")

    def _get_client(self) -> Client:
        """Retourne un client GQL, en le créant si nécessaire (lazy init)."""
        if self._client is None:
            token = self._request_token()
            transport = RequestsHTTPTransport(
                url=self.api_url,
                headers={"Authorization": token},
                verify=True,
                retries=1,
            )
            self._client = Client(transport=transport, fetch_schema_from_transport=False)
        return self._client

    def execute_gql_query(self, query: str):
        client = self._get_client()
        result = client.execute(gql(query))
        first_key = next(iter(result))
        if not result[first_key].get('status'):
            raise ValueError(f"Erreur dans la requête GraphQL : {result[first_key].get('message')}")
        return result
