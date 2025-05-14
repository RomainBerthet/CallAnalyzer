import logging
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

class DatabaseConnector:
    """Gère la connexion à la base de données et l'exécution des requêtes."""

    def __init__(self, user: str, password: str, database_name: str, host: str, port: str,
                 charset: Optional[str] = None):
        """Initialise la connexion à la base de données.

        Args:
            user: Nom d'utilisateur pour la connexion
            password: Mot de passe pour la connexion
            database_name: Nom de la base de données
            host: Adresse du serveur de base de données
            port: Port de connexion
            charset: Jeu de caractères (optionnel)
        """
        self.user = user
        self.password = password
        self.database_name = database_name
        self.host = host
        self.port = port
        self.charset = charset
        self.engine = self._create_connection()

    def _create_connection(self) -> Engine:
        """Crée une connexion à la base de données.

        Returns:
            Un objet Engine SQLAlchemy pour la connexion

        Raises:
            Exception: Si la connexion échoue
        """
        try:
            url = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database_name}'
            if self.charset:
                url += f'?charset={self.charset}'
            engine = create_engine(url, echo=False, pool_pre_ping=True)
            return engine
        except Exception as e:
            logger.error(f'Impossible de se connecter à la base de données : {e}')
            raise

    def execute_query(self, query: str) -> pd.DataFrame:
        """Exécute une requête SQL et retourne les résultats.

        Args:
            query: Requête SQL à exécuter

        Returns:
            DataFrame contenant les résultats de la requête

        Raises:
            Exception: Si l'exécution de la requête échoue
        """
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            logger.error(f'Erreur lors de l\'exécution de la requête : {e}')
            raise
