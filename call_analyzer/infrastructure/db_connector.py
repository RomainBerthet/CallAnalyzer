import logging
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

logger = logging.getLogger(__name__)

# Module-level engine cache keyed by connection URL — shared across CDRAnalyzerApp instances
_engine_cache: dict = {}


class DatabaseConnector:
    """Gère la connexion à la base de données et l'exécution des requêtes."""

    def __init__(self, user: str, password: str, database_name: str, host: str, port: str,
                 charset: Optional[str] = None):
        self.user = user
        self.password = password
        self.database_name = database_name
        self.host = host
        self.port = port
        self.charset = charset
        self.engine = self._get_or_create_engine()

    def _build_url(self) -> str:
        url = f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database_name}'
        if self.charset:
            url += f'?charset={self.charset}'
        return url

    def _get_or_create_engine(self) -> Engine:
        # Cache key excludes password for safety; the full URL is used as the actual key
        url = self._build_url()
        cache_key = (self.host, self.port, self.database_name, self.user, self.charset)
        if cache_key not in _engine_cache:
            try:
                _engine_cache[cache_key] = create_engine(
                    url,
                    echo=False,
                    pool_pre_ping=True,
                    poolclass=QueuePool,
                    pool_size=3,
                    max_overflow=2,
                    pool_recycle=1800,
                )
                logger.debug(f"Nouveau moteur SQLAlchemy créé pour {self.host}:{self.port}/{self.database_name}")
            except Exception as e:
                logger.error(f'Impossible de créer le moteur de base de données : {e}')
                raise
        return _engine_cache[cache_key]

    def execute_query(self, query: str) -> pd.DataFrame:
        try:
            with self.engine.connect() as conn:
                return pd.read_sql(text(query), conn)
        except Exception as e:
            logger.error(f"Erreur lors de l'exécution de la requête : {e}")
            raise

    def has_column(self, table: str, column: str) -> bool:
        """Returns True if the given column exists in table (MySQL SHOW COLUMNS)."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SHOW COLUMNS FROM {table} LIKE '{column}'"))
                return result.fetchone() is not None
        except Exception as e:
            logger.warning(f"Impossible de vérifier la colonne {column} dans {table}: {e}")
            return False
