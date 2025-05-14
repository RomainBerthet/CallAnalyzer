import logging
import os
from typing import Dict, Union

import pandas as pd

from call_analyzer.infrastructure.db_connector import DatabaseConnector
from call_analyzer.infrastructure.excel_reporter import ExcelExporter
from call_analyzer.infrastructure.gql_connector import GqlConnector
from call_analyzer.infrastructure.query_builder import QueryBuilder
from call_analyzer.services.call_analyzer import CallAnalyzer
from call_analyzer.services.statistics import StatisticsGenerator

logger = logging.getLogger(__name__)

class CDRAnalyzerApp:
    """Application principale pour l'analyse des CDR."""

    def __init__(self, config: Dict[str, Union[str, int, list]]):
        """Initialise l'application d'analyse CDR.

        Args:
            config: Dictionnaire de configuration contenant:
                - db_user: Nom d'utilisateur pour la base de données
                - db_password: Mot de passe pour la base de données
                - db_name: Nom de la base de données
                - db_host: Adresse du serveur de base de données
                - db_port: Port de la base de données
                - db_charset: Jeu de caractères pour la connexion (optionnel)
                - reference_numbers: Liste des numéros de référence (optionnel)
        """
        self.config = config
        self.db_connector = DatabaseConnector(
            user=config['db_user'],
            password=config['db_password'],
            database_name=config['db_name'],
            host=config['db_host'],
            port=config['db_port'],
            charset=config.get('db_charset')
        )
        self.gql_connector = GqlConnector(
            hostname=config['db_host'],
            client_id=config['client_id'],
            client_secret=config['client_secret'],
            scope=config.get('scope')
        )
        self.reference_numbers = config.get('reference_numbers', [])
        self.internal_numbers = set()
        self.extensions_dict = {}

    def _load_internal_numbers(self):
        """Charge les numéros internes depuis la base de données."""
        try:
            query = QueryBuilder.build_internal_numbers_query()
            result = self.gql_connector.execute_query(query)
            extensions = list(map(lambda x: x['extensionId'], result['fetchAllExtensions.extension'][0]))
            ring_groups = list(map(lambda x: str(x['groupNumber']), result['fetchAllRingGroups.ringgroups'][0]))
            self.internal_numbers = set(extensions).union(set(ring_groups))
            logger.info(f"Chargement réussi de {len(self.internal_numbers)} numéros internes")
            result_extensions_dict = result['fetchAllExtensions.extension'][0]
            df = pd.json_normalize(result_extensions_dict)
            self.extensions_dict = dict(zip(df['extensionId'], df['user.name']))
            result_ring_groups_dict = result['fetchAllRingGroups.ringgroups'][0]
            df = pd.json_normalize(result_ring_groups_dict)
            self.extensions_dict.update(dict(zip(df['groupNumber'].astype(str), df['description'])))
        except Exception as e:
            logger.error(f"Erreur lors du chargement des numéros internes: {e}")
            self.internal_numbers = set()
            self.extensions_dict = {}

    def run_analysis(self, date_debut: str, date_fin: str, output_dir: str = './output') -> Dict[str, str]:
        """Exécute l'analyse complète des appels.

        Args:
            date_debut: Date de début de l'analyse
            date_fin: Date de fin de l'analyse
            output_dir: Répertoire de sortie pour les fichiers générés

        Returns:
            Dictionnaire contenant les chemins des fichiers générés
        """
        # Création du répertoire de sortie
        os.makedirs(output_dir, exist_ok=True)

        # Chargement des numéros internes et extensions
        self._load_internal_numbers()

        # Construction de la requête et exécution
        query = QueryBuilder.build_call_query(date_debut, date_fin, self.reference_numbers)
        df_calls = self.db_connector.execute_query(query)

        if df_calls.empty:
            logger.warning(f"Aucun appel trouvé entre {date_debut} et {date_fin}")
            return {'status': 'no_data'}

        # Analyse des appels
        analyzer = CallAnalyzer(self.internal_numbers, self.reference_numbers)
        calls = analyzer.process_dataframe(df_calls)

        if not calls:
            logger.warning("Aucun appel analysé")
            return {'status': 'no_data'}

        # Conversion en DataFrame
        df_analyzed = analyzer.to_dataframe(calls)

        # Génération des statistiques
        statistics = StatisticsGenerator.calculate_statistics(df_analyzed, self.reference_numbers)

        # Formatage de la période pour les noms de fichiers
        period_str = f"{pd.to_datetime(date_debut).strftime('%Y%m%d')}-{pd.to_datetime(date_fin).strftime('%Y%m%d')}"
        period_display = f"{pd.to_datetime(date_debut).strftime('%d/%m/%Y')} au {pd.to_datetime(date_fin).strftime('%d/%m/%Y')}"

        # Export des données vers Excel
        files = {}

        calls_file = os.path.join(output_dir, f"appels_{period_str}.xlsx")
        stats_file = os.path.join(output_dir, f"stats_appels_{period_str}.xlsx")

        files['calls'] = ExcelExporter.export_calls_to_excel(df_analyzed, calls_file, self.extensions_dict)
        files['stats'] = ExcelExporter.export_statistics_to_excel(df_analyzed, statistics, stats_file, period_display)
        files['status'] = 'success'

        logger.info(f"Analyse terminée: {len(calls)} appels analysés")
        logger.info(f"Fichiers générés: {files}")

        return files