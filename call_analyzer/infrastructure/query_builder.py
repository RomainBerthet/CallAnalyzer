import logging
from datetime import datetime
from typing import List, Optional

logger = logging.getLogger(__name__)

class QueryBuilder:
    """Construit des requêtes SQL pour l'extraction des données d'appels."""

    @staticmethod
    def format_date(date_str: str) -> str:
        """Formate une date pour une requête SQL.

        Args:
            date_str: Date au format string

        Returns:
            Date formatée pour SQL
        """
        try:
            # Support multiple date formats
            formats = [
                '%Y-%m-%d %H:%M:%S',  # 2025-01-01 00:00:00
                '%Y-%m-%d',  # 2025-01-01
                '%d/%m/%Y %H:%M:%S',  # 01/01/2025 00:00:00
                '%d/%m/%Y'  # 01/01/2025
            ]

            for fmt in formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    return parsed_date.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    continue

            # Si aucun format n'a fonctionné
            raise ValueError(f"Format de date non reconnu: {date_str}")
        except Exception as e:
            logger.error(f"Erreur lors du formatage de la date {date_str}: {e}")
            raise

    @staticmethod
    def build_filter_condition(numeros: List[str]) -> str:
        """Construit une condition de filtrage SQL pour les numéros spécifiés.

        Args:
            numeros: Liste de numéros à filtrer

        Returns:
            Condition SQL formatée
        """
        if not numeros:
            return ""

        clean_numbers = []
        for num in numeros:
            # Standardisation des numéros (suppression des préfixes internationaux)
            if num.startswith('+33'):
                clean_numbers.append(num[3:])
                clean_numbers.append('0' + num[3:])
            elif num.startswith('0'):
                clean_numbers.append(num)
                clean_numbers.append(num[1:])
            else:
                clean_numbers.append(num)
                clean_numbers.append('0' + num)

        conditions = []
        for num in set(clean_numbers):  # Utilisation d'un set pour éviter les doublons
            conditions.append(f"src = '{num}'")
            conditions.append(f"dst = '{num}'")
            conditions.append(f"did = '{num}'")
            conditions.append(f"channel LIKE '%%PJSIP/{num}%%'")
            conditions.append(f"dstchannel LIKE '%%PJSIP/{num}%%'")

        return " AND (" + " OR ".join(conditions) + ")"

    @staticmethod
    def build_call_query(date_debut: str, date_fin: str, numeros: Optional[List[str]] = None) -> str:
        """Construit une requête SQL pour récupérer les appels dans une plage de dates.

        Args:
            date_debut: Date de début au format string
            date_fin: Date de fin au format string
            numeros: Liste des numéros à filtrer (optionnel)

        Returns:
            Requête SQL formatée
        """
        # Formatage des dates
        date_debut_sql = QueryBuilder.format_date(date_debut)
        date_fin_sql = QueryBuilder.format_date(date_fin)

        # Construction de la condition de filtrage
        filter_condition = QueryBuilder.build_filter_condition(numeros) if numeros else ""

        # Requête optimisée avec CTE (Common Table Expression) pour meilleures performances
        query = f"""
        WITH filtered_calls AS (
            SELECT linkedid
            FROM asteriskcdrdb.cdr
            WHERE calldate BETWEEN '{date_debut_sql}' AND '{date_fin_sql}'
            {filter_condition}
            GROUP BY linkedid
        )
        SELECT 
            c.calldate,
            c.uniqueid,
            c.linkedid,
            c.src,
            c.dst,
            c.channel,
            c.dstchannel,
            c.disposition,
            c.cnum,
            c.billsec,
            c.sequence,
            c.dcontext AS context,
            c.lastapp,
            c.cnam,
            c.did,
            c.accountcode,
            c.userfield,
            c.amaflags,
            c.duration,
            c.clid
        FROM asteriskcdrdb.cdr c
        WHERE c.calldate BETWEEN '{date_debut_sql}' AND '{date_fin_sql}'
        AND c.lastapp = 'Dial'
        AND c.linkedid IN (SELECT linkedid FROM filtered_calls)
        ORDER BY c.linkedid, c.sequence
        """
        return query

    @staticmethod
    def build_internal_numbers_query() -> str:
        """Construit une requête GQL pour récupérer les numéros internes: Extensions et Ringroups.

        Returns:
            Requête GQL formatée
        """
        return "query { fetchAllExtensions { status message totalCount extension { extensionId } }  fetchAllRingGroups { status message totalCount ringgroups { groupNumber } }}"

    @staticmethod
    def build_extensions_query() -> str:
        """Construit une requête GQL pour récupérer les extensions et leurs noms.

        Returns:
            Requête GQL formatée
        """
        return "query { fetchAllExtensions { status message totalCount extension { extensionId, user {name}} } }"