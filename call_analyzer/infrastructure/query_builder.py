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
            conditions.append(f"cnum = '{num}'")
            conditions.append(f"channel LIKE '%PJSIP/{num}%'")
            conditions.append(f"dstchannel LIKE '%PJSIP/{num}%'")

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
            AND c.linkedid IN (
                SELECT DISTINCT linkedid
                FROM asteriskcdrdb.cdr
                WHERE calldate BETWEEN '{date_debut_sql}' AND '{date_fin_sql}'
                {filter_condition}
            )
            ORDER BY c.linkedid, c.sequence
        """
        return query

    @staticmethod
    def build_billing_sda_filter(sda_numbers: List[str]) -> str:
        """Builds a SQL AND clause that matches rows where the SDA appears in src, cnum or did."""
        if not sda_numbers:
            return ""
        variants: set = set()
        for num in sda_numbers:
            variants.add(num)
            if num.startswith('0'):
                variants.add(f"33{num[1:]}")
                variants.add(f"+33{num[1:]}")
            elif num.startswith('33') and len(num) > 2:
                variants.add(f"0{num[2:]}")
        quoted = ', '.join(f"'{n}'" for n in variants)
        return f"AND (src IN ({quoted}) OR cnum IN ({quoted}) OR did IN ({quoted}))"

    @staticmethod
    def build_billing_query(date_debut: str, date_fin: str, sda_numbers: List[str]) -> str:
        """
        Billing query for IPBX that have the linkedid column.
        Fetches all Dial events belonging to calls that involve the given SDA numbers.
        Uses a JOIN-based subquery (faster than IN on MySQL 5.x/8.x).
        """
        start = QueryBuilder.format_date(date_debut)
        end = QueryBuilder.format_date(date_fin)
        sda_filter = QueryBuilder.build_billing_sda_filter(sda_numbers)
        return f"""
            SELECT c.calldate, c.uniqueid, c.linkedid, c.src, c.dst,
                   c.channel, c.dstchannel, c.disposition, c.cnum, c.billsec,
                   c.sequence, c.dcontext AS context, c.lastapp,
                   c.cnam, c.did, c.accountcode, c.userfield, c.duration, c.clid
            FROM asteriskcdrdb.cdr c
            INNER JOIN (
                SELECT DISTINCT linkedid
                FROM asteriskcdrdb.cdr
                WHERE calldate BETWEEN '{start}' AND '{end}'
                {sda_filter}
            ) relevant ON c.linkedid = relevant.linkedid
            WHERE c.calldate BETWEEN '{start}' AND '{end}'
            AND c.lastapp = 'Dial'
            ORDER BY c.linkedid, c.sequence
        """

    @staticmethod
    def build_billing_query_no_linkedid(date_debut: str, date_fin: str, sda_numbers: List[str]) -> str:
        """
        Simplified billing query for old IPBX that lack the linkedid column.
        Fetches outgoing trunk Dial events where the SDA appears in src or cnum.
        External renvoi detection is not possible without linkedid.
        """
        start = QueryBuilder.format_date(date_debut)
        end = QueryBuilder.format_date(date_fin)
        sda_filter = QueryBuilder.build_billing_sda_filter(sda_numbers)
        return f"""
            SELECT calldate, uniqueid, uniqueid AS linkedid, src, dst,
                   channel, dstchannel, disposition, cnum, billsec,
                   0 AS sequence, dcontext AS context, lastapp,
                   NULL AS cnam, did, NULL AS accountcode, NULL AS userfield,
                   NULL AS amaflags, duration, NULL AS clid
            FROM asteriskcdrdb.cdr
            WHERE calldate BETWEEN '{start}' AND '{end}'
            AND lastapp = 'Dial'
            AND dstchannel LIKE '%trunk%'
            {sda_filter}
            ORDER BY calldate
        """

    @staticmethod
    def build_internal_numbers_query() -> str:
        """Construit une requête GQL pour récupérer les numéros internes: Extensions et Ringroups.

        Returns:
            Requête GQL formatée
        """
        return """
        query {
          fetchAllExtensions {
            status
            message
            totalCount
            extension {
              extensionId
              user {
                name
              }
            }
          }
          fetchAllRingGroups {
            status
            message
            totalCount
            ringgroups {
              groupNumber
              description
            }
          }
        }
        """