import logging
from typing import Dict, List, Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)

class StatisticsGenerator:
    """Génère des statistiques à partir des données d'appels."""

    @staticmethod
    def calculate_statistics(df: pd.DataFrame, reference_numbers: Optional[List[str]] = None) -> Dict[
        str, Union[int, float]]:
        """Calcule les statistiques à partir d'un DataFrame d'appels.

        Args:
            df: DataFrame contenant les données d'appels
            reference_numbers: Liste des numéros de référence (optionnel)

        Returns:
            Dictionnaire contenant les statistiques
        """
        if df.empty:
            return StatisticsGenerator.get_empty_statistics()

        # Filtrage du dataframe selon les numéros de référence si nécessaire
        if not reference_numbers:
            df = df[~df['is_internal']]

        if df.empty:
            return StatisticsGenerator.get_empty_statistics()

        # Calculs vectorisés pour de meilleures performances
        stats = {
            'nb_appels_total': len(df),
            'nb_appels_recus': len(df[df['type_appel'] == 'entrant']),
            'nb_appels_emis': len(df[df['type_appel'] == 'sortant']),

            'nb_appels_internes': len(df[df['is_internal']]),
            'nb_appel_interne_emis': len(df[(df['type_appel'] == 'sortant') & df['is_internal']]),
            'nb_appel_interne_recus': len(df[(df['type_appel'] == 'entrant') & df['is_internal']]),

            'nb_appels_manques': len(df[(df['type_appel'] == 'entrant') & (~df['answered'])]),
            'nb_appels_externes_manques': len(df[(df['type_appel'] == 'entrant') & (~df['answered']) & (~df['is_internal'])]),
            'nb_appels_internes_manques': len(df[(df['type_appel'] == 'entrant') & (~df['answered']) & df['is_internal']]),
            'nb_appels_internes_repondus': len(df[(df['type_appel'] == 'entrant') & df['answered'] & df['is_internal']]),

            'nb_appels_aboutis': len(df[(df['type_appel'] == 'sortant') & df['answered']]),
            'nb_appels_externes_aboutis': len(df[(df['type_appel'] == 'sortant') & df['answered'] & (~df['is_internal'])]),
            'nb_appels_internes_aboutis': len(df[(df['type_appel'] == 'sortant') & df['answered'] & df['is_internal']]),

            'duree_appels_total': int(df['billsec'].sum()),

            'duree_appels_recus': int(df.loc[df['type_appel'] == 'entrant', 'billsec'].sum()),
            'duree_appels_internes_recus': int(df.loc[(df['type_appel'] == 'entrant') & df['is_internal'], 'billsec'].sum()),
            'duree_appels_externes_recus': int(df.loc[(df['type_appel'] == 'entrant') & (~df['is_internal']), 'billsec'].sum()),

            'duree_appels_emis': int(df.loc[df['type_appel'] == 'sortant', 'billsec'].sum()),
            'duree_appels_internes_emis': int(df.loc[(df['type_appel'] == 'sortant') & df['is_internal'], 'billsec'].sum()),
            'duree_appels_externes_emis': int(df.loc[(df['type_appel'] == 'sortant') & (~df['is_internal']), 'billsec'].sum()),

            'duree_moyenne_appels': int(df.loc[df['answered'], 'billsec'].mean()) if len(df[df['answered']]) > 0 else 0,
            'duree_moyenne_appels_internes': int(df.loc[(df['answered']) & df['is_internal'], 'billsec'].mean()) if len(df[(df['answered']) & df['is_internal']]) > 0 else 0,
            'duree_moyenne_appels_externes': int(df.loc[(df['answered']) & (~df['is_internal']), 'billsec'].mean()) if len(df[(df['answered']) & (~df['is_internal'])]) > 0 else 0,

            'nb_renvois_appels_recus': len(df[(df['type_appel'] == 'entrant') & df['renvoi_vers'].notna()]),
            'duree_renvois_appels_recus': int(df.loc[(df['type_appel'] == 'entrant') & df['renvoi_vers'].notna(), 'billsec'].sum()),

            'nb_click_to_call': len(df[df['is_click_to_call']]),
        }

        return stats

    @staticmethod
    def get_empty_statistics() -> Dict[str, int]:
        """Retourne un dictionnaire de statistiques vide.

        Returns:
            Dictionnaire de statistiques initialisé à zéro
        """
        return {
            'nb_appels_total': 0,
            'nb_appels_recus': 0,
            'nb_appels_emis': 0,

            'nb_appels_internes': 0,
            'nb_appel_interne_emis': 0,
            'nb_appel_interne_recus': 0,

            'nb_appels_manques': 0,
            'nb_appels_externes_manques': 0,
            'nb_appels_internes_manques': 0,
            'nb_appels_internes_repondus': 0,

            'nb_appels_aboutis': 0,
            'nb_appels_externes_aboutis': 0,
            'nb_appels_internes_aboutis': 0,

            'duree_appels_total': 0,

            'duree_appels_recus': 0,
            'duree_appels_internes_recus': 0,
            'duree_appels_externes_recus': 0,

            'duree_appels_emis': 0,
            'duree_appels_internes_emis': 0,
            'duree_appels_externes_emis': 0,

            'duree_moyenne_appels': 0,
            'duree_moyenne_appels_internes': 0,
            'duree_moyenne_appels_externes': 0,

            'nb_renvois_appels_recus': 0,
            'duree_renvois_appels_recus': 0,

            'nb_renvois_appels_emis': 0,
            'duree_renvois_appels_emis': 0,

            'nb_click_to_call': 0,
        }

    @staticmethod
    def calculate_hourly_statistics(df: pd.DataFrame) -> pd.DataFrame:
        """Calcule les statistiques d'appels par heure.

        Args:
            df: DataFrame contenant les données d'appels

        Returns:
            DataFrame contenant les statistiques par heure
        """
        if df.empty:
            return pd.DataFrame()

        # Création d'une colonne pour l'heure
        df['hour'] = df['call_date'].dt.hour

        # Groupement par heure
        hourly_stats = df.groupby('hour').agg(
            nb_appels=('uniqueid', 'count'),
            nb_appels_repondus=('answered', 'sum'),
            duree_totale=('billsec', 'sum'),
            duree_moyenne=('billsec', lambda x: x[x > 0].mean() if len(x[x > 0]) > 0 else 0)
        ).reset_index()

        # Arrondi des valeurs
        hourly_stats['duree_moyenne'] = hourly_stats['duree_moyenne'].round(0).astype(int)

        return hourly_stats

    @staticmethod
    def calculate_daily_statistics(df: pd.DataFrame) -> pd.DataFrame:
        """Calcule les statistiques d'appels par jour.

        Args:
            df: DataFrame contenant les données d'appels

        Returns:
            DataFrame contenant les statistiques par jour
        """
        if df.empty:
            return pd.DataFrame()

        # Création d'une colonne pour la date
        df['date'] = df['call_date'].dt.date

        # Groupement par date
        daily_stats = df.groupby('date').agg(
            nb_appels=('uniqueid', 'count'),
            nb_appels_recus=('type_appel', lambda x: (x == 'entrant').sum()),
            nb_appels_emis=('type_appel', lambda x: (x == 'sortant').sum()),
            nb_appels_repondus=('answered', 'sum'),
            duree_totale=('billsec', 'sum'),
            duree_moyenne=('billsec', lambda x: x[x > 0].mean() if len(x[x > 0]) > 0 else 0)
        ).reset_index()

        # Arrondi des valeurs
        daily_stats['duree_moyenne'] = daily_stats['duree_moyenne'].round(0).astype(int)

        return daily_stats

    @staticmethod
    def top_destinations(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
        """Identifie les principales destinations d'appels.

        Args:
            df: DataFrame contenant les données d'appels
            top_n: Nombre de destinations à retourner

        Returns:
            DataFrame contenant les principales destinations
        """
        if df.empty:
            return pd.DataFrame()

        # Filtrage des appels sortants
        df_out = df[df['type_appel'] == 'sortant']

        if df_out.empty:
            return pd.DataFrame()

        # Groupement par destination
        top_dest = df_out.groupby('dst').agg(
            nb_appels=('uniqueid', 'count'),
            nb_repondus=('answered', 'sum'),
            duree_totale=('billsec', 'sum'),
            duree_moyenne=('billsec', lambda x: x[x > 0].mean() if len(x[x > 0]) > 0 else 0)
        ).reset_index()

        # Ajout du taux de réponse
        top_dest['taux_reponse'] = (top_dest['nb_repondus'] / top_dest['nb_appels'] * 100).round(1)

        # Arrondi des valeurs
        top_dest['duree_moyenne'] = top_dest['duree_moyenne'].round(0).astype(int)

        # Tri et sélection des N premiers
        return top_dest.sort_values('nb_appels', ascending=False).head(top_n)

    @staticmethod
    def top_sources(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
        """Identifie les principales sources d'appels.

        Args:
            df: DataFrame contenant les données d'appels
            top_n: Nombre de sources à retourner

        Returns:
            DataFrame contenant les principales sources
        """
        if df.empty:
            return pd.DataFrame()

        # Filtrage des appels entrants
        df_in = df[df['type_appel'] == 'entrant']

        if df_in.empty:
            return pd.DataFrame()

        # Groupement par source
        top_src = df_in.groupby('src').agg(
            nb_appels=('uniqueid', 'count'),
            nb_repondus=('answered', 'sum'),
            duree_totale=('billsec', 'sum'),
            duree_moyenne=('billsec', lambda x: x[x > 0].mean() if len(x[x > 0]) > 0 else 0)
        ).reset_index()

        # Ajout du taux de réponse
        top_src['taux_reponse'] = (top_src['nb_repondus'] / top_src['nb_appels'] * 100).round(1)

        # Arrondi des valeurs
        top_src['duree_moyenne'] = top_src['duree_moyenne'].round(0).astype(int)

        # Tri et sélection des N premiers
        return top_src.sort_values('nb_appels', ascending=False).head(top_n)