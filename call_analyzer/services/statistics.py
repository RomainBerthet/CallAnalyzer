import logging
from typing import Dict, List, Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)


class StatisticsGenerator:
    """Génère des statistiques à partir des données d'appels."""

    @staticmethod
    def calculate_statistics(df: pd.DataFrame, reference_numbers: Optional[List[str]] = None) -> Dict[
            str, Union[int, float]]:
        if df.empty:
            return StatisticsGenerator.get_empty_statistics()

        if not reference_numbers:
            df = df[~df['is_internal']]

        if df.empty:
            return StatisticsGenerator.get_empty_statistics()

        # Pre-compute boolean masks once — each mask reused many times below
        answered = df['answered']
        entrant = df['type_appel'] == 'entrant'
        sortant = df['type_appel'] == 'sortant'
        internal = df['is_internal']
        not_internal = ~internal
        not_answered = ~answered
        has_forward = df['renvoi_vers'].notna()

        billsec = df['billsec']

        def _mean_billsec(mask) -> int:
            vals = billsec[mask & answered]
            return int(vals.mean()) if len(vals) > 0 else 0

        return {
            'nb_appels_total': len(df),
            'nb_appels_recus': int(entrant.sum()),
            'nb_appels_emis': int(sortant.sum()),

            'nb_appels_internes': int(internal.sum()),
            'nb_appel_interne_emis': int((sortant & internal).sum()),
            'nb_appel_interne_recus': int((entrant & internal).sum()),

            'nb_appels_manques': int((entrant & not_answered).sum()),
            'nb_appels_externes_manques': int((entrant & not_answered & not_internal).sum()),
            'nb_appels_internes_manques': int((entrant & not_answered & internal).sum()),
            'nb_appels_internes_repondus': int((entrant & answered & internal).sum()),

            'nb_appels_aboutis': int((sortant & answered).sum()),
            'nb_appels_externes_aboutis': int((sortant & answered & not_internal).sum()),
            'nb_appels_internes_aboutis': int((sortant & answered & internal).sum()),

            'duree_appels_total': int(billsec.sum()),

            'duree_appels_recus': int(billsec[entrant].sum()),
            'duree_appels_internes_recus': int(billsec[entrant & internal].sum()),
            'duree_appels_externes_recus': int(billsec[entrant & not_internal].sum()),

            'duree_appels_emis': int(billsec[sortant].sum()),
            'duree_appels_internes_emis': int(billsec[sortant & internal].sum()),
            'duree_appels_externes_emis': int(billsec[sortant & not_internal].sum()),

            'duree_moyenne_appels': int(billsec[answered].mean()) if answered.any() else 0,
            'duree_moyenne_appels_internes': _mean_billsec(internal),
            'duree_moyenne_appels_externes': _mean_billsec(not_internal),

            'nb_renvois_appels_recus': int((entrant & has_forward).sum()),
            'duree_renvois_appels_recus': int(billsec[entrant & has_forward].sum()),

            'nb_renvois_appels_emis': int((sortant & has_forward).sum()),
            'duree_renvois_appels_emis': int(billsec[sortant & has_forward].sum()),

            'nb_click_to_call': int(df['is_click_to_call'].sum()),
        }

    @staticmethod
    def get_empty_statistics() -> Dict[str, int]:
        return {
            'nb_appels_total': 0, 'nb_appels_recus': 0, 'nb_appels_emis': 0,
            'nb_appels_internes': 0, 'nb_appel_interne_emis': 0, 'nb_appel_interne_recus': 0,
            'nb_appels_manques': 0, 'nb_appels_externes_manques': 0,
            'nb_appels_internes_manques': 0, 'nb_appels_internes_repondus': 0,
            'nb_appels_aboutis': 0, 'nb_appels_externes_aboutis': 0, 'nb_appels_internes_aboutis': 0,
            'duree_appels_total': 0,
            'duree_appels_recus': 0, 'duree_appels_internes_recus': 0, 'duree_appels_externes_recus': 0,
            'duree_appels_emis': 0, 'duree_appels_internes_emis': 0, 'duree_appels_externes_emis': 0,
            'duree_moyenne_appels': 0, 'duree_moyenne_appels_internes': 0, 'duree_moyenne_appels_externes': 0,
            'nb_renvois_appels_recus': 0, 'duree_renvois_appels_recus': 0,
            'nb_renvois_appels_emis': 0, 'duree_renvois_appels_emis': 0,
            'nb_click_to_call': 0,
        }

    @staticmethod
    def calculate_hourly_statistics(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        # Group directly on the dt accessor — no copy needed
        hourly = df.groupby(df['call_date'].dt.hour).agg(
            nb_appels=('uniqueid', 'count'),
            nb_appels_repondus=('answered', 'sum'),
            duree_totale=('billsec', 'sum'),
            duree_moyenne=('billsec', lambda x: x[x > 0].mean() if len(x[x > 0]) > 0 else 0)
        ).reset_index().rename(columns={'call_date': 'hour'})

        hourly['duree_moyenne'] = hourly['duree_moyenne'].round(0).astype(int)
        return hourly

    @staticmethod
    def calculate_daily_statistics(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        daily = df.groupby(df['call_date'].dt.date).agg(
            nb_appels=('uniqueid', 'count'),
            nb_appels_recus=('type_appel', lambda x: (x == 'entrant').sum()),
            nb_appels_emis=('type_appel', lambda x: (x == 'sortant').sum()),
            nb_appels_repondus=('answered', 'sum'),
            duree_totale=('billsec', 'sum'),
            duree_moyenne=('billsec', lambda x: x[x > 0].mean() if len(x[x > 0]) > 0 else 0)
        ).reset_index().rename(columns={'call_date': 'date'})

        daily['duree_moyenne'] = daily['duree_moyenne'].round(0).astype(int)
        return daily

    @staticmethod
    def top_destinations(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        df_out = df[df['type_appel'] == 'sortant']
        if df_out.empty:
            return pd.DataFrame()

        top = df_out.groupby('dst').agg(
            nb_appels=('uniqueid', 'count'),
            nb_repondus=('answered', 'sum'),
            duree_totale=('billsec', 'sum'),
            duree_moyenne=('billsec', lambda x: x[x > 0].mean() if len(x[x > 0]) > 0 else 0)
        ).reset_index()

        top['taux_reponse'] = (top['nb_repondus'] / top['nb_appels'] * 100).round(1)
        top['duree_moyenne'] = top['duree_moyenne'].round(0).astype(int)
        return top.sort_values('nb_appels', ascending=False).head(top_n)

    @staticmethod
    def top_sources(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        df_in = df[df['type_appel'] == 'entrant']
        if df_in.empty:
            return pd.DataFrame()

        top = df_in.groupby('src').agg(
            nb_appels=('uniqueid', 'count'),
            nb_repondus=('answered', 'sum'),
            duree_totale=('billsec', 'sum'),
            duree_moyenne=('billsec', lambda x: x[x > 0].mean() if len(x[x > 0]) > 0 else 0)
        ).reset_index()

        top['taux_reponse'] = (top['nb_repondus'] / top['nb_appels'] * 100).round(1)
        top['duree_moyenne'] = top['duree_moyenne'].round(0).astype(int)
        return top.sort_values('nb_appels', ascending=False).head(top_n)
