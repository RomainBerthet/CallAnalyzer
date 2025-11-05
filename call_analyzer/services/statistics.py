import logging
from typing import Dict, List, Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)

class StatisticsGenerator:
    """Génère des statistiques à partir des données d'appels."""

    @staticmethod
    def calculate_statistics(df: pd.DataFrame, reference_numbers: Optional[List[str]] = None) -> Dict[
        str, Union[int, float]]:
        """
        Calcule les statistiques complètes à partir d'un DataFrame d'appels.

        Inclut toutes les métriques possibles: appels, durées, SLA, queues, voicemail, etc.

        Args:
            df: DataFrame contenant les données d'appels
            reference_numbers: Liste des numéros de référence (optionnel)

        Returns:
            Dictionnaire contenant les statistiques complètes
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
            # === STATISTIQUES DE BASE ===
            'nb_appels_total': len(df),
            'nb_appels_recus': len(df[df['type_appel'] == 'entrant']),
            'nb_appels_emis': len(df[df['type_appel'] == 'sortant']),

            # Appels internes
            'nb_appels_internes': len(df[df['is_internal']]),
            'nb_appel_interne_emis': len(df[(df['type_appel'] == 'sortant') & df['is_internal']]),
            'nb_appel_interne_recus': len(df[(df['type_appel'] == 'entrant') & df['is_internal']]),

            # Appels manqués
            'nb_appels_manques': len(df[(df['type_appel'] == 'entrant') & (~df['answered'])]),
            'nb_appels_externes_manques': len(df[(df['type_appel'] == 'entrant') & (~df['answered']) & (~df['is_internal'])]),
            'nb_appels_internes_manques': len(df[(df['type_appel'] == 'entrant') & (~df['answered']) & df['is_internal']]),
            'nb_appels_internes_repondus': len(df[(df['type_appel'] == 'entrant') & df['answered'] & df['is_internal']]),

            # Appels aboutis
            'nb_appels_aboutis': len(df[(df['type_appel'] == 'sortant') & df['answered']]),
            'nb_appels_externes_aboutis': len(df[(df['type_appel'] == 'sortant') & df['answered'] & (~df['is_internal'])]),
            'nb_appels_internes_aboutis': len(df[(df['type_appel'] == 'sortant') & df['answered'] & df['is_internal']]),

            # === DURÉES ===
            'duree_appels_total': int(df['billsec'].sum()),

            # Durées reçus
            'duree_appels_recus': int(df.loc[df['type_appel'] == 'entrant', 'billsec'].sum()),
            'duree_appels_internes_recus': int(df.loc[(df['type_appel'] == 'entrant') & df['is_internal'], 'billsec'].sum()),
            'duree_appels_externes_recus': int(df.loc[(df['type_appel'] == 'entrant') & (~df['is_internal']), 'billsec'].sum()),

            # Durées émis
            'duree_appels_emis': int(df.loc[df['type_appel'] == 'sortant', 'billsec'].sum()),
            'duree_appels_internes_emis': int(df.loc[(df['type_appel'] == 'sortant') & df['is_internal'], 'billsec'].sum()),
            'duree_appels_externes_emis': int(df.loc[(df['type_appel'] == 'sortant') & (~df['is_internal']), 'billsec'].sum()),

            # Durées moyennes
            'duree_moyenne_appels': int(df.loc[df['answered'], 'billsec'].mean()) if len(df[df['answered']]) > 0 else 0,
            'duree_moyenne_appels_internes': int(df.loc[(df['answered']) & df['is_internal'], 'billsec'].mean()) if len(df[(df['answered']) & df['is_internal']]) > 0 else 0,
            'duree_moyenne_appels_externes': int(df.loc[(df['answered']) & (~df['is_internal']), 'billsec'].mean()) if len(df[(df['answered']) & (~df['is_internal'])]) > 0 else 0,

            # === RENVOIS ET TRANSFERTS ===
            'nb_renvois_appels_recus': len(df[(df['type_appel'] == 'entrant') & df['renvoi_vers'].notna()]) if 'renvoi_vers' in df.columns else 0,
            'duree_renvois_appels_recus': int(df.loc[(df['type_appel'] == 'entrant') & df['renvoi_vers'].notna(), 'billsec'].sum()) if 'renvoi_vers' in df.columns else 0,

            'nb_transferts': len(df[df['has_transfer']]) if 'has_transfer' in df.columns else 0,
            'nb_forwards': len(df[df['has_forward']]) if 'has_forward' in df.columns else 0,

            # === CLICK-TO-CALL ===
            'nb_click_to_call': len(df[df['is_click_to_call']]),

            # === TEMPS D'ATTENTE ET SLA ===
            # Temps d'attente moyens
            'temps_attente_moyen': int(df.loc[df['wait_time'].notna(), 'wait_time'].mean()) if 'wait_time' in df.columns and len(df[df['wait_time'].notna()]) > 0 else 0,
            'temps_attente_max': int(df.loc[df['wait_time'].notna(), 'wait_time'].max()) if 'wait_time' in df.columns and len(df[df['wait_time'].notna()]) > 0 else 0,
            'temps_attente_min': int(df.loc[df['wait_time'].notna(), 'wait_time'].min()) if 'wait_time' in df.columns and len(df[df['wait_time'].notna()]) > 0 else 0,

            # SLA (Service Level Agreement) - % d'appels répondus en moins de X secondes
            'sla_20s_count': len(df[df['sla_compliant_20s']]) if 'sla_compliant_20s' in df.columns else 0,
            'sla_20s_percent': round(len(df[df['sla_compliant_20s']]) / len(df[df['type_appel'] == 'entrant']) * 100, 2) if 'sla_compliant_20s' in df.columns and len(df[df['type_appel'] == 'entrant']) > 0 else 0,

            'sla_30s_count': len(df[df['sla_compliant_30s']]) if 'sla_compliant_30s' in df.columns else 0,
            'sla_30s_percent': round(len(df[df['sla_compliant_30s']]) / len(df[df['type_appel'] == 'entrant']) * 100, 2) if 'sla_compliant_30s' in df.columns and len(df[df['type_appel'] == 'entrant']) > 0 else 0,

            # === MESSAGERIE VOCALE ===
            'nb_appels_voicemail': len(df[df['went_to_voicemail']]) if 'went_to_voicemail' in df.columns else 0,
            'taux_voicemail': round(len(df[df['went_to_voicemail']]) / len(df) * 100, 2) if 'went_to_voicemail' in df.columns and len(df) > 0 else 0,

            # === QUEUES (FILES D'ATTENTE) ===
            'nb_appels_queue': len(df[df['queue_name'].notna()]) if 'queue_name' in df.columns else 0,
            'temps_attente_queue_moyen': int(df.loc[df['queue_wait_time'].notna(), 'queue_wait_time'].mean()) if 'queue_wait_time' in df.columns and len(df[df['queue_wait_time'].notna()]) > 0 else 0,
            'temps_attente_queue_max': int(df.loc[df['queue_wait_time'].notna(), 'queue_wait_time'].max()) if 'queue_wait_time' in df.columns and len(df[df['queue_wait_time'].notna()]) > 0 else 0,

            # === TAUX DE RÉPONSE ===
            'taux_reponse_global': round((len(df[df['answered']]) / len(df[df['type_appel'] == 'entrant']) * 100), 2) if len(df[df['type_appel'] == 'entrant']) > 0 else 0,
            'taux_reponse_externe': round((len(df[(df['answered']) & (~df['is_internal']) & (df['type_appel'] == 'entrant')]) / len(df[(~df['is_internal']) & (df['type_appel'] == 'entrant')]) * 100), 2) if len(df[(~df['is_internal']) & (df['type_appel'] == 'entrant')]) > 0 else 0,

            # === MÉTRIQUES AVANCÉES ===
            'nb_participants_moyen': round(df['total_participants'].mean(), 1) if 'total_participants' in df.columns else 0,
            'nb_events_moyen': round(df['event_count'].mean(), 1) if 'event_count' in df.columns else 0,
        }

        return stats

    @staticmethod
    def get_empty_statistics() -> Dict[str, Union[int, float]]:
        """
        Retourne un dictionnaire de statistiques vide avec toutes les métriques.

        Returns:
            Dictionnaire de statistiques initialisé à zéro
        """
        return {
            # Statistiques de base
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

            # Durées
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

            # Renvois et transferts
            'nb_renvois_appels_recus': 0,
            'duree_renvois_appels_recus': 0,
            'nb_transferts': 0,
            'nb_forwards': 0,

            # Click-to-call
            'nb_click_to_call': 0,

            # Temps d'attente et SLA
            'temps_attente_moyen': 0,
            'temps_attente_max': 0,
            'temps_attente_min': 0,
            'sla_20s_count': 0,
            'sla_20s_percent': 0.0,
            'sla_30s_count': 0,
            'sla_30s_percent': 0.0,

            # Messagerie vocale
            'nb_appels_voicemail': 0,
            'taux_voicemail': 0.0,

            # Queues
            'nb_appels_queue': 0,
            'temps_attente_queue_moyen': 0,
            'temps_attente_queue_max': 0,

            # Taux de réponse
            'taux_reponse_global': 0.0,
            'taux_reponse_externe': 0.0,

            # Métriques avancées
            'nb_participants_moyen': 0.0,
            'nb_events_moyen': 0.0,
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

    @staticmethod
    def calculate_queue_statistics(df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcule les statistiques par queue (file d'attente).

        Args:
            df: DataFrame contenant les données d'appels

        Returns:
            DataFrame contenant les statistiques par queue
        """
        if df.empty or 'queue_name' not in df.columns:
            return pd.DataFrame()

        # Filtrage des appels avec queue
        df_queue = df[df['queue_name'].notna()]

        if df_queue.empty:
            return pd.DataFrame()

        # Groupement par queue
        queue_stats = df_queue.groupby('queue_name').agg(
            nb_appels=('uniqueid', 'count'),
            nb_repondus=('answered', 'sum'),
            duree_totale=('billsec', 'sum'),
            duree_moyenne=('billsec', lambda x: x[x > 0].mean() if len(x[x > 0]) > 0 else 0),
            temps_attente_moyen=('queue_wait_time', lambda x: x[x.notna()].mean() if len(x[x.notna()]) > 0 else 0),
            temps_attente_max=('queue_wait_time', lambda x: x[x.notna()].max() if len(x[x.notna()]) > 0 else 0)
        ).reset_index()

        # Ajout du taux de réponse
        queue_stats['taux_reponse'] = (queue_stats['nb_repondus'] / queue_stats['nb_appels'] * 100).round(1)

        # Arrondi des valeurs
        queue_stats['duree_moyenne'] = queue_stats['duree_moyenne'].round(0).astype(int)
        queue_stats['temps_attente_moyen'] = queue_stats['temps_attente_moyen'].round(0).astype(int)
        queue_stats['temps_attente_max'] = queue_stats['temps_attente_max'].round(0).astype(int)

        return queue_stats.sort_values('nb_appels', ascending=False)

    @staticmethod
    def calculate_sla_statistics(df: pd.DataFrame, thresholds: List[int] = None) -> pd.DataFrame:
        """
        Calcule les statistiques SLA pour différents seuils.

        Args:
            df: DataFrame contenant les données d'appels
            thresholds: Liste des seuils en secondes (défaut: [10, 20, 30, 60])

        Returns:
            DataFrame contenant les statistiques SLA
        """
        if df.empty or 'wait_time' not in df.columns:
            return pd.DataFrame()

        if thresholds is None:
            thresholds = [10, 20, 30, 60]

        # Filtrage des appels entrants répondus avec wait_time
        df_sla = df[(df['type_appel'] == 'entrant') & (df['answered']) & (df['wait_time'].notna())]

        if df_sla.empty:
            return pd.DataFrame()

        total_calls = len(df_sla)
        sla_data = []

        for threshold in thresholds:
            compliant = len(df_sla[df_sla['wait_time'] <= threshold])
            percentage = round(compliant / total_calls * 100, 2)
            sla_data.append({
                'seuil_secondes': threshold,
                'nb_conformes': compliant,
                'nb_total': total_calls,
                'pourcentage': percentage
            })

        return pd.DataFrame(sla_data)

    @staticmethod
    def calculate_wait_time_distribution(df: pd.DataFrame, bins: List[int] = None) -> pd.DataFrame:
        """
        Calcule la distribution des temps d'attente par tranches.

        Args:
            df: DataFrame contenant les données d'appels
            bins: Liste des limites de tranches en secondes (défaut: [0, 10, 20, 30, 60, float('inf')])

        Returns:
            DataFrame contenant la distribution
        """
        if df.empty or 'wait_time' not in df.columns:
            return pd.DataFrame()

        if bins is None:
            bins = [0, 10, 20, 30, 60, float('inf')]

        # Filtrage des appels avec wait_time
        df_wait = df[df['wait_time'].notna()]

        if df_wait.empty:
            return pd.DataFrame()

        # Création des tranches
        labels = []
        for i in range(len(bins) - 1):
            if bins[i + 1] == float('inf'):
                labels.append(f"> {bins[i]}s")
            else:
                labels.append(f"{bins[i]}-{bins[i + 1]}s")

        df_wait['tranche'] = pd.cut(df_wait['wait_time'], bins=bins, labels=labels, right=False)

        # Calcul de la distribution
        distribution = df_wait.groupby('tranche', observed=False).agg(
            nb_appels=('uniqueid', 'count')
        ).reset_index()

        total = distribution['nb_appels'].sum()
        distribution['pourcentage'] = (distribution['nb_appels'] / total * 100).round(2)

        return distribution