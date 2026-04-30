import logging
import os
from typing import Dict, Optional, Union

import pandas as pd

from ..services.statistics import StatisticsGenerator

logger = logging.getLogger(__name__)


class ExcelExporter:
    """Exporte les données et statistiques vers des fichiers Excel."""

    @staticmethod
    def format_duration(seconds: int) -> str:
        if pd.isna(seconds) or seconds == 0:
            return "00:00:00"
        h, rem = divmod(int(seconds), 3600)
        m, s = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    @staticmethod
    def _format_duration_series(series: pd.Series) -> pd.Series:
        """Vectorised version of format_duration for a whole column."""
        sec = series.fillna(0).astype(int)
        h = sec // 3600
        m = (sec % 3600) // 60
        s = sec % 60
        return h.map('{:02d}'.format) + ':' + m.map('{:02d}'.format) + ':' + s.map('{:02d}'.format)

    @staticmethod
    def _auto_column_widths(worksheet, df: pd.DataFrame):
        """Ajuste les largeurs de colonnes en une passe vectorisée."""
        for i, col in enumerate(df.columns):
            # str.len() is vectorised; max of header vs data
            max_len = max(
                df[col].astype(str).str.len().max(),
                len(str(col))
            ) + 2
            worksheet.column_dimensions[chr(65 + i)].width = min(int(max_len), 50)

    @staticmethod
    def export_calls_to_excel(df: pd.DataFrame, filename: str,
                               extensions_dict: Optional[Dict[str, str]] = None) -> str:
        if df.empty:
            logger.warning("Aucune donnée à exporter vers Excel.")
            return ""

        export_df = df.copy()
        export_df['duree'] = ExcelExporter._format_duration_series(export_df['billsec'])

        if extensions_dict:
            export_df['src_name'] = export_df['src'].map(extensions_dict)
            export_df['dst_name'] = export_df['dst'].map(extensions_dict)

        columns_mapping = {
            'call_date': 'Date et heure',
            'src': 'Source',
            'original_caller_name': 'Nom appelant',
            'dst': 'Destination',
            'dst_name': 'Nom destination',
            'duree': 'Durée',
            'status': 'Statut',
            'type_appel': 'Type',
            'path': "Chemin d'appel",
            'renvoi_depuis': 'Renvoi depuis',
            'renvoi_vers': 'Renvoi vers',
            'transfert_depuis': 'Transfert depuis',
            'transfert_vers': 'Transfert vers',
            'did': 'DID',
        }

        export_columns = [c for c in columns_mapping if c in export_df.columns]
        export_df = export_df[export_columns].rename(columns={c: columns_mapping[c] for c in export_columns})

        os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)

        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                export_df.to_excel(writer, sheet_name='Appels', index=False)
                ExcelExporter._auto_column_widths(writer.sheets['Appels'], export_df)
            logger.info(f"Données exportées avec succès vers {filename}")
            return filename
        except Exception as e:
            logger.error(f"Erreur lors de l'export vers Excel: {e}")
            return ""

    @staticmethod
    def export_statistics_to_excel(df: pd.DataFrame, statistics: Dict[str, Union[int, float]],
                                    filename: str, period: str) -> str:
        if df.empty:
            logger.warning("Aucune donnée à exporter vers Excel.")
            return ""

        hourly_stats = StatisticsGenerator.calculate_hourly_statistics(df)
        daily_stats = StatisticsGenerator.calculate_daily_statistics(df)
        top_dest = StatisticsGenerator.top_destinations(df)
        top_src = StatisticsGenerator.top_sources(df)

        fmt = ExcelExporter.format_duration
        nb_recus = statistics['nb_appels_recus']
        nb_manques = statistics['nb_appels_manques']

        global_stats_df = pd.DataFrame([
            {'Métrique': 'Période', 'Valeur': period},
            {'Métrique': "Nombre total d'appels", 'Valeur': statistics['nb_appels_total']},
            {'Métrique': 'Appels reçus', 'Valeur': nb_recus},
            {'Métrique': 'Appels reçus internes', 'Valeur': statistics['nb_appel_interne_recus']},
            {'Métrique': 'Appels reçus externes', 'Valeur': nb_recus - statistics['nb_appel_interne_recus']},
            {'Métrique': 'Appels émis', 'Valeur': statistics['nb_appels_emis']},
            {'Métrique': 'Appels émis internes', 'Valeur': statistics['nb_appel_interne_emis']},
            {'Métrique': 'Appels émis externes', 'Valeur': statistics['nb_appels_emis'] - statistics['nb_appel_interne_emis']},
            {'Métrique': 'Appels internes', 'Valeur': statistics['nb_appels_internes']},
            {'Métrique': 'Appels manqués', 'Valeur': nb_manques},
            {'Métrique': 'Appels manqués internes', 'Valeur': statistics['nb_appels_internes_manques']},
            {'Métrique': 'Appels manqués externes', 'Valeur': statistics['nb_appels_externes_manques']},
            {'Métrique': 'Taux de réponse',
             'Valeur': f"{(nb_recus - nb_manques) / nb_recus * 100:.1f}%" if nb_recus > 0 else "N/A"},
            {'Métrique': 'Durée totale des appels', 'Valeur': fmt(statistics['duree_appels_total'])},
            {'Métrique': 'Durée des appels reçus', 'Valeur': fmt(statistics['duree_appels_recus'])},
            {'Métrique': 'Durée des appels reçus internes', 'Valeur': fmt(statistics['duree_appels_internes_recus'])},
            {'Métrique': 'Durée des appels reçus externes', 'Valeur': fmt(statistics['duree_appels_externes_recus'])},
            {'Métrique': 'Durée des appels émis', 'Valeur': fmt(statistics['duree_appels_emis'])},
            {'Métrique': 'Durée des appels émis internes', 'Valeur': fmt(statistics['duree_appels_internes_emis'])},
            {'Métrique': 'Durée des appels émis externes', 'Valeur': fmt(statistics['duree_appels_externes_emis'])},
            {'Métrique': 'Durée moyenne des appels', 'Valeur': fmt(statistics['duree_moyenne_appels'])},
            {'Métrique': 'Durée moyenne internes', 'Valeur': fmt(statistics['duree_moyenne_appels_internes'])},
            {'Métrique': 'Durée moyenne externes', 'Valeur': fmt(statistics['duree_moyenne_appels_externes'])},
            {'Métrique': 'Appels internes émis aboutis', 'Valeur': statistics['nb_appels_internes_aboutis']},
            {'Métrique': 'Appels externes émis aboutis', 'Valeur': statistics['nb_appels_externes_aboutis']},
            {'Métrique': 'Appels internes reçus répondus', 'Valeur': statistics['nb_appels_internes_repondus']},
            {'Métrique': "Nombre de renvois d'appels", 'Valeur': statistics['nb_renvois_appels_recus']},
            {'Métrique': "Durée des renvois d'appels", 'Valeur': fmt(statistics['duree_renvois_appels_recus'])},
            {'Métrique': 'Appels Click-to-Call', 'Valeur': statistics['nb_click_to_call']},
        ])

        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                global_stats_df.to_excel(writer, sheet_name='Stats Globales', index=False)
                ExcelExporter._auto_column_widths(writer.sheets['Stats Globales'], global_stats_df)

                if not hourly_stats.empty:
                    h = hourly_stats.copy()
                    h['duree_totale_format'] = ExcelExporter._format_duration_series(h['duree_totale'])
                    h['duree_moyenne_format'] = ExcelExporter._format_duration_series(h['duree_moyenne'])
                    h['taux_reponse'] = (h['nb_appels_repondus'] / h['nb_appels'] * 100).round(1)
                    h['heure_format'] = h['hour'].apply(lambda x: f"{x:02d}h-{x + 1:02d}h")
                    out = h[['heure_format', 'nb_appels', 'nb_appels_repondus', 'taux_reponse',
                              'duree_totale_format', 'duree_moyenne_format']]
                    out.columns = ['Heure', 'Nb Appels', 'Nb Répondus', 'Taux Réponse (%)', 'Durée Totale', 'Durée Moyenne']
                    out.to_excel(writer, sheet_name='Stats par Heure', index=False)
                    ExcelExporter._auto_column_widths(writer.sheets['Stats par Heure'], out)

                if not daily_stats.empty:
                    d = daily_stats.copy()
                    d['duree_totale_format'] = ExcelExporter._format_duration_series(d['duree_totale'])
                    d['duree_moyenne_format'] = ExcelExporter._format_duration_series(d['duree_moyenne'])
                    d['taux_reponse'] = (d['nb_appels_repondus'] / d['nb_appels'] * 100).round(1)
                    d['date_format'] = pd.to_datetime(d['date']).dt.strftime('%d/%m/%Y')
                    out = d[['date_format', 'nb_appels', 'nb_appels_recus', 'nb_appels_emis',
                              'nb_appels_repondus', 'taux_reponse', 'duree_totale_format', 'duree_moyenne_format']]
                    out.columns = ['Date', 'Nb Appels', 'Nb Reçus', 'Nb Émis', 'Nb Répondus',
                                   'Taux Réponse (%)', 'Durée Totale', 'Durée Moyenne']
                    out.to_excel(writer, sheet_name='Stats par Jour', index=False)
                    ExcelExporter._auto_column_widths(writer.sheets['Stats par Jour'], out)

                if not top_dest.empty:
                    td = top_dest.copy()
                    td['duree_totale_format'] = ExcelExporter._format_duration_series(td['duree_totale'])
                    td['duree_moyenne_format'] = ExcelExporter._format_duration_series(td['duree_moyenne'])
                    out = td[['dst', 'nb_appels', 'nb_repondus', 'taux_reponse', 'duree_totale_format', 'duree_moyenne_format']]
                    out.columns = ['Destination', 'Nb Appels', 'Nb Répondus', 'Taux Réponse (%)', 'Durée Totale', 'Durée Moyenne']
                    out.to_excel(writer, sheet_name='Top Destinations', index=False)
                    ExcelExporter._auto_column_widths(writer.sheets['Top Destinations'], out)

                if not top_src.empty:
                    ts = top_src.copy()
                    ts['duree_totale_format'] = ExcelExporter._format_duration_series(ts['duree_totale'])
                    ts['duree_moyenne_format'] = ExcelExporter._format_duration_series(ts['duree_moyenne'])
                    out = ts[['src', 'nb_appels', 'nb_repondus', 'taux_reponse', 'duree_totale_format', 'duree_moyenne_format']]
                    out.columns = ['Source', 'Nb Appels', 'Nb Répondus', 'Taux Réponse (%)', 'Durée Totale', 'Durée Moyenne']
                    out.to_excel(writer, sheet_name='Top Sources', index=False)
                    ExcelExporter._auto_column_widths(writer.sheets['Top Sources'], out)

            logger.info(f"Statistiques exportées avec succès vers {filename}")
            return filename
        except Exception as e:
            logger.error(f"Erreur lors de l'export des statistiques vers Excel: {e}")
            return ""
