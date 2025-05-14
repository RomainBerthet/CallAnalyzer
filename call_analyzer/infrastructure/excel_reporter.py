import logging
import os
from typing import Dict, Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)

class ExcelExporter:
    """Exporte les données et statistiques vers des fichiers Excel."""

    @staticmethod
    def format_duration(seconds: int) -> str:
        """Formate une durée en secondes en format HH:MM:SS.

        Args:
            seconds: Nombre de secondes

        Returns:
            Durée formatée
        """
        if pd.isna(seconds) or seconds == 0:
            return "00:00:00"

        hours, remainder = divmod(int(seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    @staticmethod
    def export_calls_to_excel(df: pd.DataFrame, filename: str, extensions_dict: Optional[Dict[str, str]] = None) -> str:
        """Exporte les données d'appels vers un fichier Excel.

        Args:
            df: DataFrame contenant les données d'appels
            filename: Nom du fichier Excel
            extensions_dict: Dictionnaire des extensions et leurs noms

        Returns:
            Chemin du fichier créé
        """
        if df.empty:
            logger.warning("Aucune donnée à exporter vers Excel.")
            return ""

        # Création d'une copie du DataFrame pour éviter de modifier l'original
        export_df = df.copy()

        # Conversion des durées en format lisible
        export_df['duree'] = export_df['billsec'].apply(ExcelExporter.format_duration)

        # Ajout des noms d'extensions si disponibles
        if extensions_dict:
            export_df['src_name'] = export_df['src'].map(extensions_dict)
            export_df['dst_name'] = export_df['dst'].map(extensions_dict)

        # Sélection et renommage des colonnes pour l'export
        columns_mapping = {
            'call_date': 'Date et heure',
            'src': 'Source',
            'src_name': 'Nom source',
            'dst': 'Destination',
            'dst_name': 'Nom destination',
            'duree': 'Durée',
            'status': 'Statut',
            'type_appel': 'Type',
            'path': 'Chemin d\'appel',
            'original_caller_name': 'Nom appelant',
            'renvoi_depuis': 'Renvoi depuis',
            'renvoi_vers': 'Renvoi vers',
            'transfert_depuis': 'Transfert depuis',
            'transfert_vers': 'Transfert vers',
            'did': 'DID',
            'accountcode': 'Code compte',
            'userfield': 'Champ utilisateur'
        }

        # Sélection des colonnes qui existent dans le DataFrame
        export_columns = [col for col in columns_mapping.keys() if col in export_df.columns]

        # Création du DataFrame d'export avec les colonnes sélectionnées et renommées
        export_df = export_df[export_columns].rename(columns={col: columns_mapping[col] for col in export_columns})

        # Ajout du répertoire si nécessaire
        os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)

        # Export vers Excel
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                export_df.to_excel(writer, sheet_name='Appels', index=False)

                # Définition des largeurs de colonnes
                worksheet = writer.sheets['Appels']
                for i, col in enumerate(export_df.columns):
                    max_length = max(
                        export_df[col].astype(str).apply(len).max(),
                        len(col)
                    ) + 2
                    worksheet.column_dimensions[chr(65 + i)].width = min(max_length, 50)

            logger.info(f"Données exportées avec succès vers {filename}")
            return filename
        except Exception as e:
            logger.error(f"Erreur lors de l'export vers Excel: {e}")
            return ""

    @staticmethod
    def export_statistics_to_excel(df: pd.DataFrame, statistics: Dict[str, Union[int, float]],
                                   filename: str, period: str) -> str:
        """Exporte les statistiques d'appels vers un fichier Excel.

        Args:
            df: DataFrame contenant les données d'appels
            statistics: Dictionnaire des statistiques globales
            filename: Nom du fichier Excel
            period: Période des données (ex: "Janvier 2025")

        Returns:
            Chemin du fichier créé
        """
        if df.empty:
            logger.warning("Aucune donnée à exporter vers Excel.")
            return ""

        # Création du DataFrame de statistiques horaires et journalières
        hourly_stats = StatisticsGenerator.calculate_hourly_statistics(df)
        daily_stats = StatisticsGenerator.calculate_daily_statistics(df)
        top_dest = StatisticsGenerator.top_destinations(df)
        top_src = StatisticsGenerator.top_sources(df)

        # Format des statistiques globales pour l'affichage
        stats_for_display = {
            'Statistiques globales': [
                {'Métrique': 'Période', 'Valeur': period},
                {'Métrique': 'Nombre total d\'appels', 'Valeur': statistics['nb_appels_total']},
                {'Métrique': 'Appels reçus', 'Valeur': statistics['nb_appels_recus']},
                {'Métrique': 'Appels émis', 'Valeur': statistics['nb_appels_emis']},
                {'Métrique': 'Appels internes', 'Valeur': statistics['nb_appels_internes']},
                {'Métrique': 'Appels manqués', 'Valeur': statistics['nb_appels_manques']},
                {'Métrique': 'Taux de réponse', 'Valeur': f"{(statistics['nb_appels_recus'] - statistics['nb_appels_manques']) / statistics['nb_appels_recus'] * 100:.1f}%" if statistics['nb_appels_recus'] > 0 else "N/A"},
                {'Métrique': 'Durée totale des appels', 'Valeur': ExcelExporter.format_duration(statistics['duree_appels_total'])},
                {'Métrique': 'Durée des appels reçus', 'Valeur': ExcelExporter.format_duration(statistics['duree_appels_recus'])},
                {'Métrique': 'Durée des appels émis', 'Valeur': ExcelExporter.format_duration(statistics['duree_appels_emis'])},
                {'Métrique': 'Durée moyenne des appels', 'Valeur': ExcelExporter.format_duration(statistics['duree_moyenne_appels'])},
                {'Métrique': 'Appels avec renvoi reçus', 'Valeur': statistics['nb_renvoi_appel_recus']},
                {'Métrique': 'Appels avec renvoi émis', 'Valeur': statistics['nb_renvoi_appel_emis']},
                {'Métrique': 'Appels Click-to-Call', 'Valeur': statistics['nb_click_to_call']}
            ]
        }

        global_stats_df = pd.DataFrame(stats_for_display['Statistiques globales'])

        # Préparation des données horaires pour l'affichage
        if not hourly_stats.empty:
            hourly_stats['duree_totale_format'] = hourly_stats['duree_totale'].apply(ExcelExporter.format_duration)
            hourly_stats['duree_moyenne_format'] = hourly_stats['duree_moyenne'].apply(ExcelExporter.format_duration)
            hourly_stats['taux_reponse'] = (hourly_stats['nb_appels_repondus'] / hourly_stats['nb_appels'] * 100).round(1)
            hourly_stats['heure_format'] = hourly_stats['hour'].apply(lambda x: f"{x:02d}h-{(x + 1):02d}h")

        # Préparation des données journalières pour l'affichage
        if not daily_stats.empty:
            daily_stats['duree_totale_format'] = daily_stats['duree_totale'].apply(ExcelExporter.format_duration)
            daily_stats['duree_moyenne_format'] = daily_stats['duree_moyenne'].apply(ExcelExporter.format_duration)
            daily_stats['taux_reponse'] = (daily_stats['nb_appels_repondus'] / daily_stats['nb_appels'] * 100).round(1)
            daily_stats['date_format'] = pd.to_datetime(daily_stats['date']).dt.strftime('%d/%m/%Y')

        # Export vers Excel
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Feuille des statistiques globales
                global_stats_df.to_excel(writer, sheet_name='Stats Globales', index=False)

                # Feuille des statistiques horaires
                if not hourly_stats.empty:
                    hourly_display = hourly_stats[
                        ['heure_format', 'nb_appels', 'nb_appels_repondus', 'taux_reponse', 'duree_totale_format', 'duree_moyenne_format']]
                    hourly_display.columns = ['Heure', 'Nb Appels', 'Nb Répondus', 'Taux Réponse (%)', 'Durée Totale', 'Durée Moyenne']
                    hourly_display.to_excel(writer, sheet_name='Stats par Heure', index=False)

                # Feuille des statistiques journalières
                if not daily_stats.empty:
                    daily_display = daily_stats[
                        ['date_format', 'nb_appels', 'nb_appels_recus', 'nb_appels_emis', 'nb_appels_repondus', 'taux_reponse', 'duree_totale_format', 'duree_moyenne_format']]
                    daily_display.columns = ['Date', 'Nb Appels', 'Nb Reçus', 'Nb Émis', 'Nb Répondus', 'Taux Réponse (%)', 'Durée Totale', 'Durée Moyenne']
                    daily_display.to_excel(writer, sheet_name='Stats par Jour', index=False)

                # Feuille des principales destinations
                if not top_dest.empty:
                    top_dest['duree_totale_format'] = top_dest['duree_totale'].apply(ExcelExporter.format_duration)
                    top_dest['duree_moyenne_format'] = top_dest['duree_moyenne'].apply(ExcelExporter.format_duration)
                    dest_display = top_dest[['dst', 'nb_appels', 'nb_repondus', 'taux_reponse', 'duree_totale_format', 'duree_moyenne_format']]
                    dest_display.columns = ['Destination', 'Nb Appels', 'Nb Répondus', 'Taux Réponse (%)', 'Durée Totale', 'Durée Moyenne']
                    dest_display.to_excel(writer, sheet_name='Top Destinations', index=False)

                # Feuille des principales sources
                if not top_src.empty:
                    top_src['duree_totale_format'] = top_src['duree_totale'].apply(ExcelExporter.format_duration)
                    top_src['duree_moyenne_format'] = top_src['duree_moyenne'].apply(ExcelExporter.format_duration)
                    src_display = top_src[['src', 'nb_appels', 'nb_repondus', 'taux_reponse', 'duree_totale_format', 'duree_moyenne_format']]
                    src_display.columns = ['Source', 'Nb Appels', 'Nb Répondus', 'Taux Réponse (%)', 'Durée Totale', 'Durée Moyenne']
                    src_display.to_excel(writer, sheet_name='Top Sources', index=False)

                # Ajustement des largeurs de colonnes pour toutes les feuilles
                for sheet_name in writer.sheets:
                    worksheet = writer.sheets[sheet_name]
                    for i, col in enumerate(worksheet.iter_cols(min_row=1, max_row=1)):
                        max_length = len(str(col[0].value)) + 2
                        worksheet.column_dimensions[chr(65 + i)].width = min(max_length, 50)

            logger.info(f"Statistiques exportées avec succès vers {filename}")
            return filename
        except Exception as e:
            logger.error(f"Erreur lors de l'export des statistiques vers Excel: {e}")
            return ""