import logging
import os
from typing import Dict, Optional, Union

import pandas as pd

from call_analyzer.services.statistics import StatisticsGenerator

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
        """
        Exporte les données d'appels vers un fichier Excel avec toutes les informations disponibles.

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

        # Formatage des temps d'attente
        if 'wait_time' in export_df.columns:
            export_df['temps_attente'] = export_df['wait_time'].apply(
                lambda x: f"{int(x)}s" if pd.notna(x) else "")

        if 'queue_wait_time' in export_df.columns:
            export_df['temps_attente_queue'] = export_df['queue_wait_time'].apply(
                lambda x: f"{int(x)}s" if pd.notna(x) else "")

        # Ajout des noms d'extensions si disponibles
        if extensions_dict:
            export_df['src_name'] = export_df['src'].map(extensions_dict)
            export_df['dst_name'] = export_df['dst'].map(extensions_dict)

        # Sélection et renommage des colonnes pour l'export
        columns_mapping = {
            'call_date': 'Date et heure',
            'answer_date': 'Heure réponse',
            'src': 'Source',
            'src_name': 'Nom source',
            'original_caller_name': 'Nom appelant',
            'dst': 'Destination',
            'dst_name': 'Nom destination',
            'duree': 'Durée',
            'temps_attente': 'Temps attente',
            'status': 'Statut',
            'type_appel': 'Type',
            'path': 'Chemin d\'appel',
            'renvoi_depuis': 'Renvoi depuis',
            'renvoi_vers': 'Renvoi vers',
            'transfert_depuis': 'Transfert depuis',
            'transfert_vers': 'Transfert vers',
            'went_to_voicemail': 'Messagerie vocale',
            'queue_name': 'Queue',
            'temps_attente_queue': 'Attente queue',
            'did': 'DID',
            'sla_compliant_20s': 'SLA 20s',
            'is_click_to_call': 'Click-to-Call',
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

        # Format des statistiques globales pour l'affichage (toutes les nouvelles métriques)
        stats_for_display = {
            'Statistiques globales': [
                # En-tête
                {'Métrique': '=== PÉRIODE ===', 'Valeur': ''},
                {'Métrique': 'Période', 'Valeur': period},
                {'Métrique': '', 'Valeur': ''},

                # Volumétrie de base
                {'Métrique': '=== VOLUMÉTRIE ===', 'Valeur': ''},
                {'Métrique': 'Nombre total d\'appels', 'Valeur': statistics['nb_appels_total']},
                {'Métrique': 'Appels reçus', 'Valeur': statistics['nb_appels_recus']},
                {'Métrique': '  - Reçus internes', 'Valeur': statistics['nb_appel_interne_recus']},
                {'Métrique': '  - Reçus externes', 'Valeur': statistics['nb_appels_recus'] - statistics['nb_appel_interne_recus']},
                {'Métrique': 'Appels émis', 'Valeur': statistics['nb_appels_emis']},
                {'Métrique': '  - Émis internes', 'Valeur': statistics['nb_appel_interne_emis']},
                {'Métrique': '  - Émis externes', 'Valeur': statistics['nb_appels_emis'] - statistics['nb_appel_interne_emis']},
                {'Métrique': '', 'Valeur': ''},

                # Appels manqués et taux de réponse
                {'Métrique': '=== APPELS MANQUÉS & TAUX DE RÉPONSE ===', 'Valeur': ''},
                {'Métrique': 'Appels manqués totaux', 'Valeur': statistics['nb_appels_manques']},
                {'Métrique': '  - Manqués internes', 'Valeur': statistics['nb_appels_internes_manques']},
                {'Métrique': '  - Manqués externes', 'Valeur': statistics['nb_appels_externes_manques']},
                {'Métrique': 'Taux de réponse global', 'Valeur': f"{statistics['taux_reponse_global']}%"},
                {'Métrique': 'Taux de réponse externe', 'Valeur': f"{statistics['taux_reponse_externe']}%"},
                {'Métrique': '', 'Valeur': ''},

                # Durées
                {'Métrique': '=== DURÉES ===', 'Valeur': ''},
                {'Métrique': 'Durée totale des appels', 'Valeur': ExcelExporter.format_duration(statistics['duree_appels_total'])},
                {'Métrique': 'Durée moyenne des appels', 'Valeur': ExcelExporter.format_duration(statistics['duree_moyenne_appels'])},
                {'Métrique': 'Durée appels reçus', 'Valeur': ExcelExporter.format_duration(statistics['duree_appels_recus'])},
                {'Métrique': '  - Durée appels reçus internes', 'Valeur': ExcelExporter.format_duration(statistics['duree_appels_internes_recus'])},
                {'Métrique': '  - Durée appels reçus externes', 'Valeur': ExcelExporter.format_duration(statistics['duree_appels_externes_recus'])},
                {'Métrique': 'Durée appels émis', 'Valeur': ExcelExporter.format_duration(statistics['duree_appels_emis'])},
                {'Métrique': '  - Durée appels émis internes', 'Valeur': ExcelExporter.format_duration(statistics['duree_appels_internes_emis'])},
                {'Métrique': '  - Durée appels émis externes', 'Valeur': ExcelExporter.format_duration(statistics['duree_appels_externes_emis'])},
                {'Métrique': '', 'Valeur': ''},

                # SLA (Service Level Agreement)
                {'Métrique': '=== SLA (Service Level Agreement) ===', 'Valeur': ''},
                {'Métrique': 'Appels répondus en < 20s', 'Valeur': f"{statistics['sla_20s_count']} ({statistics['sla_20s_percent']}%)"},
                {'Métrique': 'Appels répondus en < 30s', 'Valeur': f"{statistics['sla_30s_count']} ({statistics['sla_30s_percent']}%)"},
                {'Métrique': '', 'Valeur': ''},

                # Temps d'attente
                {'Métrique': '=== TEMPS D\'ATTENTE ===', 'Valeur': ''},
                {'Métrique': 'Temps d\'attente moyen', 'Valeur': f"{statistics['temps_attente_moyen']}s"},
                {'Métrique': 'Temps d\'attente minimum', 'Valeur': f"{statistics['temps_attente_min']}s"},
                {'Métrique': 'Temps d\'attente maximum', 'Valeur': f"{statistics['temps_attente_max']}s"},
                {'Métrique': '', 'Valeur': ''},

                # Renvois et transferts
                {'Métrique': '=== RENVOIS & TRANSFERTS ===', 'Valeur': ''},
                {'Métrique': 'Nombre de renvois', 'Valeur': statistics['nb_renvois_appels_recus']},
                {'Métrique': 'Durée des renvois', 'Valeur': ExcelExporter.format_duration(statistics['duree_renvois_appels_recus'])},
                {'Métrique': 'Nombre de transferts', 'Valeur': statistics['nb_transferts']},
                {'Métrique': 'Nombre de forwards', 'Valeur': statistics['nb_forwards']},
                {'Métrique': '', 'Valeur': ''},

                # Queues (files d'attente)
                {'Métrique': '=== QUEUES (FILES D\'ATTENTE) ===', 'Valeur': ''},
                {'Métrique': 'Appels passés par queue', 'Valeur': statistics['nb_appels_queue']},
                {'Métrique': 'Temps d\'attente moyen en queue', 'Valeur': f"{statistics['temps_attente_queue_moyen']}s"},
                {'Métrique': 'Temps d\'attente max en queue', 'Valeur': f"{statistics['temps_attente_queue_max']}s"},
                {'Métrique': '', 'Valeur': ''},

                # Messagerie vocale
                {'Métrique': '=== MESSAGERIE VOCALE ===', 'Valeur': ''},
                {'Métrique': 'Appels vers messagerie vocale', 'Valeur': statistics['nb_appels_voicemail']},
                {'Métrique': 'Taux de messagerie vocale', 'Valeur': f"{statistics['taux_voicemail']}%"},
                {'Métrique': '', 'Valeur': ''},

                # Autres métriques
                {'Métrique': '=== AUTRES MÉTRIQUES ===', 'Valeur': ''},
                {'Métrique': 'Appels Click-to-Call', 'Valeur': statistics['nb_click_to_call']},
                {'Métrique': 'Nombre moyen de participants', 'Valeur': statistics['nb_participants_moyen']},
                {'Métrique': 'Nombre moyen d\'événements CDR', 'Valeur': statistics['nb_events_moyen']},
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

        # Génération des nouvelles statistiques
        queue_stats = StatisticsGenerator.calculate_queue_statistics(df)
        sla_stats = StatisticsGenerator.calculate_sla_statistics(df)
        wait_dist = StatisticsGenerator.calculate_wait_time_distribution(df)

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

                # Feuille des statistiques SLA
                if not sla_stats.empty:
                    sla_display = sla_stats.copy()
                    sla_display.columns = ['Seuil (s)', 'Conformes', 'Total', 'Pourcentage (%)']
                    sla_display.to_excel(writer, sheet_name='SLA', index=False)

                # Feuille des statistiques de queues
                if not queue_stats.empty:
                    queue_stats['duree_totale_format'] = queue_stats['duree_totale'].apply(ExcelExporter.format_duration)
                    queue_stats['duree_moyenne_format'] = queue_stats['duree_moyenne'].apply(ExcelExporter.format_duration)
                    queue_stats['temps_attente_moyen_format'] = queue_stats['temps_attente_moyen'].apply(lambda x: f"{x}s")
                    queue_stats['temps_attente_max_format'] = queue_stats['temps_attente_max'].apply(lambda x: f"{x}s")
                    queue_display = queue_stats[['queue_name', 'nb_appels', 'nb_repondus', 'taux_reponse',
                                                  'duree_totale_format', 'duree_moyenne_format',
                                                  'temps_attente_moyen_format', 'temps_attente_max_format']]
                    queue_display.columns = ['Queue', 'Nb Appels', 'Nb Répondus', 'Taux Réponse (%)',
                                              'Durée Totale', 'Durée Moyenne', 'Attente Moy.', 'Attente Max']
                    queue_display.to_excel(writer, sheet_name='Queues', index=False)

                # Feuille de distribution des temps d'attente
                if not wait_dist.empty:
                    wait_display = wait_dist.copy()
                    wait_display.columns = ['Tranche de Temps', 'Nombre d\'Appels', 'Pourcentage (%)']
                    wait_display.to_excel(writer, sheet_name='Distribution Attente', index=False)

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