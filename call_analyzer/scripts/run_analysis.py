from call_analyzer.config.settings import config
from call_analyzer.services.app import CDRAnalyzerApp

def main():
    """Fonction principale pour l'exécution du script."""
    # Dates d'analyse
    date_debut = '2025-05-01 00:00:00'
    date_fin = '2025-05-31 23:59:59'
    # Répertoire de sortie
    output_dir = './output'
    # Création de l'application d'analyse
    analyzer_app = CDRAnalyzerApp(config)

    analyzer_app.run_analysis(date_debut, date_fin, output_dir)

if __name__ == '__main__':
    main()