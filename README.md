<div align="center">

# CallAnalyzer

**Transformez vos logs téléphoniques en intelligence opérationnelle.**

Analyse complète des CDR FreePBX/Asterisk — reconstitution des parcours d'appel, métriques SLA, export Excel multi-feuilles, détection automatique des transferts, renvois et Click-to-Call.

[![Python](https://img.shields.io/badge/Python-3.9%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![pandas](https://img.shields.io/badge/pandas-2.2%2B-150458?style=flat-square&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0%2B-red?style=flat-square)](https://www.sqlalchemy.org/)
[![FreePBX](https://img.shields.io/badge/FreePBX-14%2B-orange?style=flat-square)](https://www.freepbx.org/)
[![Asterisk](https://img.shields.io/badge/Asterisk-16%2B-green?style=flat-square)](https://www.asterisk.org/)
[![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square)](LICENSE)

</div>

---

## Pourquoi CallAnalyzer ?

La base de données CDR d'Asterisk est une mine d'or — mais brute, elle n'est qu'une pile d'événements individuels sans cohérence. Un appel transféré génère 3 lignes. Un groupe d'appel en génère 6. Un Click-to-Call avec renvoi, une dizaine.

CallAnalyzer **reconstruit la réalité** : il groupe, corrèle et interprète ces événements pour en extraire des appels lisibles, des parcours intelligibles et des métriques actionnables — le tout exporté en Excel en quelques secondes.

```
CDR brut (12 événements)          →     Appel analysé
────────────────────────────────────────────────────────────────────────────
linkedid=1234, src=0033...              Source : 0033 3 83 36 95 55
linkedid=1234, dst=200                  Chemin : Externe → Ring group Accueil (200)
linkedid=1234, dst=101                             → Extension Alice (101) (NO ANSWER)
linkedid=1234, dst=102                             → Extension Bob (102) (ANSWERED)
...                                     Durée  : 2 min 14 s  |  SLA : ✓ < 20 s
```

---

## Fonctionnalités

### Reconstruction du parcours d'appel

CallAnalyzer reconstitue le chemin complet de chaque appel, quel que soit son niveau de complexité :

| Scénario | Parcours généré |
|---|---|
| Appel direct | `Externe 0033... → Extension Alice (101)` |
| Groupe d'appel | `Externe → Ring group Accueil (200) → Alice (101), Bob (102) (ANSWERED)` |
| Transfert | `Externe → Alice (101) → Bob (102) (ANSWERED)` |
| Renvoi | `Externe → Alice (101) (NO ANSWER) → Mobile Alice (ANSWERED)` |
| Click-to-Call | `Alice (101) (ANSWERED) → Externe 0033...` |

### Métriques calculées

| Catégorie | Métriques |
|---|---|
| **Volumétrie** | Total, entrants, sortants, internes, manqués |
| **Taux** | Taux de réponse global, externe, messagerie vocale |
| **Durées** | Totale, moyenne, min/max — global et par type |
| **SLA** | % répondus en < 10 s, 20 s, 30 s, 60 s (configurables) |
| **Files d'attente** | Temps d'attente moyen/max, taux par queue |
| **Transferts & renvois** | Comptage, durée, destination finale |
| **Click-to-Call** | Détection automatique, initiateur et destination |

### Export Excel — 9 feuilles

```
stats_appels_20250101-20250131.xlsx
├── Stats Globales       → Vue d'ensemble avec toutes les métriques
├── Stats par Heure      → Répartition horaire (pic d'activité)
├── Stats par Jour       → Évolution sur la période
├── Top Destinations     → Numéros les plus appelés
├── Top Sources          → Appelants les plus fréquents
├── SLA                  → Conformité aux seuils de temps de réponse
├── Queues               → Performance par file d'attente
├── Distribution Attente → Histogramme des temps d'attente
└── Appels Détaillés     → Export exhaustif ligne par ligne
```

---

## Architecture

```
CallAnalyzer/
├── call_analyzer/
│   ├── config/
│   │   └── settings.py          # Configuration centrale
│   ├── models/
│   │   ├── call.py              # Appel consolidé (résultat de l'analyse)
│   │   └── call_event.py        # Événement CDR brut
│   ├── services/
│   │   ├── app.py               # Orchestration — point d'entrée principal
│   │   ├── call_analyzer.py     # Moteur d'analyse et reconstruction de chemin
│   │   ├── statistics.py        # Génération des métriques
│   │   └── billing_classifier.py
│   ├── infrastructure/
│   │   ├── db_connector.py      # Connexion MySQL via SQLAlchemy
│   │   ├── gql_connector.py     # API GraphQL FreePBX (extensions, ring groups)
│   │   ├── query_builder.py     # Construction des requêtes SQL/GQL
│   │   └── excel_reporter.py    # Export openpyxl multi-feuilles
│   └── scripts/
│       └── run_analysis.py      # Script CLI d'exécution
├── requirements.txt
└── pyproject.toml
```

**Flux de traitement :**

```
MySQL (asteriskcdrdb)          GraphQL API (FreePBX)
        │                               │
        ▼                               ▼
  QueryBuilder              Extensions + Ring Groups
        │                               │
        └───────────────┬───────────────┘
                        ▼
                  CallAnalyzer
            (groupement par linkedid,
             reconstruction de chemin,
             détection transferts/renvois/CTC)
                        │
                        ▼
              StatisticsGenerator
            (métriques vectorisées pandas)
                        │
                        ▼
               ExcelExporter (openpyxl)
                        │
                 appels_*.xlsx
                stats_appels_*.xlsx
```

---

## Installation

**Prérequis :** Python 3.9+, accès MySQL à la base `asteriskcdrdb`, FreePBX 14+ avec API GraphQL activée.

```bash
# Cloner le dépôt
git clone https://github.com/votre-org/call-analyzer.git
cd call-analyzer

# Créer un environnement virtuel
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Linux/macOS

# Installer les dépendances
pip install -r requirements.txt
```

---

## Configuration

Éditez `call_analyzer/config/settings.py` :

```python
config = {
    # Connexion à la base de données CDR Asterisk
    'db_host':     '192.168.1.10',
    'db_port':     3306,
    'db_user':     'cdr_reader',
    'db_password': 'votre_mot_de_passe',
    'db_name':     'asteriskcdrdb',
    'db_charset':  'utf8',

    # Numéro(s) de référence — si renseigné, l'analyse se centre sur ce(s) numéro(s)
    # Utile pour analyser une ligne DID spécifique plutôt que tout le système
    'reference_numbers': ['0383369555'],

    # API GraphQL FreePBX — pour résoudre les noms d'extensions et de ring groups
    'client_id':     'votre_client_id_oauth2',
    'client_secret': 'votre_client_secret',
    'scope':         'gql',
}
```

> **`reference_numbers`** : si vide (`[]`), CallAnalyzer analyse l'intégralité du trafic du système. Si renseigné, il filtre les appels impliquant ce(s) numéro(s) et adapte le calcul de durée facturable en conséquence.

---

## Utilisation

### Script CLI

```bash
python -m call_analyzer.scripts.run_analysis
```

Le script est configuré directement dans `run_analysis.py` :

```python
date_debut = '2025-01-01 00:00:00'
date_fin   = '2025-01-31 23:59:59'
```

### Intégration Python

```python
from call_analyzer.services.app import CDRAnalyzerApp
from call_analyzer.config.settings import config

app = CDRAnalyzerApp(config)

# Analyse sans export — retourne les stats et le DataFrame
statistics, df = app.run_analysis(
    date_debut='2025-01-01',
    date_fin='2025-01-31',
    export=False,
)

print(f"Appels reçus    : {statistics['nb_appels_recus']}")
print(f"Taux de réponse : {statistics['taux_reponse_externe']:.1f}%")
print(f"Temps d'attente : {statistics['temps_attente_moyen']}s en moyenne")
print(f"SLA < 20s       : {statistics['sla_20s_percent']:.1f}%")
```

```python
# Analyse avec export Excel
statistics, files = app.run_analysis(
    date_debut='2025-01-01',
    date_fin='2025-01-31',
    export=True,
    output_dir='./rapports/janvier',
)

# files['calls']  → chemin vers appels_20250101-20250131.xlsx
# files['stats']  → chemin vers stats_appels_20250101-20250131.xlsx
print(f"Rapport généré : {files['stats']}")
```

### Exemple de sortie statistiques

```python
{
    # Volumétrie
    'nb_appels_total':           1247,
    'nb_appels_recus':            834,
    'nb_appels_emis':             312,
    'nb_appels_internes':         101,
    'nb_appels_manques':           89,
    'nb_appels_externes_manques':  76,

    # Taux de réponse
    'taux_reponse_global':        92.8,   # %
    'taux_reponse_externe':       90.9,   # %

    # Durées (en secondes)
    'duree_appels_total':        84320,
    'duree_moyenne_appels':        211,
    'duree_moyenne_appels_externes': 248,

    # SLA
    'sla_20s_count':              701,
    'sla_20s_percent':            84.0,   # %
    'sla_30s_count':              752,
    'sla_30s_percent':            90.2,   # %

    # Temps d'attente
    'temps_attente_moyen':         14,    # secondes
    'temps_attente_min':            1,
    'temps_attente_max':          187,

    # Files d'attente
    'nb_appels_queue':            423,
    'temps_attente_queue_moyen':   18,
    'temps_attente_queue_max':    210,

    # Transferts & renvois
    'nb_transferts':               34,
    'nb_forwards':                 58,

    # Messagerie vocale
    'nb_appels_voicemail':         12,
    'taux_voicemail':              1.4,   # %

    # Click-to-Call
    'nb_click_to_call':            27,
}
```

---

## Détection automatique des scénarios

CallAnalyzer identifie et gère automatiquement les cas suivants via l'analyse du `dcontext`, des canaux et du `cnam` :

| Scénario | Signal détecté |
|---|---|
| Appel entrant | Trunk dans `channel` |
| Appel sortant | Trunk dans `dstchannel` |
| Appel interne | Source et destination dans les extensions connues |
| Groupe d'appel | `context = ext-group` + canal `Local/` |
| Transfert | Canal `Local/` dans `from-internal` / `ext-local` |
| Renvoi | Canal `Local/0` dans `from-internal` |
| File d'attente | `lastapp = Queue` |
| Messagerie vocale | `lastapp = VoiceMail` |
| Click-to-Call | `cnam` contient `"Répondre pour appeler le"` |

---

## Stack technique

| Composant | Rôle |
|---|---|
| [pandas 2.2+](https://pandas.pydata.org/) | Manipulation vectorisée des données CDR |
| [SQLAlchemy 2.0+](https://www.sqlalchemy.org/) | Connexion MySQL avec connection pooling |
| [PyMySQL 1.1+](https://pymysql.readthedocs.io/) | Driver MySQL pur Python |
| [gql 3.5+](https://gql.readthedocs.io/) | Client GraphQL pour l'API FreePBX |
| [aiohttp 3.11+](https://docs.aiohttp.org/) | Transport async pour les requêtes GQL |
| [openpyxl 3.1+](https://openpyxl.readthedocs.io/) | Génération des fichiers Excel multi-feuilles |

---

## Compatibilité

- **FreePBX** 14, 15, 16, 17+
- **Asterisk** 16, 18, 20, 21+
- **Python** 3.9, 3.10, 3.11, 3.12
- **Protocoles** : PJSIP, SIP, IAX2, Local channels, Trunks

---

## Licence

MIT — voir [LICENSE](LICENSE)

---

<div align="center">

Développé par **Romain Berthet** · [Réseau Concept](https://www.reseau-concept.com)

</div>
