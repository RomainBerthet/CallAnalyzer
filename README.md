# CallAnalyzer - Module d'Analyse CDR FreePBX Optimisé

CallAnalyzer est un outil d'analyse et de reporting complet pour les données CDR (Call Detail Records) de FreePBX/Asterisk. Il fournit des statistiques détaillées, des tableaux de bord Excel, et des insights de performance pour monitorer l'activité téléphonique, identifier les tendances, et optimiser les opérations.

## 🚀 Caractéristiques Principales

### ✅ Extraction Complète des Données CDR
- **Tous les champs CDR disponibles** : calldate, start, answer, end, uniqueid, linkedid, sequence, src, dst, channel, dstchannel, disposition, duration, billsec, clid, cnam, did, accountcode, peeraccount, lastapp, lastdata, amaflags, userfield, etc.
- **Support multi-protocoles** : PJSIP, SIP, IAX2, Local channels, Trunks
- **Analyse de session complète** : Regroupement par linkedid pour reconstituer l'historique complet des appels

### 📊 Statistiques Avancées

#### Métriques de Base
- Volumétrie (total, reçus, émis, internes, externes)
- Appels manqués et taux de réponse
- Durées totales, moyennes, min/max
- Répartition par heure, jour, semaine

#### Métriques SLA (Service Level Agreement)
- **Temps d'attente** : moyen, minimum, maximum avant réponse
- **SLA configurable** : % d'appels répondus en moins de 10s, 20s, 30s, 60s
- **Distribution** : Répartition des temps d'attente par tranches

#### Analyse des Queues (Files d'Attente)
- Statistiques par queue
- Temps d'attente en queue
- Taux de réponse par queue
- Durées moyennes par queue

#### Transferts et Renvois
- Détection automatique des transferts
- Détection des renvois d'appels
- Tracking complet du chemin d'appel (A → B → C)
- Click-to-Call identification

#### Messagerie Vocale
- Nombre d'appels vers messagerie
- Taux de messagerie vocale
- Identification automatique

### 📈 Exports Excel Enrichis

Le module génère des fichiers Excel multi-feuilles avec:

1. **Stats Globales** : Vue d'ensemble complète avec toutes les métriques
2. **Stats par Heure** : Répartition horaire des appels
3. **Stats par Jour** : Évolution journalière
4. **Top Destinations** : Numéros les plus appelés
5. **Top Sources** : Appelants les plus fréquents
6. **SLA** : Conformité aux seuils de temps de réponse
7. **Queues** : Performance des files d'attente
8. **Distribution Attente** : Répartition des temps d'attente
9. **Appels Détaillés** : Liste complète avec tous les champs

### 🔍 Analyse de Chemin d'Appel

Reconstitution intelligente du parcours complet:
- Appel simple : `Source → Destination`
- Avec transfert : `Source → Extension1 → Extension2 (ANSWERED)`
- Avec renvoi : `Source → Extension1 (NO ANSWER) → Extension2 (ANSWERED)`
- Groupe d'appel : `Source → RingGroup → Extension1, Extension2, Extension3 (ANSWERED)`
- Combiné : `Source → RingGroup → Extension1 → Extension2`

### 🎯 Cas d'Usage Détectés

Le module identifie automatiquement:
- **Appels standards** (entrants/sortants/internes)
- **Click-to-Call** (via analyse du CNAM)
- **Transferts** (contexte ext-local, from-internal)
- **Renvois** (Local/0 channels)
- **Appels de groupe** (contexte ext-group)
- **Files d'attente** (lastapp = Queue)
- **Messagerie vocale** (lastapp = VoiceMail)

## 🛠️ Architecture Technique

### Structure du Projet
```
CallAnalyzer/
├── call_analyzer/
│   ├── config/                 # Configuration
│   │   └── settings.py
│   ├── models/                 # Modèles de données
│   │   ├── call.py            # Appel consolidé
│   │   └── call_event.py      # Événement CDR individuel
│   ├── services/               # Logique métier
│   │   ├── app.py             # Orchestration
│   │   ├── call_analyzer.py   # Analyse des appels
│   │   └── statistics.py      # Génération statistiques
│   ├── infrastructure/         # Accès données & exports
│   │   ├── db_connector.py    # Connexion MySQL
│   │   ├── gql_connector.py   # GraphQL API
│   │   ├── query_builder.py   # Construction requêtes SQL
│   │   └── excel_reporter.py  # Export Excel
│   └── scripts/
│       └── run_analysis.py
├── requirements.txt
├── pyproject.toml
└── README.md
```

### Technologies
- **Python 3.9+**
- **pandas 2.2+** : Manipulation de données vectorisée
- **SQLAlchemy 2.0+** : ORM base de données
- **PyMySQL 1.1+** : Driver MySQL
- **GraphQL (gql 3.5+)** : API extensions/ring groups
- **openpyxl 3.1+** : Export Excel
- **aiohttp 3.11+** : Client async GraphQL

## 📋 Champs CDR Analysés

### Horodatages
- `calldate` : Date/heure de l'appel
- `start` : Début de l'appel
- `answer` : Heure de réponse
- `end` : Fin de l'appel

### Identifiants
- `uniqueid` : ID unique de l'événement
- `linkedid` : ID de session (regroupe les événements liés)
- `sequence` : Numéro de séquence

### Numéros et Canaux
- `src` : Numéro source (appelant)
- `dst` : Numéro destination (appelé)
- `cnum` : Numéro appelant
- `channel` : Canal source
- `dstchannel` : Canal destination

### Identification
- `clid` : Calling Line ID
- `cnam` : Caller ID Name

### Contexte
- `dcontext` : Contexte du dialplan
- `lastapp` : Dernière application (Dial, Queue, VoiceMail...)
- `lastdata` : Arguments de lastapp

### État
- `disposition` : Résultat (ANSWERED, NO ANSWER, BUSY, CONGESTION, FAILED)

### Durées
- `duration` : Durée totale (secondes)
- `billsec` : Durée facturable (secondes)

### Routage et Facturation
- `did` : Direct Inward Dial
- `accountcode` : Code compte
- `peeraccount` : Code compte du pair

### Données Personnalisées
- `amaflags` : AMA flags
- `userfield` : Champ utilisateur

## 📊 Métriques Calculées

### Statistiques Générées

```python
{
    # Volumétrie
    'nb_appels_total': int,
    'nb_appels_recus': int,
    'nb_appels_emis': int,
    'nb_appels_internes': int,

    # Appels manqués
    'nb_appels_manques': int,
    'nb_appels_externes_manques': int,
    'nb_appels_internes_manques': int,

    # Taux de réponse
    'taux_reponse_global': float,  # %
    'taux_reponse_externe': float, # %

    # Durées
    'duree_appels_total': int,  # secondes
    'duree_moyenne_appels': int,
    'duree_moyenne_appels_internes': int,
    'duree_moyenne_appels_externes': int,

    # SLA (Service Level Agreement)
    'sla_20s_count': int,
    'sla_20s_percent': float,  # %
    'sla_30s_count': int,
    'sla_30s_percent': float,  # %

    # Temps d'attente
    'temps_attente_moyen': int,  # secondes
    'temps_attente_min': int,
    'temps_attente_max': int,

    # Transferts et renvois
    'nb_transferts': int,
    'nb_forwards': int,
    'nb_renvois_appels_recus': int,
    'duree_renvois_appels_recus': int,

    # Queues
    'nb_appels_queue': int,
    'temps_attente_queue_moyen': int,
    'temps_attente_queue_max': int,

    # Messagerie vocale
    'nb_appels_voicemail': int,
    'taux_voicemail': float,  # %

    # Click-to-Call
    'nb_click_to_call': int,

    # Métriques avancées
    'nb_participants_moyen': float,
    'nb_events_moyen': float,
}
```

## 🚀 Utilisation

### Installation

```bash
pip install -r requirements.txt
```

### Configuration

Éditer `call_analyzer/config/settings.py`:

```python
config = {
    # Base de données MySQL/Asterisk
    'db_user': 'rc-back',
    'db_password': 'votre_mot_de_passe',
    'db_name': 'asteriskcdrdb',
    'db_host': '172.27.0.46',
    'db_port': 3306,
    'db_charset': 'utf8',

    # Numéros de référence (optionnel)
    'reference_numbers': ['0383369555'],

    # GraphQL OAuth2
    'client_id': 'votre_client_id',
    'client_secret': 'votre_client_secret',
    'scope': 'gql'
}
```

### Exemple d'Utilisation

```python
from call_analyzer.services.app import CDRAnalyzerApp
from call_analyzer.config.settings import config

# Initialiser l'application
app = CDRAnalyzerApp(config)

# Lancer l'analyse avec export Excel
result = app.run_analysis(
    date_debut='2025-01-01',
    date_fin='2025-01-31',
    export=True,
    output_dir='./rapports'
)

# Résultat contient les statistiques et chemins des fichiers
print(result)
```

## 📈 Optimisations Implémentées

### Performance
- **Calculs vectorisés** avec pandas pour traitement rapide de grands volumes
- **CTE (Common Table Expression)** pour optimisation SQL
- **Connection pooling** SQLAlchemy avec `pool_pre_ping=True`
- **Groupement efficace** par linkedid pour reconstitution des sessions

### Fiabilité
- **Gestion d'erreurs** complète avec logging
- **Validation** des données à chaque étape
- **Formats de date multiples** supportés
- **Gestion des champs optionnels** (None-safe)

### Maintenabilité
- **Architecture en couches** claire (models, services, infrastructure)
- **Type hints** complets pour auto-documentation
- **Docstrings** détaillées sur toutes les méthodes
- **Design patterns** : Builder, Connector, Analyzer, Generator, Exporter

## 🎯 Améliorations Apportées

### v2.0 - Optimisation Complète

✅ **Extraction exhaustive des données**
- Ajout de tous les champs CDR manquants (start, answer, end, peeraccount, lastdata)
- Support complet des horodatages pour calculs précis

✅ **Nouvelles métriques**
- SLA (Service Level Agreement) avec seuils configurables
- Temps d'attente (moyen, min, max)
- Statistiques par queue
- Détection automatique messagerie vocale
- Tracking avancé des transferts/renvois

✅ **Exports enrichis**
- 9 feuilles Excel au lieu de 5
- Nouvelles vues : SLA, Queues, Distribution temps d'attente
- Formatage amélioré avec sections dans Stats Globales

✅ **Analyse améliorée**
- Calcul automatique du temps d'attente (answer - start)
- Détection des appels en queue avec temps d'attente
- Comptage des participants uniques
- Métriques SLA par appel

✅ **Code quality**
- Documentation exhaustive
- Type hints complets
- Méthodes utilitaires dans les modèles
- Architecture optimisée

## 📞 Support

Pour toute question ou demande d'amélioration, veuillez contacter l'équipe de développement.

---

**Version**: 2.0
**Dernière mise à jour**: 2025-11-05
**Compatibilité**: FreePBX 14+, Asterisk 16+
