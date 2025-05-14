import logging
import re
from datetime import timedelta
from typing import List, Optional, Set, Tuple

import pandas as pd

from call_analyzer.models.call import Call
from call_analyzer.models.call_event import CallEvent

logger = logging.getLogger(__name__)


class CallAnalyzer:
    """Analyse les données d'appels pour extraire des informations pertinentes."""

    def __init__(self, internal_numbers: Set[str], reference_numbers: Optional[List[str]] = None):
        """Initialise l'analyseur d'appels.

        Args:
            internal_numbers: Ensemble des numéros internes du système
            reference_numbers: Liste des numéros de référence pour l'analyse (optionnel)
        """
        self.internal_numbers = internal_numbers
        self.reference_numbers = reference_numbers

    def _is_internal_number(self, number: str) -> bool:
        """Vérifie si un numéro est un numéro interne.

        Args:
            number: Numéro à vérifier

        Returns:
            True si le numéro est interne, False sinon
        """
        return str(number) in self.internal_numbers

    def _extract_number_from_channel(self, channel: str) -> Optional[str]:
        """Extrait un numéro de téléphone à partir d'une chaîne de canal.

        Args:
            channel: Chaîne de canal Asterisk

        Returns:
            Numéro extrait ou None si aucun numéro n'est trouvé
        """
        if not channel:
            return None

        patterns = [
            # Patterns pour différents types de canaux
            (r'PJSIP/(\d+)-', 1),
            (r'Local/([^@]+)@', 1),
            (r'SIP/(\d+)-', 1),
            (r'IAX2/(\d+)-', 1),
        ]

        for pattern, group in patterns:
            match = re.search(pattern, channel)
            if match:
                # Suppression du préfixe 9 si nécessaire (reprend un RingGroup)
                return match.group(group).lstrip('9')

        return None

    def _check_if_forward(self, events: List[CallEvent]) -> bool:
        """Vérifie si l'appel contient un renvoi.

        Args:
            events: Liste des événements d'appel

        Returns:
            True si l'appel contient un renvoi, False sinon
        """
        return any('Local/0' in e.channel and e.context == 'from-internal' for e in events)

    def _get_forward_call_events(self, events: List[CallEvent]) -> List[CallEvent]:
        """Récupère les événements de renvoi d'appel.

        Args:
            events: Liste des événements d'appel

        Returns:
            Liste des événements de renvoi
        """
        if self._check_if_forward(events):
            return [e for e in events if 'Local/0' in e.channel and e.context == 'from-internal']
        return []

    def _check_if_group_call(self, events: List[CallEvent]) -> bool:
        """Vérifie si l'appel est un appel de groupe.

        Args:
            events: Liste des événements d'appel

        Returns:
            True si l'appel est un appel de groupe, False sinon
        """
        return any(e.context == 'ext-group' and 'Local/' in e.dstchannel for e in events)

    def _get_call_direction(self, events: List[CallEvent], is_internal: bool) -> str:
        """Détermine la direction de l'appel.

        Args:
            events: Liste des événements d'appel
            is_internal: Indique si l'appel est interne

        Returns:
            Direction de l'appel ('entrant', 'sortant', ou 'interne')
        """
        if is_internal:
            if self.reference_numbers:
                if events[0].src in self.reference_numbers:
                    return 'sortant'
                if events[0].dst in self.reference_numbers:
                    return 'entrant'
            return 'interne'

        if any('trunk' in e.channel for e in events):
            return 'entrant'
        if any('trunk' in e.dstchannel for e in events):
            return 'sortant'

        return 'interne'

    def _get_call_status(self, events: List[CallEvent]) -> str:
        """Détermine le statut de l'appel.

        Args:
            events: Liste des événements d'appel

        Returns:
            Statut de l'appel ('ANSWERED', 'NO ANSWER', 'BUSY', 'FAILED', etc.)
        """
        # Cas spécial pour les appels de référence
        if self.reference_numbers and len(self.reference_numbers) == 1:
            status = 'NO ANSWER'
            if self._check_if_forward(events):
                return 'ANSWERED' if any('ANSWERED' in e.disposition for e in events) else 'NO ANSWER'

            for event in events:
                src = self._extract_number_from_channel(event.channel) or event.src
                dst = self._extract_number_from_channel(event.dstchannel) or event.dst
                if src in self.reference_numbers or dst in self.reference_numbers:
                    status = event.disposition
                    if status == 'ANSWERED':
                        return status
            return status

        # Cas général
        # Priorité aux statuts dans l'ordre: ANSWERED > BUSY > CONGESTION > NO ANSWER > FAILED
        if any(e.disposition == 'ANSWERED' for e in events):
            return 'ANSWERED'
        if any(e.disposition == 'BUSY' for e in events):
            return 'BUSY'
        if any(e.disposition == 'CONGESTION' for e in events):
            return 'CONGESTION'
        if any(e.disposition == 'NO ANSWER' for e in events):
            return 'NO ANSWER'

        return 'FAILED'

    def _get_call_billsec(self, events: List[CallEvent]) -> int:
        """Calcule la durée facturable de l'appel.

        Args:
            events: Liste des événements d'appel

        Returns:
            Durée facturable en secondes
        """
        if not events:
            return 0

        # Cas spécial pour les appels de référence
        if self.reference_numbers and len(self.reference_numbers) == 1:
            billsec = 0

            # Cas des renvois et appels de groupe
            if self._check_if_forward(events) and self._check_if_group_call(events):
                return sum(e.billsec for e in events)

            # Calcul de la durée pour les appels impliquant les numéros de référence
            for event in events:
                src = self._extract_number_from_channel(event.channel) or event.src
                dst = (self._extract_number_from_channel(event.dstchannel)
                       if not (event.dstchannel and 'Local/0' in event.dstchannel)
                       else event.dst) or event.dst

                if src in self.reference_numbers or dst in self.reference_numbers:
                    billsec += event.billsec

            return billsec

        # Cas général
        if self._check_if_forward(events):  # Renvoi d'appel
            return sum(e.billsec for e in events if 'Local/0' in e.channel and e.context == 'from-internal')

        if self._check_if_group_call(events):  # Appel de groupe
            return sum(e.billsec for e in events if not (e.context == 'ext-group' and 'Local/' in e.dstchannel))

        # Cas standard: somme des durées de tous les événements
        return sum(e.billsec for e in events)

    def _identify_click_to_call(self, events: List[CallEvent]) -> Tuple[
        bool, Optional[str], Optional[str], int, List[str]]:
        """Identifie si l'appel est un Click-to-Call et extrait les informations pertinentes.

        Args:
            events: Liste des événements d'appel

        Returns:
            Tuple contenant:
            - Un booléen indiquant si c'est un Click-to-Call
            - Le numéro source
            - Le numéro de destination
            - La durée
            - La liste des numéros de renvoi
        """
        is_click_to_call = any(e.cnam and 'Répondre pour appeler le' in e.cnam for e in events)
        forward = []

        if is_click_to_call and events:
            first_event = events[0]
            first_event_src = self._extract_number_from_channel(first_event.channel) or first_event.src

            # Recherche de l'événement macro-dial
            event_macro_dial = next((e for e in events if 'macro-dial' in e.context), first_event)

            # Détermination de la source
            if first_event_src.startswith('0') or first_event_src.startswith('+'):
                src = event_macro_dial.src
            else:
                src = first_event_src

            dst = first_event.dst
            duration = event_macro_dial.billsec

            # Analyse des renvois
            forward_events = self._get_forward_call_events(events)
            for forward_event in forward_events:
                duration += forward_event.billsec
                forward.append(forward_event.dst)

            return True, src, dst, duration, forward

        return False, None, None, 0, forward

    def _identify_actions_by_context(self, events: List[CallEvent]) -> Tuple[
        Optional[str], Optional[str], Optional[str], Optional[str], str, List[dict]]:
        """
        Identifie et catégorise les actions d'appel en analysant le contexte des événements.

        Args:
            events: Liste d'événements d'appel à analyser

        Returns:
            Tuple contenant:
            - transfers_from: Numéro à partir duquel le transfert a été initié
            - transfers_to: Numéro vers lequel le transfert a été effectué
            - forwards_from: Numéro à partir duquel le renvoi a été initié
            - forwards_to: Numéro vers lequel le renvoi a été effectué
            - call_path: Représentation textuelle du chemin d'appel
            - call_path_details: Liste détaillée des étapes du chemin d'appel avec métadonnées
        """
        # Initialisation des variables
        transfers_from = transfers_to = forwards_from = forwards_to = None
        path = []
        call_path_details = []
        virtual_forward = ''
        seen_path_keys = set()
        group_members = {}
        internal_calls = {}
        external_calls = {}

        def base_number(number: str) -> str:
            """Supprime le suffixe (ANSWERED) pour comparer les racines."""
            return number.split(" ")[0] if number else ''

        def add_to_path(number: str, call_type: str, disposition: str = None):
            """Ajoute un numéro au chemin d'appel s'il n'est pas déjà présent."""
            if not number:
                return

            suffix = f" ({disposition})" if disposition == 'ANSWERED' else ''
            path_key = number + suffix

            last_in_path = path[-1] if path else ''
            if (base_number(last_in_path) != base_number(path_key)
                    and path_key not in seen_path_keys
                    and base_number(path_key) != virtual_forward):
                path.append(path_key)
                seen_path_keys.add(path_key)
                call_path_details.append({
                    'number': number,
                    'type': call_type,
                    'disposition': disposition,
                    'timestamp': event.timestamp if 'event' in locals() else None
                })

        # Première passe: collection des informations de groupe et d'appels entrants/sortants
        for event in events:
            is_local = event.dstchannel and 'Local/' in event.dstchannel
            src_number = self._extract_number_from_channel(event.channel) or event.src
            dst_number = (
                event.dst if is_local else self._extract_number_from_channel(event.dstchannel) or event.dst
            )

            # Traitement des appels de groupe
            if event.context == 'ext-group':
                group_id = event.dst
                if group_id not in group_members:
                    group_members[group_id] = []
                if dst_number and dst_number not in group_members[group_id]:
                    group_members[group_id].append(dst_number)

            # Collecter les appels internes
            if event.context == 'from-internal':
                call_id = f"{src_number}_{dst_number}_{event.timestamp}"
                internal_calls[call_id] = {
                    'src': src_number,
                    'dst': dst_number,
                    'time': event.timestamp,
                    'disposition': event.disposition
                }


        # Deuxième passe: analyse principale
        for event in events:
            is_local = event.dstchannel and 'Local/' in event.dstchannel
            src_number = self._extract_number_from_channel(event.channel) or event.src
            dst_number = (
                event.dst if is_local else self._extract_number_from_channel(event.dstchannel) or event.dst
            )

            # Initialisation du chemin
            if src_number and dst_number and not path:
                add_to_path(src_number, 'source')
                # Ajouter un destinataire initial si différent
                if event.dst != dst_number:
                    add_to_path(event.dst, 'initial_destination')

            # Mise à jour du chemin si nouvelle destination
            if src_number and dst_number:
                call_type = 'group_call' if event.context == 'ext-group' else ('internal' if event.context == 'from-internal' else ('external' if event.context in ['from-trunk', 'outbound-allroutes'] else 'standard'))
                add_to_path(dst_number, call_type, event.disposition)

            # Traitement des suivis d'appels
            if event.context == 'followme-check' and event.dstchannel and 'Local/' in event.dstchannel and event.disposition == 'ANSWERED':
                local_key = event.dstchannel.split(';')[0]

                # Recherche de l'événement interne correspondant
                match = next(
                    (e for e in events if e.channel and e.channel.startswith(local_key) and e.context == 'from-internal'),
                    None
                )

                if match:
                    forwards_to = self._extract_number_from_channel(match.dstchannel) or match.dst
                    add_to_path(forwards_to, 'forwarded', match.disposition)
                    virtual_forward = forwards_to
                    continue

            # Traitement des transferts et renvois internes
            if event.context == 'from-internal':
                if event.channel and 'Local/0' in event.channel:
                    if event.disposition == 'ANSWERED':
                        forwards_to = dst_number
                        add_to_path(dst_number, 'forward_answered', event.disposition)
                    virtual_forward = dst_number
                elif 'Local/' in event.channel and not is_local:
                    if not transfers_to and dst_number:
                        transfers_to = dst_number
                        add_to_path(dst_number, 'transfer_internal', event.disposition)

            # Traitement des transferts externes via ext-local
            elif event.context == 'ext-local' and event.dst != dst_number:
                if not transfers_to:
                    transfers_to = dst_number
                    add_to_path(dst_number, 'transfer_external', event.disposition)

            # Traitement des appels de groupe
            elif event.context == 'ext-group':
                group_id = event.dst
                if group_id in group_members and dst_number in group_members[group_id]:
                    if event.disposition == 'ANSWERED':
                        add_to_path(dst_number, 'group_answered', event.disposition)
                    else:
                        # Ne pas ajouter les membres du groupe non répondus au chemin principal
                        pass

        # Analyse finale pour détecter les cas spéciaux

        # Vérifier les appels internes manqués
        for call_id, call in internal_calls.items():
            if call['disposition'] == 'NO ANSWER' and call['dst'] not in seen_path_keys:
                # Ajouter les appels internes manqués au chemin
                if base_number(path[-1] if path else '') != base_number(call['dst']):
                    add_to_path(call['dst'], 'missed_internal', 'NO ANSWER')

        # Vérifier les appels externes manqués
        for call_id, call in external_calls.items():
            if call['disposition'] == 'NO ANSWER' and call['dst'] not in seen_path_keys:
                # Ajouter les appels externes manqués au chemin
                if base_number(path[-1] if path else '') != base_number(call['dst']):
                    add_to_path(call['dst'], 'missed_external', 'NO ANSWER')

        # Construire la représentation en chaîne du chemin
        call_path = " --> ".join(path)

        return transfers_from, transfers_to, forwards_from, forwards_to, call_path, call_path_details

    def analyze_call(self, events: List[CallEvent]) -> Optional[Call]:
        """Analyse un appel complet à partir de ses événements.

        Args:
            events: Liste des événements d'appel

        Returns:
            Objet Call représentant l'appel analysé ou None si l'appel n'est pas valide
        """
        if not events:
            return None

        # Tri des événements par séquence
        events = sorted(events, key=lambda e: e.sequence)

        # Vérification si l'appel est un Click-to-Call
        is_click_to_call, src_ctc, dst_ctc, duration_ctc, forward_numbers = self._identify_click_to_call(events)

        if is_click_to_call and src_ctc and dst_ctc:
            return Call(
                start_time=events[0].timestamp,
                uniqueid=events[0].linkedid,
                source=src_ctc,
                destination=dst_ctc,
                duration=duration_ctc,
                status=self._get_call_status(events),
                type=self._get_call_direction(events, self._is_internal_number(src_ctc) and self._is_internal_number(dst_ctc)),
                is_internal=self._is_internal_number(src_ctc) and self._is_internal_number(dst_ctc),
                transfers_from=None,
                transfers_to=None,
                forwards_from=None,
                forwards_to=None,
                is_click_to_call=True,
                final_path=" --> ".join([src_ctc] + forward_numbers + [dst_ctc]),
                original_caller_name=events[0].cnam,
                did=events[0].did,
                accountcode=events[0].accountcode,
                userfield=events[0].userfield
            )

        first_event = events[0]

        # Validation des données de base
        if not first_event.src or not first_event.dst:
            return None

        # Identification des actions et construction du chemin
        transfers_from, transfers_to, forwards_from, forwards_to, path, call_path_details = self._identify_actions_by_context(events)
        is_internal = self._is_internal_number(first_event.src) and self._is_internal_number(first_event.dst)

        return Call(
            start_time=first_event.timestamp,
            uniqueid=first_event.linkedid,
            source=first_event.src,
            destination=first_event.dst,
            duration=self._get_call_billsec(events),
            status=self._get_call_status(events),
            type=self._get_call_direction(events, is_internal),
            is_internal=is_internal,
            transfers_from=transfers_from,
            transfers_to=transfers_to,
            forwards_from=forwards_from,
            forwards_to=forwards_to,
            final_path=path,
            is_click_to_call=False,
            original_caller_name=first_event.cnam,
            did=first_event.did,
            accountcode=first_event.accountcode,
            userfield=first_event.userfield
        )

    def process_dataframe(self, df: pd.DataFrame) -> List[Call]:
        """Traite un DataFrame de données d'appels et retourne les appels analysés.

        Args:
            df: DataFrame contenant les données d'appels

        Returns:
            Liste des appels analysés
        """
        if df.empty:
            logger.warning("Le DataFrame est vide.")
            return []

        calls = []
        for linkedid, group in df.groupby('linkedid'):
            events = [
                CallEvent(
                    timestamp=row['calldate'],
                    uniqueid=row['uniqueid'],
                    linkedid=row['linkedid'],
                    src=row['src'],
                    dst=row['dst'],
                    channel=row['channel'],
                    dstchannel=row['dstchannel'],
                    disposition=row['disposition'],
                    billsec=row['billsec'],
                    sequence=row['sequence'],
                    context=row['context'],
                    lastapp=row['lastapp'],
                    cnum=row['cnum'],
                    cnam=row.get('cnam', None),
                    did=row.get('did', None),
                    accountcode=row.get('accountcode', None),
                    userfield=row.get('userfield', None),
                    amaflags=row.get('amaflags', None),
                    duration=row.get('duration', None),
                    clid=row.get('clid', None)
                )
                for _, row in group.iterrows()
            ]
            call = self.analyze_call(events)
            if call:
                calls.append(call)

        return calls

    def to_dataframe(self, calls: List[Call]) -> pd.DataFrame:
        """Convertit une liste d'appels en DataFrame.

        Args:
            calls: Liste des appels à convertir

        Returns:
            DataFrame contenant les données d'appels
        """
        if not calls:
            return pd.DataFrame()

        data = [{
            'call_date': call.start_time,
            'end_date': call.start_time + timedelta(seconds=call.duration),
            'uniqueid': call.uniqueid,
            'src': call.source,
            'dst': call.destination,
            'billsec': call.duration,
            'status': call.status,
            'answered': call.status == 'ANSWERED',
            'type_appel': call.type,
            'is_internal': call.is_internal,
            'transfert_depuis': call.transfers_from,
            'transfert_vers': call.transfers_to,
            'renvoi_depuis': call.forwards_from,
            'renvoi_vers': call.forwards_to,
            'path': call.final_path,
            'is_click_to_call': call.is_click_to_call,
            'original_caller_name': call.original_caller_name,
            'did': call.did,
            'accountcode': call.accountcode,
            'userfield': call.userfield
        } for call in calls]

        df = pd.DataFrame(data)

        # Conversion des colonnes de date en datetime
        for col in ['call_date', 'end_date']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

        return df