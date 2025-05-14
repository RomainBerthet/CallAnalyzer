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
        return number in self.internal_numbers

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
        Optional[str], Optional[str], Optional[str], Optional[str], str]:
        """Identifie les actions d'appel (transferts, renvois) et reconstruit le chemin de l'appel.

        Args:
            events: Liste des événements d'appel

        Returns:
            Tuple contenant:
            - Numéro depuis lequel un transfert a été effectué
            - Numéro vers lequel un transfert a été effectué
            - Numéro depuis lequel un renvoi a été effectué
            - Numéro vers lequel un renvoi a été effectué
            - Chemin complet de l'appel
        """
        transfers_from, transfers_to = None, None
        forwards_from, forwards_to = None, None
        path = []
        virtual_forward = ''

        for event in events:
            src_number = self._extract_number_from_channel(event.channel) or event.src
            dst_number = (self._extract_number_from_channel(event.dstchannel)
                          if not (event.dstchannel and 'Local/0' in event.dstchannel)
                          else event.dst) or event.dst

            # Mise à jour du chemin
            if src_number and dst_number:
                if not path:
                    path.append(src_number)
                    if event.dst != dst_number:
                        path.append(event.dst)

                # Éviter les répétitions dans le chemin
                if path[-1] != dst_number and (path[-1] != virtual_forward and path[-1] != virtual_forward + ' (ANSWERED)'):
                    status_suffix = f" ({event.disposition})" if event.disposition == 'ANSWERED' and not 'Local/' in event.dstchannel else ''
                    path.append(dst_number + status_suffix)

            # Analyse par contexte
            if event.context == 'from-internal' and event.channel and 'Local/0' in event.channel:
                if event.disposition == 'ANSWERED':
                    forwards_to = dst_number
                if not forwards_from:
                    forwards_from = src_number
                virtual_forward = dst_number

            if event.dst != dst_number and event.context == 'ext-local':
                if not transfers_to:
                    transfers_to = dst_number
                if not transfers_from:
                    transfers_from = src_number

        return transfers_from, transfers_to, forwards_from, forwards_to, " --> ".join(path)

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
                type=self._get_call_direction(events,
                                              self._is_internal_number(src_ctc) and self._is_internal_number(dst_ctc)),
                is_internal=self._is_internal_number(src_ctc) and self._is_internal_number(dst_ctc),
                transfers_from=None,
                transfers_to=None,
                forwards_from=forward_numbers[-1] if forward_numbers else None,
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
        transfers_from, transfers_to, forwards_from, forwards_to, path = self._identify_actions_by_context(events)
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