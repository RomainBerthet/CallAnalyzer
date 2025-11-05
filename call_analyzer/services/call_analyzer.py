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

        if any('trunk' in e.channel.lower() for e in events):
            return 'entrant'
        if any('trunk' in e.dstchannel.lower() for e in events):
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
            # Calcul des temps pour Click-to-Call
            # Note: sans start/answer/end dans la DB, on utilise calldate et duration
            ctc_answer_time = None
            ctc_end_time = None
            ctc_wait_time = None

            # Trouver le premier événement répondu pour estimer l'heure de réponse
            for event in events:
                if event.disposition == 'ANSWERED' and event.billsec > 0:
                    # Estimer answer_time = timestamp (approximation)
                    ctc_answer_time = event.timestamp
                    # Estimer wait_time = duration - billsec
                    if event.duration and event.billsec:
                        ctc_wait_time = event.duration - event.billsec
                    break

            if events:
                last_event = max(events, key=lambda e: e.timestamp)
                # Calculer end_time à partir du timestamp + duration
                if last_event.duration:
                    ctc_end_time = last_event.timestamp + timedelta(seconds=last_event.duration)
                else:
                    ctc_end_time = last_event.timestamp

            return Call(
                # Identification
                uniqueid=events[0].linkedid,
                start_time=events[0].timestamp,

                # Participants
                source=src_ctc,
                destination=dst_ctc,
                original_caller_name=events[0].cnam,

                # État et classification
                status=self._get_call_status(events),
                type=self._get_call_direction(events, self._is_internal_number(src_ctc) and self._is_internal_number(dst_ctc)),
                is_internal=self._is_internal_number(src_ctc) and self._is_internal_number(dst_ctc),
                is_click_to_call=True,

                # Durées et horodatages
                duration=duration_ctc,
                answer_time=ctc_answer_time,
                end_time=ctc_end_time,
                wait_time=ctc_wait_time,
                ring_time=ctc_wait_time,

                # Analyse du chemin
                final_path=" --> ".join([src_ctc] + forward_numbers + [dst_ctc]),
                call_path_details=None,

                # Transferts et renvois
                transfers_from=None,
                transfers_to=None,
                forwards_from=None,
                forwards_to=None,

                # Applications spéciales
                went_to_voicemail=False,
                queue_name=None,
                queue_wait_time=None,

                # Routage et facturation
                did=events[0].did,
                accountcode=events[0].accountcode,
                peeraccount=events[0].peeraccount,
                userfield=events[0].userfield,

                # Métriques
                total_participants=len(set([src_ctc, dst_ctc] + forward_numbers)),
                event_count=len(events)
            )

        first_event = events[0]

        # Validation des données de base
        if not first_event.src or not first_event.dst:
            return None

        # Identification des actions et construction du chemin
        transfers_from, transfers_to, forwards_from, forwards_to, path, call_path_details = self._identify_actions_by_context(events)
        is_internal = self._is_internal_number(first_event.src) and self._is_internal_number(first_event.dst)

        # Calcul des temps d'attente et de réponse
        # Note: sans start/answer/end dans la DB, on utilise calldate et duration
        answer_time = None
        end_time = None
        wait_time = None
        ring_time = None

        # Trouver le premier événement répondu pour estimer l'heure de réponse
        for event in events:
            if event.disposition == 'ANSWERED' and event.billsec > 0:
                # Estimer answer_time = timestamp (approximation)
                answer_time = event.timestamp
                # Estimer wait_time = duration - billsec (temps de sonnerie)
                if event.duration and event.billsec:
                    wait_time = event.duration - event.billsec
                    ring_time = wait_time  # Temps de sonnerie = temps d'attente
                break

        # Déterminer l'heure de fin
        if events:
            last_event = max(events, key=lambda e: e.timestamp)
            # Calculer end_time à partir du timestamp + duration
            if last_event.duration:
                end_time = last_event.timestamp + timedelta(seconds=last_event.duration)
            else:
                end_time = last_event.timestamp

        # Détection voicemail et queue
        went_to_voicemail = any(e.is_voicemail() for e in events)
        queue_name = None
        queue_wait_time = None

        for event in events:
            if event.is_queue_call() and event.lastdata:
                queue_name = event.lastdata.split(',')[0] if ',' in event.lastdata else event.lastdata
                # Calculer le temps d'attente en queue: duration - billsec
                if event.duration and event.billsec:
                    queue_wait_time = event.duration - event.billsec
                break

        # Compter les participants uniques
        participants = set()
        for event in events:
            if event.src:
                participants.add(event.src)
            if event.dst:
                participants.add(event.dst)
            src_from_channel = self._extract_number_from_channel(event.channel)
            if src_from_channel:
                participants.add(src_from_channel)
            dst_from_channel = self._extract_number_from_channel(event.dstchannel)
            if dst_from_channel:
                participants.add(dst_from_channel)

        # === NOUVELLES ANALYSES DÉTAILLÉES ===

        # Construire les segments détaillés du chemin d'appel
        call_segments = self._build_call_segments(events)

        # Analyser les interactions IVR
        has_ivr, ivr_selections, ivr_path = self._analyze_ivr_interactions(events)

        # Analyser les détails RingGroup
        ringgroup_number, ringgroup_members, ringgroup_answerer, ringgroup_duration = self._analyze_ringgroup_details(events)

        # Identifier qui a répondu (avec nom si disponible via extensions_dict si fourni)
        answering_party, answering_party_name = self._identify_answering_party(events)

        # Identifier boîte vocale si utilisée
        voicemail_box = None
        if went_to_voicemail:
            vm_event = next((e for e in events if e.is_voicemail()), None)
            if vm_event:
                voicemail_box = vm_event.get_voicemail_box()

        # Détecter les conférences
        is_conference = any(e.is_conference_call() for e in events)
        conference_id = None
        if is_conference:
            conf_event = next((e for e in events if e.is_conference_call()), None)
            if conf_event and conf_event.lastdata:
                # Extraire l'ID de conférence depuis lastdata
                conf_parts = conf_event.lastdata.split(',')
                conference_id = conf_parts[0] if conf_parts else None

        return Call(
            # Identification
            uniqueid=first_event.linkedid,
            start_time=first_event.timestamp,

            # Participants
            source=first_event.src,
            destination=first_event.dst,
            original_caller_name=first_event.cnam,

            # État et classification
            status=self._get_call_status(events),
            type=self._get_call_direction(events, is_internal),
            is_internal=is_internal,
            is_click_to_call=False,

            # Durées et horodatages
            duration=self._get_call_billsec(events),
            answer_time=answer_time,
            end_time=end_time,
            wait_time=wait_time,
            ring_time=ring_time,

            # Analyse du chemin DÉTAILLÉE
            final_path=path,
            call_segments=call_segments,
            call_path_details=call_path_details,

            # Qui a répondu
            answering_party=answering_party,
            answering_party_name=answering_party_name,

            # Transferts et renvois
            transfers_from=transfers_from,
            transfers_to=transfers_to,
            forwards_from=forwards_from,
            forwards_to=forwards_to,

            # Applications spéciales
            went_to_voicemail=went_to_voicemail,
            voicemail_box=voicemail_box,
            queue_name=queue_name,
            queue_wait_time=queue_wait_time,

            # IVR (Interactive Voice Response)
            has_ivr=has_ivr,
            ivr_selections=ivr_selections,
            ivr_path=ivr_path,

            # RingGroups détaillés
            ringgroup_used=ringgroup_number,
            ringgroup_members_tried=ringgroup_members if ringgroup_members else [],
            ringgroup_answerer=ringgroup_answerer,
            ringgroup_ring_duration=ringgroup_duration,

            # Conférence
            is_conference=is_conference,
            conference_id=conference_id,

            # Routage et facturation
            did=first_event.did,
            accountcode=first_event.accountcode,
            peeraccount=first_event.peeraccount,
            userfield=first_event.userfield,

            # Métriques
            total_participants=len(participants),
            event_count=len(events)
        )

    def _build_call_segments(self, events: List[CallEvent]) -> List:
        """
        Construit la liste détaillée des segments d'appel avec durées et participants.

        Args:
            events: Liste des événements d'appel

        Returns:
            Liste des CallSegment
        """
        from call_analyzer.models.call import CallSegment

        segments = []
        segment_number = 0

        for event in events:
            # Extraction des parties source et destination
            src_number = self._extract_number_from_channel(event.channel) or event.src
            dst_number = self._extract_number_from_channel(event.dstchannel) or event.dst

            # Calcul des durées
            # Note: sans start/answer/end, on estime ring_duration = duration - billsec
            ring_duration = 0
            talk_duration = event.billsec

            if event.duration and event.billsec:
                ring_duration = event.duration - event.billsec

            # Extraction des sélections DTMF
            dtmf_selections = event.get_dtmf_selections()

            # Création du segment
            segment = CallSegment(
                segment_number=segment_number,
                from_party=src_number,
                to_party=dst_number,
                application=event.lastapp,
                context=event.context,
                protocol=event.get_protocol(),
                start_time=event.timestamp,  # start_time = calldate
                answer_time=event.timestamp if event.is_answered() else None,  # Approximation
                end_time=event.timestamp + timedelta(seconds=event.duration) if event.duration else event.timestamp,
                ring_duration=ring_duration,
                talk_duration=talk_duration,
                disposition=event.disposition,
                answered=event.is_answered(),
                dtmf_selections=dtmf_selections,
                queue_name=event.get_queue_name() if event.is_queue_call() else None
            )

            segments.append(segment)
            segment_number += 1

        return segments

    def _analyze_ivr_interactions(self, events: List[CallEvent]) -> tuple:
        """
        Analyse les interactions IVR (menus interactifs).

        Args:
            events: Liste des événements d'appel

        Returns:
            Tuple (has_ivr: bool, ivr_selections: dict, ivr_path: str)
        """
        has_ivr = any(e.is_ivr_call() for e in events)

        if not has_ivr:
            return False, {}, ""

        ivr_selections = {}
        ivr_path_parts = []
        step_number = 0

        for event in events:
            if event.is_ivr_call():
                dtmf = event.get_dtmf_selections()
                if dtmf:
                    step_key = f"step_{step_number}"
                    ivr_selections[step_key] = dtmf
                    ivr_path_parts.extend(dtmf)
                    step_number += 1

        ivr_path = " → ".join(ivr_path_parts) if ivr_path_parts else ""

        return has_ivr, ivr_selections, ivr_path

    def _analyze_ringgroup_details(self, events: List[CallEvent]) -> tuple:
        """
        Analyse détaillée des RingGroups: membres contactés, qui a répondu, durées.

        Args:
            events: Liste des événements d'appel

        Returns:
            Tuple (ringgroup_number, members_tried, answerer, ring_duration)
        """
        # Identifier si un RingGroup est utilisé
        ringgroup_events = [e for e in events if e.context == 'ext-group']

        if not ringgroup_events:
            return None, [], None, None

        # Numéro du RingGroup
        ringgroup_number = ringgroup_events[0].dst if ringgroup_events else None

        # Membres contactés
        members_tried = []
        answerer = None
        ring_duration = 0

        for event in ringgroup_events:
            # Extraire le membre du RingGroup
            member = self._extract_number_from_channel(event.dstchannel) or event.dst

            if member and member not in members_tried and member != ringgroup_number:
                members_tried.append(member)

            # Identifier qui a répondu
            if event.disposition == 'ANSWERED' and not answerer:
                answerer = member

            # Calculer durée totale de sonnerie du RingGroup
            # Note: sans start/answer, on estime avec duration - billsec
            if event.duration and event.billsec:
                member_ring = event.duration - event.billsec
                ring_duration = max(ring_duration, member_ring)
            elif event.duration:
                ring_duration = max(ring_duration, event.duration)

        return ringgroup_number, members_tried, answerer, ring_duration

    def _identify_answering_party(self, events: List[CallEvent], extensions_dict: dict = None) -> tuple:
        """
        Identifie précisément qui a répondu à l'appel.

        Args:
            events: Liste des événements d'appel
            extensions_dict: Dictionnaire des extensions avec noms

        Returns:
            Tuple (answering_party: str, answering_party_name: str)
        """
        # Trouver le premier événement ANSWERED
        answered_event = next((e for e in events if e.disposition == 'ANSWERED'), None)

        if not answered_event:
            return None, None

        # Extraire le numéro qui a répondu
        answering_party = self._extract_number_from_channel(answered_event.dstchannel) or answered_event.dst

        # Trouver le nom si disponible
        answering_party_name = None
        if extensions_dict and answering_party:
            answering_party_name = extensions_dict.get(answering_party)

        return answering_party, answering_party_name

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
                    # Horodatages
                    timestamp=row['calldate'],

                    # Identifiants
                    uniqueid=row['uniqueid'],
                    linkedid=row['linkedid'],
                    sequence=row['sequence'],

                    # Numéros et canaux
                    src=row['src'],
                    dst=row['dst'],
                    cnum=row['cnum'],
                    channel=row['channel'],
                    dstchannel=row['dstchannel'],

                    # Identification appelant
                    clid=row.get('clid', None),
                    cnam=row.get('cnam', None),
                    outbound_cnum=row.get('outbound_cnum', None),
                    outbound_cnam=row.get('outbound_cnam', None),
                    dst_cnam=row.get('dst_cnam', None),

                    # Contexte et applications
                    context=row['context'],
                    lastapp=row['lastapp'],
                    lastdata=row.get('lastdata', None),

                    # État
                    disposition=row['disposition'],

                    # Durées
                    duration=row.get('duration', None),
                    billsec=row['billsec'],

                    # Routage et facturation
                    did=row.get('did', None),
                    accountcode=row.get('accountcode', None),
                    peeraccount=row.get('peeraccount', None),

                    # Flags et données personnalisées
                    amaflags=row.get('amaflags', None),
                    userfield=row.get('userfield', None),

                    # Enregistrement
                    recordingfile=row.get('recordingfile', None)
                )
                for _, row in group.iterrows()
            ]
            call = self.analyze_call(events)
            if call:
                calls.append(call)

        return calls

    def to_dataframe(self, calls: List[Call]) -> pd.DataFrame:
        """
        Convertit une liste d'appels en DataFrame.

        Args:
            calls: Liste des appels à convertir

        Returns:
            DataFrame contenant les données d'appels avec TOUS les champs incluant
            les nouveaux champs détaillés (IVR, RingGroup, segments, etc.)
        """
        if not calls:
            return pd.DataFrame()

        data = [{
            # Horodatages
            'call_date': call.start_time,
            'answer_date': call.answer_time,
            'end_date': call.end_time if call.end_time else (call.start_time + timedelta(seconds=call.duration)),

            # Identification
            'uniqueid': call.uniqueid,

            # Participants
            'src': call.source,
            'dst': call.destination,
            'original_caller_name': call.original_caller_name,

            # Qui a répondu (NOUVEAU)
            'answering_party': call.answering_party,
            'answering_party_name': call.answering_party_name,

            # État et classification
            'status': call.status,
            'answered': call.status == 'ANSWERED',
            'type_appel': call.type,
            'is_internal': call.is_internal,
            'is_click_to_call': call.is_click_to_call,

            # Durées et temps d'attente
            'billsec': call.duration,
            'wait_time': call.wait_time,
            'ring_time': call.ring_time,

            # Chemins et transferts
            'path': call.final_path,
            'detailed_path': call.get_detailed_path_string(),  # NOUVEAU: chemin avec durées
            'segments_count': call.segments_count,  # NOUVEAU
            'transfert_depuis': call.transfers_from,
            'transfert_vers': call.transfers_to,
            'renvoi_depuis': call.forwards_from,
            'renvoi_vers': call.forwards_to,
            'has_transfer': call.has_transfer,
            'has_forward': call.has_forward,

            # Applications spéciales
            'went_to_voicemail': call.went_to_voicemail,
            'voicemail_box': call.voicemail_box,  # NOUVEAU
            'queue_name': call.queue_name,
            'queue_wait_time': call.queue_wait_time,

            # IVR (NOUVEAU)
            'has_ivr': call.has_ivr,
            'ivr_path': call.ivr_path,
            'ivr_summary': call.get_ivr_summary(),

            # RingGroups (NOUVEAU détaillé)
            'ringgroup_used': call.ringgroup_used,
            'ringgroup_members_tried': ','.join(call.ringgroup_members_tried) if call.ringgroup_members_tried else None,
            'ringgroup_answerer': call.ringgroup_answerer,
            'ringgroup_ring_duration': call.ringgroup_ring_duration,
            'ringgroup_summary': call.get_ringgroup_summary(),

            # Conférence (NOUVEAU)
            'is_conference': call.is_conference,
            'conference_id': call.conference_id,

            # Routage et facturation
            'did': call.did,
            'accountcode': call.accountcode,
            'peeraccount': call.peeraccount,
            'userfield': call.userfield,

            # Métriques
            'total_participants': call.total_participants,
            'event_count': call.event_count,

            # Indicateurs SLA
            'sla_compliant_20s': call.sla_compliant(20),
            'sla_compliant_30s': call.sla_compliant(30),
            'is_missed': call.is_missed(),
            'is_successful_outbound': call.is_successful_outbound()
        } for call in calls]

        df = pd.DataFrame(data)

        # Conversion des colonnes de date en datetime
        for col in ['call_date', 'answer_date', 'end_date']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

        return df