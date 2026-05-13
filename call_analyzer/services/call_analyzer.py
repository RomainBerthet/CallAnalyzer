import logging
import re
from datetime import timedelta
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from ..models.call import Call
from ..models.call_event import CallEvent

logger = logging.getLogger(__name__)


class CallAnalyzer:
    """Analyse les données d'appels pour extraire des informations pertinentes."""

    def __init__(self, internal_numbers: Set[str], reference_numbers: Optional[List[str]] = None,
                 ring_group_numbers: Optional[Set[str]] = None, extension_numbers: Optional[Set[str]] = None,
                 display_names: Optional[Dict[str, str]] = None):
        self.internal_numbers = {str(number) for number in internal_numbers}
        self.reference_numbers = reference_numbers
        self.ring_group_numbers = {str(number) for number in ring_group_numbers} if ring_group_numbers else set()
        self.extension_numbers = {str(number) for number in extension_numbers} if extension_numbers else set()
        self.display_names = {
            str(number): display
            for number, display in (display_names or {}).items()
            if display
        }
        # Compile once — used many times per analysis run
        self._channel_patterns = [
            re.compile(r'PJSIP/(\d+)-'),
            re.compile(r'Local/([^@]+)@'),
            re.compile(r'SIP/(\d+)-'),
            re.compile(r'IAX2/(\d+)-'),
        ]

    def _is_internal_number(self, number: str) -> bool:
        return str(number) in self.internal_numbers

    def _is_ring_group_number(self, number: str) -> bool:
        return str(number) in self.ring_group_numbers

    def _is_extension_number(self, number: str) -> bool:
        return str(number) in self.extension_numbers

    def _get_path_entity_type(self, number: str, call_type: str) -> str:
        if self._is_ring_group_number(number):
            return 'ring_group'
        if self._is_extension_number(number):
            return 'extension'
        if call_type in ('group_member', 'group_member_answered'):
            return 'group_member'
        if self._is_internal_number(number):
            return 'internal_number'
        return 'external'

    def _get_path_display(self, number: str) -> Optional[str]:
        return self.display_names.get(str(number))

    def _format_path_label(self, number: str, call_type: str, disposition: str = None) -> str:
        entity_type = self._get_path_entity_type(number, call_type)
        display = self._get_path_display(number)
        if entity_type == 'ring_group':
            label = f"Ring group {display} ({number})" if display else f"Ring group {number}"
        elif entity_type == 'extension':
            label = f"Extension {display} ({number})" if display else f"Extension {number}"
        elif entity_type == 'group_member':
            label = f"Membre groupe {number}"
        elif entity_type == 'internal_number':
            label = f"Interne {number}"
        else:
            label = f"Externe {number}"

        if disposition == 'ANSWERED':
            return f"{label} (ANSWERED)"
        if disposition == 'NO ANSWER':
            return f"{label} (NO ANSWER)"
        return label

    def _extract_number_from_channel(self, channel: str) -> Optional[str]:
        if not channel:
            return None
        for pattern in self._channel_patterns:
            m = pattern.search(channel)
            if m:
                return m.group(1).lstrip('9')
        return None

    def _check_if_forward(self, events: List[CallEvent]) -> bool:
        return any('Local/0' in e.channel and e.context == 'from-internal' for e in events)

    def _get_forward_call_events(self, events: List[CallEvent]) -> List[CallEvent]:
        return [e for e in events if 'Local/0' in e.channel and e.context == 'from-internal']

    def _check_if_group_call(self, events: List[CallEvent]) -> bool:
        return any(e.context == 'ext-group' and 'Local/' in e.dstchannel for e in events)

    def _get_call_direction(self, events: List[CallEvent], is_internal: bool,
                            trunk_in_channel: bool = None, trunk_in_dstchannel: bool = None) -> str:
        if is_internal:
            if self.reference_numbers:
                if events[0].src in self.reference_numbers:
                    return 'sortant'
                if events[0].dst in self.reference_numbers:
                    return 'entrant'
            return 'interne'

        if trunk_in_channel is None:
            trunk_in_channel = any('trunk' in e.channel.lower() for e in events)
        if trunk_in_dstchannel is None:
            trunk_in_dstchannel = any('trunk' in e.dstchannel.lower() for e in events)

        if trunk_in_channel:
            return 'entrant'
        if trunk_in_dstchannel:
            return 'sortant'
        return 'interne'

    def _get_call_status(self, events: List[CallEvent],
                         dispositions: Set[str] = None, has_forward: bool = None) -> str:
        # Reference-number mode: check per-event which one involves our number
        if self.reference_numbers and len(self.reference_numbers) == 1:
            if has_forward is None:
                has_forward = self._check_if_forward(events)
            if has_forward:
                if dispositions is None:
                    dispositions = {e.disposition for e in events}
                return 'ANSWERED' if 'ANSWERED' in dispositions else 'NO ANSWER'

            status = 'NO ANSWER'
            for event in events:
                src = self._extract_number_from_channel(event.channel) or event.src
                dst = (self._extract_number_from_channel(event.dstchannel)
                       if not (event.dstchannel and 'Local/0' in event.dstchannel)
                       else event.dst) or event.dst
                if src in self.reference_numbers or dst in self.reference_numbers:
                    status = event.disposition
                    if status == 'ANSWERED':
                        return status
            return status

        # General case — use pre-computed dispositions set
        if dispositions is None:
            dispositions = {e.disposition for e in events}
        if 'ANSWERED' in dispositions:
            return 'ANSWERED'
        if 'BUSY' in dispositions:
            return 'BUSY'
        if 'CONGESTION' in dispositions:
            return 'CONGESTION'
        if 'NO ANSWER' in dispositions:
            return 'NO ANSWER'
        return 'FAILED'

    def _event_targets_number(self, event: CallEvent, number: Optional[str]) -> bool:
        if not number:
            return False
        target = str(number)
        dst = str(event.dst or '')
        dstchannel_number = self._extract_number_from_channel(event.dstchannel or '') or ''
        return dst == target or dstchannel_number == target

    def _get_call_billsec(self, events: List[CallEvent],
                          has_forward: bool = None, has_group: bool = None,
                          forwards_to: Optional[str] = None) -> int:
        if not events:
            return 0

        if has_forward is None:
            has_forward = self._check_if_forward(events)
        if has_group is None:
            has_group = self._check_if_group_call(events)

        if self.reference_numbers and len(self.reference_numbers) == 1:
            if has_forward and has_group:
                return sum(e.billsec for e in events)
            billsec = 0
            for event in events:
                src = self._extract_number_from_channel(event.channel) or event.src
                dst = (self._extract_number_from_channel(event.dstchannel)
                       if not (event.dstchannel and 'Local/0' in event.dstchannel)
                       else event.dst) or event.dst
                if src in self.reference_numbers or dst in self.reference_numbers:
                    billsec += event.billsec
            return billsec

        if has_forward:
            forward_events = self._get_forward_call_events(events)
            billsec = sum(e.billsec for e in forward_events)
            if billsec > 0:
                return billsec

            answered_forward_events = [
                e for e in events
                if e.billsec > 0
                and e.disposition == 'ANSWERED'
                and (
                    self._event_targets_number(e, forwards_to)
                    or (e.context in ('from-internal', 'outbound-allroutes') and 'trunk' in (e.dstchannel or '').lower())
                )
            ]
            if answered_forward_events:
                return max(e.billsec for e in answered_forward_events)

            answered_events = [e.billsec for e in events if e.billsec > 0 and e.disposition == 'ANSWERED']
            if answered_events:
                return max(answered_events)

            return 0
        if has_group:
            return sum(e.billsec for e in events if not (e.context == 'ext-group' and 'Local/' in e.dstchannel))
        return sum(e.billsec for e in events)

    def _identify_click_to_call(self, events: List[CallEvent]) -> Tuple[
            bool, Optional[str], Optional[str], int, List[str], List[str]]:
        is_click_to_call = any(e.cnam and 'Répondre pour appeler le' in e.cnam for e in events)
        initiator_answered: List[str] = []
        dest_forwards: List[str] = []

        if is_click_to_call and events:
            first_event = events[0]
            first_event_src = self._extract_number_from_channel(first_event.channel) or first_event.src
            event_macro_dial = next((e for e in events if 'macro-dial' in e.context), first_event)

            if first_event_src.startswith('0') or first_event_src.startswith('+'):
                src = event_macro_dial.src
            else:
                src = first_event_src

            dst = first_event.dst
            duration = event_macro_dial.billsec

            # Base du canal CTC (sans ;1/;2) pour distinguer initiateur (;2) et destination (;1).
            ctc_base = first_event.channel.rsplit(';', 1)[0]

            for fe in self._get_forward_call_events(events):
                duration += fe.billsec
                fwd_base = fe.channel.rsplit(';', 1)[0]
                # Renvoi côté destination : un followme-check relie CTC;1 → Local/fwd;1.
                is_dest_side = any(
                    e.context == 'followme-check'
                    and e.channel.endswith(';1')
                    and e.channel.rsplit(';', 1)[0] == ctc_base
                    and e.dstchannel
                    and e.dstchannel.rsplit(';', 1)[0] == fwd_base
                    for e in events
                )
                if is_dest_side:
                    dest_forwards.append(fe.dst + (' (ANSWERED)' if fe.disposition == 'ANSWERED' else ''))
                elif fe.disposition == 'ANSWERED':
                    # Renvoi côté initiateur ayant décroché (ex : mobile de 163).
                    initiator_answered.append(fe.dst + ' (ANSWERED)')

            return True, src, dst, duration, initiator_answered, dest_forwards

        return False, None, None, 0, initiator_answered, dest_forwards

    def _identify_actions_by_context(self, events: List[CallEvent]) -> Tuple[
            Optional[str], Optional[str], Optional[str], Optional[str], str, List[dict]]:
        transfers_from = transfers_to = forwards_from = forwards_to = None
        path = []
        call_path_details = []
        virtual_forward = ''
        seen_path_keys: Set[str] = set()
        last_path_key = ''
        group_members = {}
        internal_calls = {}

        def base_number(number: str) -> str:
            return number.split(" ")[0] if number else ''

        def add_to_path(number: str, call_type: str, disposition: str = None, timestamp=None):
            nonlocal last_path_key
            if not number:
                return
            suffix = f" ({disposition})" if disposition == 'ANSWERED' else ''
            path_key = number + suffix
            if (base_number(last_path_key) != base_number(path_key)
                    and path_key not in seen_path_keys
                    and base_number(path_key) != virtual_forward):
                path.append(self._format_path_label(number, call_type, disposition))
                seen_path_keys.add(path_key)
                last_path_key = path_key
                call_path_details.append({
                    'number': number,
                    'display': self._get_path_display(number),
                    'type': call_type,
                    'entity_type': self._get_path_entity_type(number, call_type),
                    'disposition': disposition,
                    'timestamp': timestamp.isoformat() if hasattr(timestamp, 'isoformat') else timestamp,
                })

        # Première passe: groupes et appels internes
        for event in events:
            is_local = event.dstchannel and 'Local/' in event.dstchannel
            src_number = self._extract_number_from_channel(event.channel) or event.src
            group_member_number = self._extract_number_from_channel(event.dstchannel) if event.context == 'ext-group' else None
            dst_number = (event.dst if is_local
                          else self._extract_number_from_channel(event.dstchannel) or event.dst)

            if event.context == 'ext-group':
                group_id = event.dst
                if group_id not in group_members:
                    group_members[group_id] = []
                if group_member_number and group_member_number not in group_members[group_id]:
                    group_members[group_id].append(group_member_number)

            if event.context == 'from-internal':
                call_id = f"{src_number}_{dst_number}_{event.timestamp}"
                internal_calls[call_id] = {
                    'src': src_number,
                    'dst': dst_number,
                    'time': event.timestamp,
                    'disposition': event.disposition
                }

        # Deuxième passe: construction du chemin
        for event in events:
            is_local = event.dstchannel and 'Local/' in event.dstchannel
            src_number = self._extract_number_from_channel(event.channel) or event.src
            group_member_number = self._extract_number_from_channel(event.dstchannel) if event.context == 'ext-group' else None
            dst_number = (event.dst if is_local
                          else self._extract_number_from_channel(event.dstchannel) or event.dst)

            if src_number and dst_number and not path:
                add_to_path(src_number, 'source', timestamp=event.timestamp)
                if event.dst != dst_number:
                    add_to_path(event.dst, 'initial_destination', timestamp=event.timestamp)

            if src_number and dst_number:
                if event.context == 'ext-group':
                    call_type = 'ring_group'
                    dst_number = event.dst
                elif event.context == 'from-internal':
                    call_type = 'internal'
                elif event.context in ('from-trunk', 'outbound-allroutes'):
                    call_type = 'external'
                else:
                    call_type = 'standard'
                if self._is_ring_group_number(dst_number):
                    call_type = 'ring_group'
                # La jambe ;1 du canal Local (followme-check avec dstchannel=Local/...)
                # porte event.dst=163 comme dst_number (is_local=True) mais ne représente
                # pas un nouveau saut — le gestionnaire followme-check ci-dessous s'en charge.
                if not (
                    (event.context == 'followme-check' and is_local)
                    or (event.context == 'from-internal' and event.channel and 'Local/0' in event.channel)
                ):
                    add_to_path(dst_number, call_type, event.disposition, timestamp=event.timestamp)

            if event.context == 'followme-check' and event.dstchannel and 'Local/' in event.dstchannel:
                if event.dst:
                    add_to_path(event.dst, 'forward_source', None, timestamp=event.timestamp)
                local_key = event.dstchannel.split(';')[0]
                match = next(
                    (e for e in events if e.channel and e.channel.startswith(local_key) and e.context == 'from-internal'),
                    None
                )
                if match:
                    fwd_number = self._extract_number_from_channel(match.dstchannel) or match.dst
                    if not forwards_to:
                        forwards_to = fwd_number
                    if event.disposition == 'ANSWERED':
                        add_to_path(fwd_number, 'forwarded', match.disposition, timestamp=match.timestamp)
                        virtual_forward = fwd_number
                        continue

            if event.context == 'from-internal':
                if event.channel and 'Local/0' in event.channel:
                    if not forwards_to:
                        forwards_to = dst_number
                    if event.disposition == 'ANSWERED':
                        add_to_path(dst_number, 'forward_answered', event.disposition, timestamp=event.timestamp)
                    virtual_forward = dst_number
                elif 'Local/' in event.channel and not is_local:
                    if not transfers_to and dst_number:
                        transfers_to = dst_number
                        add_to_path(dst_number, 'transfer_internal', event.disposition, timestamp=event.timestamp)

            elif event.context == 'ext-local' and event.dst != dst_number:
                if not transfers_to:
                    transfers_to = dst_number
                    add_to_path(dst_number, 'transfer_external', event.disposition, timestamp=event.timestamp)

            elif event.context == 'ext-group':
                group_id = event.dst
                if group_id in group_members and group_member_number in group_members[group_id]:
                    if event.disposition == 'ANSWERED':
                        add_to_path(group_member_number, 'group_member_answered', event.disposition, timestamp=event.timestamp)

        # Appels internes manqués
        for call in internal_calls.values():
            if call['disposition'] == 'NO ANSWER' and call['dst'] not in seen_path_keys:
                if base_number(last_path_key) != base_number(call['dst']):
                    add_to_path(call['dst'], 'missed_internal', 'NO ANSWER', timestamp=call['time'])

        return transfers_from, transfers_to, forwards_from, forwards_to, " --> ".join(path), call_path_details

    def analyze_call(self, events: List[CallEvent]) -> Optional[Call]:
        if not events:
            return None

        events = sorted(events, key=lambda e: e.sequence)

        # Single pre-scan — collect all flags in one pass
        has_forward = False
        has_group = False
        trunk_in_channel = False
        trunk_in_dstchannel = False
        is_ctc = False
        dispositions: Set[str] = set()

        for e in events:
            ch = e.channel or ''
            dch = e.dstchannel or ''
            if not has_forward and 'Local/0' in ch and e.context == 'from-internal':
                has_forward = True
            if not has_group and e.context == 'ext-group' and 'Local/' in dch:
                has_group = True
            if not trunk_in_channel and 'trunk' in ch.lower():
                trunk_in_channel = True
            if not trunk_in_dstchannel and 'trunk' in dch.lower():
                trunk_in_dstchannel = True
            if not is_ctc and e.cnam and 'Répondre pour appeler le' in e.cnam:
                is_ctc = True
            dispositions.add(e.disposition)

        # Click-to-Call path
        if is_ctc:
            _, src_ctc, dst_ctc, duration_ctc, initiator_answered, dest_forwards = self._identify_click_to_call(events)
            if src_ctc and dst_ctc:
                is_both_internal = self._is_internal_number(src_ctc) and self._is_internal_number(dst_ctc)
                # La destination a-t-elle décroché directement (pas via un renvoi) ?
                # En click-to-call, la jambe ;2 correspond à l'initiateur et la jambe ;1 à la destination.
                # Une réponse sur le renvoi mobile de l'initiateur ne doit donc pas marquer dst_ctc comme répondu.
                ctc_base = events[0].channel.rsplit(';', 1)[0]
                dst_answered = (
                    not dest_forwards
                    and any(
                        e.disposition == 'ANSWERED'
                        and e.channel
                        and e.channel.endswith(';1')
                        and e.channel.rsplit(';', 1)[0] == ctc_base
                        and e.dstchannel
                        and 'Local/' not in e.dstchannel
                        for e in events
                    )
                )
                # Structure : initiateur --> [appareil répondant] --> destination --> [renvois destination]
                path_parts = [self._format_path_label(src_ctc, 'source')]
                path_parts.extend(
                    self._format_path_label(item.split(' ')[0], 'click_to_call_answered', 'ANSWERED')
                    for item in initiator_answered
                )
                path_parts.append(self._format_path_label(dst_ctc, 'destination', 'ANSWERED' if dst_answered else None))
                path_parts.extend(
                    self._format_path_label(item.split(' ')[0], 'forward_answered', 'ANSWERED')
                    for item in dest_forwards
                )
                path_details = [{
                    'number': src_ctc,
                    'display': self._get_path_display(src_ctc),
                    'type': 'source',
                    'entity_type': self._get_path_entity_type(src_ctc, 'source'),
                    'disposition': None,
                }]
                path_details.extend(
                    {
                        'number': item.split(' ')[0],
                        'display': self._get_path_display(item.split(' ')[0]),
                        'type': 'click_to_call_answered',
                        'entity_type': self._get_path_entity_type(item.split(' ')[0], 'click_to_call_answered'),
                        'disposition': 'ANSWERED',
                    }
                    for item in initiator_answered
                )
                path_details.append({
                    'number': dst_ctc,
                    'display': self._get_path_display(dst_ctc),
                    'type': 'destination',
                    'entity_type': self._get_path_entity_type(dst_ctc, 'destination'),
                    'disposition': 'ANSWERED' if dst_answered else None,
                })
                path_details.extend(
                    {
                        'number': item.split(' ')[0],
                        'display': self._get_path_display(item.split(' ')[0]),
                        'type': 'forward_answered',
                        'entity_type': self._get_path_entity_type(item.split(' ')[0], 'forward_answered'),
                        'disposition': 'ANSWERED',
                    }
                    for item in dest_forwards
                )
                return Call(
                    start_time=events[0].timestamp,
                    uniqueid=events[0].linkedid,
                    source=src_ctc,
                    destination=dst_ctc,
                    duration=duration_ctc,
                    status=self._get_call_status(events, dispositions, has_forward),
                    type=self._get_call_direction(events, is_both_internal, trunk_in_channel, trunk_in_dstchannel),
                    is_internal=is_both_internal,
                    transfers_from=None, transfers_to=None, forwards_from=None,
                    forwards_to=dest_forwards[0].split(' ')[0] if dest_forwards else None,
                    is_click_to_call=True,
                    final_path=" --> ".join(path_parts),
                    final_path_details=path_details,
                    original_caller_name=events[0].cnam,
                    did=events[0].did,
                    accountcode=events[0].accountcode,
                    userfield=events[0].userfield
                )

        first_event = events[0]
        if not first_event.src or not first_event.dst:
            return None

        transfers_from, transfers_to, forwards_from, forwards_to, path, path_details = self._identify_actions_by_context(events)
        is_internal = self._is_internal_number(first_event.src) and self._is_internal_number(first_event.dst)

        return Call(
            start_time=first_event.timestamp,
            uniqueid=first_event.linkedid,
            source=first_event.src,
            destination=first_event.dst,
            duration=self._get_call_billsec(events, has_forward or bool(forwards_to), has_group, forwards_to),
            status=self._get_call_status(events, dispositions, has_forward),
            type=self._get_call_direction(events, is_internal, trunk_in_channel, trunk_in_dstchannel),
            is_internal=is_internal,
            transfers_from=transfers_from,
            transfers_to=transfers_to,
            forwards_from=forwards_from,
            forwards_to=forwards_to,
            final_path=path,
            final_path_details=path_details,
            is_click_to_call=False,
            original_caller_name=first_event.cnam,
            did=first_event.did,
            accountcode=first_event.accountcode,
            userfield=first_event.userfield
        )

    def process_dataframe(self, df: pd.DataFrame) -> List[Call]:
        if df.empty:
            logger.warning("Le DataFrame est vide.")
            return []

        # Determine which optional columns exist once, not per row
        optional_cols = {col: col in df.columns
                         for col in ('cnam', 'did', 'accountcode', 'userfield', 'amaflags', 'duration', 'clid')}

        calls = []
        # sort=False: SQL already orders by linkedid, sequence
        for linkedid, group in df.groupby('linkedid', sort=False):
            events = []
            # itertuples is ~5-10x faster than iterrows for row-by-row access
            for row in group.itertuples(index=False):
                events.append(CallEvent(
                    timestamp=row.calldate,
                    uniqueid=row.uniqueid,
                    linkedid=row.linkedid,
                    src=row.src,
                    dst=row.dst,
                    channel=row.channel,
                    dstchannel=row.dstchannel,
                    disposition=row.disposition,
                    cnum=row.cnum,
                    billsec=row.billsec,
                    sequence=row.sequence,
                    context=row.context,
                    lastapp=row.lastapp,
                    cnam=getattr(row, 'cnam', None) if optional_cols['cnam'] else None,
                    did=getattr(row, 'did', None) if optional_cols['did'] else None,
                    accountcode=getattr(row, 'accountcode', None) if optional_cols['accountcode'] else None,
                    userfield=getattr(row, 'userfield', None) if optional_cols['userfield'] else None,
                    amaflags=getattr(row, 'amaflags', None) if optional_cols['amaflags'] else None,
                    duration=getattr(row, 'duration', None) if optional_cols['duration'] else None,
                    clid=getattr(row, 'clid', None) if optional_cols['clid'] else None,
                ))
            call = self.analyze_call(events)
            if call:
                calls.append(call)

        return calls

    def to_dataframe(self, calls: List[Call]) -> pd.DataFrame:
        if not calls:
            return pd.DataFrame()

        data = [{
            'call_date': call.start_time,
            'uniqueid': call.uniqueid,
            'src': call.source,
            'dst': call.destination,
            'billsec': call.duration,
            'status': call.status,
            'answered': call.status == 'ANSWERED',
            'type_appel': call.type,
            'is_internal': call.is_internal,
            'renvoi_vers': call.forwards_to,
            'path': call.final_path,
            'path_details': call.final_path_details,
            'is_click_to_call': call.is_click_to_call,
            'original_caller_name': call.original_caller_name,
            'did': call.did,
            'accountcode': call.accountcode,
            'userfield': call.userfield,
        } for call in calls]

        df = pd.DataFrame(data)
        df['call_date'] = pd.to_datetime(df['call_date'])
        # Vectorised end_date — avoids a timedelta() per row in the comprehension
        df['end_date'] = df['call_date'] + pd.to_timedelta(df['billsec'], unit='s')
        return df
