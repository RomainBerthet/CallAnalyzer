import logging
import re
from datetime import timedelta
from typing import List, Optional, Set, Tuple

import pandas as pd

from ..models.call import Call
from ..models.call_event import CallEvent

logger = logging.getLogger(__name__)


class CallAnalyzer:
    """Analyse les données d'appels pour extraire des informations pertinentes."""

    def __init__(self, internal_numbers: Set[str], reference_numbers: Optional[List[str]] = None):
        self.internal_numbers = internal_numbers
        self.reference_numbers = reference_numbers
        # Compile once — used many times per analysis run
        self._channel_patterns = [
            re.compile(r'PJSIP/(\d+)-'),
            re.compile(r'Local/([^@]+)@'),
            re.compile(r'SIP/(\d+)-'),
            re.compile(r'IAX2/(\d+)-'),
        ]

    def _is_internal_number(self, number: str) -> bool:
        return str(number) in self.internal_numbers

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

    def _get_call_billsec(self, events: List[CallEvent],
                          has_forward: bool = None, has_group: bool = None) -> int:
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
            return sum(e.billsec for e in events if 'Local/0' in e.channel and e.context == 'from-internal')
        if has_group:
            return sum(e.billsec for e in events if not (e.context == 'ext-group' and 'Local/' in e.dstchannel))
        return sum(e.billsec for e in events)

    def _identify_click_to_call(self, events: List[CallEvent]) -> Tuple[
            bool, Optional[str], Optional[str], int, List[str]]:
        is_click_to_call = any(e.cnam and 'Répondre pour appeler le' in e.cnam for e in events)
        forward = []

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

            for fe in self._get_forward_call_events(events):
                duration += fe.billsec
                forward.append(fe.dst)

            return True, src, dst, duration, forward

        return False, None, None, 0, forward

    def _identify_actions_by_context(self, events: List[CallEvent]) -> Tuple[
            Optional[str], Optional[str], Optional[str], Optional[str], str, List[dict]]:
        transfers_from = transfers_to = forwards_from = forwards_to = None
        path = []
        call_path_details = []
        virtual_forward = ''
        seen_path_keys: Set[str] = set()
        group_members = {}
        internal_calls = {}

        def base_number(number: str) -> str:
            return number.split(" ")[0] if number else ''

        def add_to_path(number: str, call_type: str, disposition: str = None, timestamp=None):
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
                    'timestamp': timestamp
                })

        # Première passe: groupes et appels internes
        for event in events:
            is_local = event.dstchannel and 'Local/' in event.dstchannel
            src_number = self._extract_number_from_channel(event.channel) or event.src
            dst_number = (event.dst if is_local
                          else self._extract_number_from_channel(event.dstchannel) or event.dst)

            if event.context == 'ext-group':
                group_id = event.dst
                if group_id not in group_members:
                    group_members[group_id] = []
                if dst_number and dst_number not in group_members[group_id]:
                    group_members[group_id].append(dst_number)

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
            dst_number = (event.dst if is_local
                          else self._extract_number_from_channel(event.dstchannel) or event.dst)

            if src_number and dst_number and not path:
                add_to_path(src_number, 'source', timestamp=event.timestamp)
                if event.dst != dst_number:
                    add_to_path(event.dst, 'initial_destination', timestamp=event.timestamp)

            if src_number and dst_number:
                if event.context == 'ext-group':
                    call_type = 'group_call'
                elif event.context == 'from-internal':
                    call_type = 'internal'
                elif event.context in ('from-trunk', 'outbound-allroutes'):
                    call_type = 'external'
                else:
                    call_type = 'standard'
                add_to_path(dst_number, call_type, event.disposition, timestamp=event.timestamp)

            if event.context == 'followme-check' and event.dstchannel and 'Local/' in event.dstchannel and event.disposition == 'ANSWERED':
                local_key = event.dstchannel.split(';')[0]
                match = next(
                    (e for e in events if e.channel and e.channel.startswith(local_key) and e.context == 'from-internal'),
                    None
                )
                if match:
                    forwards_to = self._extract_number_from_channel(match.dstchannel) or match.dst
                    add_to_path(forwards_to, 'forwarded', match.disposition, timestamp=match.timestamp)
                    virtual_forward = forwards_to
                    continue

            if event.context == 'from-internal':
                if event.channel and 'Local/0' in event.channel:
                    if event.disposition == 'ANSWERED':
                        forwards_to = dst_number
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
                if group_id in group_members and dst_number in group_members[group_id]:
                    if event.disposition == 'ANSWERED':
                        add_to_path(dst_number, 'group_answered', event.disposition, timestamp=event.timestamp)

        # Appels internes manqués
        for call in internal_calls.values():
            if call['disposition'] == 'NO ANSWER' and call['dst'] not in seen_path_keys:
                if base_number(path[-1] if path else '') != base_number(call['dst']):
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
            _, src_ctc, dst_ctc, duration_ctc, forward_numbers = self._identify_click_to_call(events)
            if src_ctc and dst_ctc:
                is_both_internal = self._is_internal_number(src_ctc) and self._is_internal_number(dst_ctc)
                return Call(
                    start_time=events[0].timestamp,
                    uniqueid=events[0].linkedid,
                    source=src_ctc,
                    destination=dst_ctc,
                    duration=duration_ctc,
                    status=self._get_call_status(events, dispositions, has_forward),
                    type=self._get_call_direction(events, is_both_internal, trunk_in_channel, trunk_in_dstchannel),
                    is_internal=is_both_internal,
                    transfers_from=None, transfers_to=None, forwards_from=None, forwards_to=None,
                    is_click_to_call=True,
                    final_path=" --> ".join([src_ctc] + forward_numbers + [dst_ctc]),
                    original_caller_name=events[0].cnam,
                    did=events[0].did,
                    accountcode=events[0].accountcode,
                    userfield=events[0].userfield
                )

        first_event = events[0]
        if not first_event.src or not first_event.dst:
            return None

        transfers_from, transfers_to, forwards_from, forwards_to, path, _ = self._identify_actions_by_context(events)
        is_internal = self._is_internal_number(first_event.src) and self._is_internal_number(first_event.dst)

        return Call(
            start_time=first_event.timestamp,
            uniqueid=first_event.linkedid,
            source=first_event.src,
            destination=first_event.dst,
            duration=self._get_call_billsec(events, has_forward, has_group),
            status=self._get_call_status(events, dispositions, has_forward),
            type=self._get_call_direction(events, is_internal, trunk_in_channel, trunk_in_dstchannel),
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
            'is_click_to_call': call.is_click_to_call,
            'original_caller_name': call.original_caller_name,
            'did': call.did,
        } for call in calls]

        df = pd.DataFrame(data)
        df['call_date'] = pd.to_datetime(df['call_date'])
        # Vectorised end_date — avoids a timedelta() per row in the comprehension
        df['end_date'] = df['call_date'] + pd.to_timedelta(df['billsec'], unit='s')
        return df
