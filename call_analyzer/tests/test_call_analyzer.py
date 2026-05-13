from datetime import datetime
from unittest import TestCase

import pandas as pd

from call_analyzer.services.call_analyzer import CallAnalyzer


class CallAnalyzerTest(TestCase):
    def test_incoming_external_forward_uses_outbound_trunk_billsec(self):
        df = pd.DataFrame([
            {
                'calldate': datetime(2026, 5, 12, 10, 0, 0),
                'uniqueid': 'call-1.1',
                'linkedid': 'call-1',
                'src': '0123456789',
                'dst': '163',
                'channel': 'PJSIP/trunk-in-00000001',
                'dstchannel': 'Local/163@from-trunk-00000002;1',
                'disposition': 'ANSWERED',
                'cnum': '0123456789',
                'billsec': 0,
                'sequence': 1,
                'context': 'from-trunk',
                'lastapp': 'Dial',
            },
            {
                'calldate': datetime(2026, 5, 12, 10, 0, 1),
                'uniqueid': 'call-1.2',
                'linkedid': 'call-1',
                'src': '163',
                'dst': '0612345678',
                'channel': 'Local/0612345678@from-internal-00000003;1',
                'dstchannel': '',
                'disposition': 'ANSWERED',
                'cnum': '0123456789',
                'billsec': 0,
                'sequence': 2,
                'context': 'from-internal',
                'lastapp': 'Dial',
            },
            {
                'calldate': datetime(2026, 5, 12, 10, 0, 1),
                'uniqueid': 'call-1.3',
                'linkedid': 'call-1',
                'src': '163',
                'dst': '0612345678',
                'channel': 'PJSIP/163-00000004',
                'dstchannel': 'PJSIP/trunk-out-00000005',
                'disposition': 'ANSWERED',
                'cnum': '0123456789',
                'billsec': 42,
                'sequence': 3,
                'context': 'outbound-allroutes',
                'lastapp': 'Dial',
            },
        ])

        analyzer = CallAnalyzer(internal_numbers={'163'})
        calls = analyzer.process_dataframe(df)
        result = analyzer.to_dataframe(calls)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['type_appel'], 'entrant')
        self.assertEqual(result.iloc[0]['renvoi_vers'], '0612345678')
        self.assertEqual(result.iloc[0]['billsec'], 42)
        self.assertEqual(
            result.iloc[0]['path'],
            'Externe 0123456789 --> Extension 163 (ANSWERED) --> Externe 0612345678 (ANSWERED)',
        )
        self.assertEqual(
            [(step['number'], step['type'], step['disposition']) for step in result.iloc[0]['path_details']],
            [
                ('0123456789', 'source', None),
                ('163', 'external', 'ANSWERED'),
                ('0612345678', 'forward_answered', 'ANSWERED'),
            ],
        )

    def test_ring_group_path_distinguishes_group_from_member_extension(self):
        df = pd.DataFrame([
            {
                'calldate': datetime(2026, 5, 12, 11, 0, 0),
                'uniqueid': 'call-2.1',
                'linkedid': 'call-2',
                'src': '0123456789',
                'dst': '600',
                'channel': 'PJSIP/trunk-in-00000001',
                'dstchannel': 'Local/600@from-trunk-00000002;1',
                'disposition': 'ANSWERED',
                'cnum': '0123456789',
                'billsec': 0,
                'sequence': 1,
                'context': 'from-trunk',
                'lastapp': 'Dial',
            },
            {
                'calldate': datetime(2026, 5, 12, 11, 0, 1),
                'uniqueid': 'call-2.2',
                'linkedid': 'call-2',
                'src': '0123456789',
                'dst': '600',
                'channel': 'Local/600@ext-group-00000002;2',
                'dstchannel': 'Local/101@from-internal-00000003;1',
                'disposition': 'ANSWERED',
                'cnum': '0123456789',
                'billsec': 18,
                'sequence': 2,
                'context': 'ext-group',
                'lastapp': 'Dial',
            },
        ])

        analyzer = CallAnalyzer(internal_numbers={'101', '600'}, ring_group_numbers={'600'})
        calls = analyzer.process_dataframe(df)
        result = analyzer.to_dataframe(calls)

        self.assertEqual(len(result), 1)
        self.assertEqual(
            result.iloc[0]['path'],
            'Externe 0123456789 --> Ring group 600 (ANSWERED) --> Extension 101 (ANSWERED)',
        )
        self.assertEqual(
            [(step['number'], step['type'], step['entity_type'], step['disposition'])
             for step in result.iloc[0]['path_details']],
            [
                ('0123456789', 'source', 'external', None),
                ('600', 'ring_group', 'ring_group', 'ANSWERED'),
                ('101', 'group_member_answered', 'extension', 'ANSWERED'),
            ],
        )
