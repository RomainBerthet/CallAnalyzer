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

        analyzer = CallAnalyzer(
            internal_numbers={'163'},
            extension_numbers={'163'},
            display_names={'163': 'Standard'},
        )
        calls = analyzer.process_dataframe(df)
        result = analyzer.to_dataframe(calls)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['type_appel'], 'entrant')
        self.assertEqual(result.iloc[0]['renvoi_vers'], '0612345678')
        self.assertEqual(result.iloc[0]['billsec'], 42)
        self.assertEqual(
            result.iloc[0]['path'],
            'Externe 0123456789 --> Extension Standard (163) (ANSWERED) --> Externe 0612345678 (ANSWERED)',
        )
        self.assertEqual(
            [(step['number'], step.get('display'), step['type'], step['disposition']) for step in result.iloc[0]['path_details']],
            [
                ('0123456789', None, 'source', None),
                ('163', 'Standard', 'external', 'ANSWERED'),
                ('0612345678', None, 'forward_answered', 'ANSWERED'),
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

        analyzer = CallAnalyzer(
            internal_numbers={'101', '600'},
            ring_group_numbers={'600'},
            extension_numbers={'101'},
            display_names={'101': 'Alice Martin', '600': 'Support'},
        )
        calls = analyzer.process_dataframe(df)
        result = analyzer.to_dataframe(calls)

        self.assertEqual(len(result), 1)
        self.assertEqual(
            result.iloc[0]['path'],
            'Externe 0123456789 --> Ring group Support (600) (ANSWERED) --> Extension Alice Martin (101) (ANSWERED)',
        )
        self.assertEqual(
            [(step['number'], step.get('display'), step['type'], step['entity_type'], step['disposition'])
             for step in result.iloc[0]['path_details']],
            [
                ('0123456789', None, 'source', 'external', None),
                ('600', 'Support', 'ring_group', 'ring_group', 'ANSWERED'),
                ('101', 'Alice Martin', 'group_member_answered', 'extension', 'ANSWERED'),
            ],
        )

    def test_path_does_not_label_unknown_internal_number_as_extension(self):
        df = pd.DataFrame([
            {
                'calldate': datetime(2026, 5, 12, 12, 0, 0),
                'uniqueid': 'call-3.1',
                'linkedid': 'call-3',
                'src': '101',
                'dst': '777',
                'channel': 'PJSIP/101-00000001',
                'dstchannel': 'PJSIP/777-00000002',
                'disposition': 'NO ANSWER',
                'cnum': '101',
                'billsec': 0,
                'sequence': 1,
                'context': 'from-internal',
                'lastapp': 'Dial',
            },
        ])

        analyzer = CallAnalyzer(internal_numbers={'101', '777'}, extension_numbers={'101'})
        calls = analyzer.process_dataframe(df)
        result = analyzer.to_dataframe(calls)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['path'], 'Extension 101 --> Interne 777 (NO ANSWER)')
        self.assertEqual(
            [(step['number'], step['entity_type']) for step in result.iloc[0]['path_details']],
            [('101', 'extension'), ('777', 'internal_number')],
        )

    def test_followme_path_keeps_forwarding_extension_before_external_target(self):
        df = pd.DataFrame([
            {
                'calldate': datetime(2026, 5, 12, 13, 0, 0),
                'uniqueid': 'call-4.1',
                'linkedid': 'call-4',
                'src': '117',
                'dst': '130',
                'channel': 'PJSIP/117-00000001',
                'dstchannel': 'Local/0666828301@from-internal-00000002;1',
                'disposition': 'ANSWERED',
                'cnum': '117',
                'billsec': 0,
                'sequence': 1,
                'context': 'followme-check',
                'lastapp': 'Dial',
            },
            {
                'calldate': datetime(2026, 5, 12, 13, 0, 1),
                'uniqueid': 'call-4.2',
                'linkedid': 'call-4',
                'src': '130',
                'dst': '0666828301',
                'channel': 'Local/0666828301@from-internal-00000002;1',
                'dstchannel': 'PJSIP/trunk-out-00000003',
                'disposition': 'ANSWERED',
                'cnum': '117',
                'billsec': 35,
                'sequence': 2,
                'context': 'from-internal',
                'lastapp': 'Dial',
            },
        ])

        analyzer = CallAnalyzer(
            internal_numbers={'117', '130'},
            extension_numbers={'117', '130'},
            display_names={'117': 'Baptiste GODARD', '130': 'Thomas CHAPOTIN'},
        )
        calls = analyzer.process_dataframe(df)
        result = analyzer.to_dataframe(calls)

        self.assertEqual(len(result), 1)
        self.assertEqual(
            result.iloc[0]['path'],
            'Extension Baptiste GODARD (117) --> Extension Thomas CHAPOTIN (130) --> Externe 0666828301 (ANSWERED)',
        )
        self.assertEqual(
            [(step['number'], step.get('display'), step['type'], step['entity_type'], step['disposition'])
             for step in result.iloc[0]['path_details']],
            [
                ('117', 'Baptiste GODARD', 'source', 'extension', None),
                ('130', 'Thomas CHAPOTIN', 'forward_source', 'extension', None),
                ('0666828301', None, 'forwarded', 'external', 'ANSWERED'),
            ],
        )

    def test_click_to_call_destination_forward_does_not_mark_destination_extension_answered(self):
        df = pd.DataFrame([
            {
                'calldate': datetime(2026, 5, 12, 14, 0, 0),
                'uniqueid': 'call-5.1',
                'linkedid': 'call-5',
                'src': '117',
                'dst': '130',
                'channel': 'Local/117@from-internal-00000001;2',
                'dstchannel': 'Local/0600000001@from-internal-00000002;1',
                'disposition': 'ANSWERED',
                'cnum': '117',
                'billsec': 0,
                'sequence': 1,
                'context': 'followme-check',
                'lastapp': 'Dial',
                'cnam': 'Répondre pour appeler le 130',
            },
            {
                'calldate': datetime(2026, 5, 12, 14, 0, 1),
                'uniqueid': 'call-5.2',
                'linkedid': 'call-5',
                'src': '117',
                'dst': '0600000001',
                'channel': 'Local/0600000001@from-internal-00000002;1',
                'dstchannel': 'PJSIP/trunk-out-00000003',
                'disposition': 'ANSWERED',
                'cnum': '117',
                'billsec': 8,
                'sequence': 2,
                'context': 'from-internal',
                'lastapp': 'Dial',
                'cnam': 'Répondre pour appeler le 130',
            },
            {
                'calldate': datetime(2026, 5, 12, 14, 0, 2),
                'uniqueid': 'call-5.3',
                'linkedid': 'call-5',
                'src': '117',
                'dst': '130',
                'channel': 'Local/117@from-internal-00000001;1',
                'dstchannel': 'Local/0666828301@from-internal-00000004;1',
                'disposition': 'ANSWERED',
                'cnum': '117',
                'billsec': 0,
                'sequence': 3,
                'context': 'followme-check',
                'lastapp': 'Dial',
                'cnam': 'Répondre pour appeler le 130',
            },
            {
                'calldate': datetime(2026, 5, 12, 14, 0, 3),
                'uniqueid': 'call-5.4',
                'linkedid': 'call-5',
                'src': '130',
                'dst': '0666828301',
                'channel': 'Local/0666828301@from-internal-00000004;1',
                'dstchannel': 'PJSIP/trunk-out-00000005',
                'disposition': 'ANSWERED',
                'cnum': '117',
                'billsec': 25,
                'sequence': 4,
                'context': 'from-internal',
                'lastapp': 'Dial',
                'cnam': 'Répondre pour appeler le 130',
            },
        ])

        analyzer = CallAnalyzer(
            internal_numbers={'117', '130'},
            extension_numbers={'117', '130'},
            display_names={'117': 'Baptiste GODARD', '130': 'Thomas CHAPOTIN'},
        )
        calls = analyzer.process_dataframe(df)
        result = analyzer.to_dataframe(calls)

        self.assertEqual(len(result), 1)
        self.assertEqual(
            result.iloc[0]['path'],
            'Extension Baptiste GODARD (117) --> Externe 0600000001 (ANSWERED) --> Extension Thomas CHAPOTIN (130) --> Externe 0666828301 (ANSWERED)',
        )
        self.assertEqual(
            [(step['number'], step.get('display'), step['type'], step['entity_type'], step['disposition'])
             for step in result.iloc[0]['path_details']],
            [
                ('117', 'Baptiste GODARD', 'source', 'extension', None),
                ('0600000001', None, 'click_to_call_answered', 'external', 'ANSWERED'),
                ('130', 'Thomas CHAPOTIN', 'destination', 'extension', None),
                ('0666828301', None, 'forward_answered', 'external', 'ANSWERED'),
            ],
        )
