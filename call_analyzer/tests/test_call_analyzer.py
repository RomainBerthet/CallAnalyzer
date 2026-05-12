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
