import logging
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

logger = logging.getLogger('rc_global.telephonie')

# Imported lazily to avoid circular imports at module level
_RESERVED_NUMBERS: Optional[Set[str]] = {'15', '17', '18', '112', '114', '115', '196', '191', '197', '3919', '116000', '119', '3977', '116117'}


def _get_reserved_numbers() -> Set[str]:
    global _RESERVED_NUMBERS
    return _RESERVED_NUMBERS


class BillingClassifier:
    """
    Classifies analyzed calls (output of CallAnalyzer) for telephony billing.

    Billing scope:
      - Outgoing calls (type_appel == 'sortant'), answered or not
      - Calls with an external renvoi (renvoi_vers is an external number), answered or not

    All DB lookups (SVA, TarifsInternationaux) are pre-loaded once via load_db_caches()
    to avoid per-row N+1 queries.
    """

    _MOBILE_PREFIXES = ('06', '07', '00336', '00337', '+336', '+337', '006', '007')
    _FIXED_PREFIXES = (
        '01', '02', '03', '04', '05', '09',
        '+331', '+332', '+333', '+334', '+335', '+339',
        '001', '002', '003', '004', '005', '009',
        '331', '332', '333', '334', '335', '339',
    )
    _FREE_SVA_PREFIXES = ('0800', '0801', '0802', '0803', '0804', '0805')
    _COMM_COST_SVA_PREFIXES = ('0806', '0807', '0808', '0809')

    def __init__(self, internal_numbers: Set[str], out_with_zero: bool = False):
        self.internal_numbers = internal_numbers
        self.out_with_zero = out_with_zero
        self._sva_cache: Dict[str, object] = {}
        # Sorted by indicatif length descending for longest-prefix matching
        self._tarifs_int: List[object] = []

    def load_db_caches(self, sva_qs, tarifs_int_qs) -> None:
        """Pre-load SVA and TarifsInternationaux querysets to avoid per-row DB hits."""
        self._sva_cache = {str(sva.numero): sva for sva in sva_qs}
        self._tarifs_int = sorted(list(tarifs_int_qs), key=lambda t: len(str(t.indicatif)), reverse=True)

    def _is_external(self, number: str) -> bool:
        return str(number) not in self.internal_numbers

    def _normalize_number(self, number: str) -> str:
        """
        Normalizes a phone number before classification.
        Mirrors the existing logic: strip one leading '0' if out_with_zero,
        then strip one leading '00' (international dialing prefix).
        """
        number = str(number).strip().replace(' ', '')
        if self.out_with_zero:
            number = number.removeprefix('0')
        if number.startswith('00'):
            number = number.removeprefix('00')
        return number

    def _classify_number(self, raw_number: str) -> Optional[Tuple[str, Optional[str]]]:
        """
        Returns (category, key) or None if the number is unrecognized.

        Categories:
          urgence              → key=None
          mobile               → key=None
          fixe                 → key=None
          sva_gratuit          → key=None
          sva_cout_communication → key=None
          sva_payant           → key=numero
          international        → key='pays__pk'
        """
        number = self._normalize_number(raw_number)
        if not number:
            return None

        reserved = _get_reserved_numbers()
        if number in reserved:
            return ('urgence', None)

        if any(number.startswith(p) for p in self._MOBILE_PREFIXES):
            return ('mobile', None)

        if any(number.startswith(p) for p in self._FIXED_PREFIXES):
            return ('fixe', None)

        if any(number.startswith(p) for p in self._FREE_SVA_PREFIXES):
            return ('sva_gratuit', None)
        if len(number) == 4 and (number.startswith('30') or number.startswith('31')):
            return ('sva_gratuit', None)

        if any(number.startswith(p) for p in self._COMM_COST_SVA_PREFIXES):
            return ('sva_cout_communication', None)

        clean = number.removeprefix('00')

        if clean in self._sva_cache:
            sva = self._sva_cache[clean]
            if not sva.a_viser:
                return ('sva_payant', clean)

        for tarif in self._tarifs_int:
            if clean.startswith(str(tarif.indicatif)):
                return ('international', f"{tarif.pays}__{tarif.pk}")

        return None

    def classify_calls_for_billing(
        self,
        calls_df: pd.DataFrame,
        params: dict,
        traceback: list,
    ) -> Tuple[dict, list, pd.DataFrame, List[str]]:
        """
        Filters and classifies analyzed calls for billing.

        Billing scope:
          - type_appel == 'sortant' → destination is dst
          - renvoi_vers is not None and external → destination is renvoi_vers

        Updates params in-place with call counts and durations per category.
        Returns (params, traceback, billing_df, unrecognized_numbers).
        """
        unrecognized: List[str] = []

        if calls_df.empty:
            return params, traceback, pd.DataFrame(), unrecognized

        is_sortant = calls_df['type_appel'] == 'sortant'
        has_renvoi_externe = (
            calls_df['renvoi_vers'].notna()
            & calls_df['renvoi_vers'].apply(
                lambda x: bool(x) and self._is_external(str(x)) if pd.notna(x) else False
            )
        )
        billing_mask = is_sortant | has_renvoi_externe
        billing_df = calls_df[billing_mask].copy()

        if billing_df.empty:
            return params, traceback, pd.DataFrame(), unrecognized

        # For renvoi externe calls the billable destination is renvoi_vers, otherwise dst
        billing_df['billing_dst'] = billing_df.apply(
            lambda row: (
                str(row['renvoi_vers'])
                if row['type_appel'] != 'sortant' and pd.notna(row['renvoi_vers'])
                else str(row['dst'])
            ),
            axis=1,
        )

        rows_to_drop = []
        for idx, row in billing_df.iterrows():
            billsec = int(row['billsec'])
            raw_dst = row['billing_dst']
            result = self._classify_number(raw_dst)

            if result is None:
                unrecognized.append(raw_dst)
                rows_to_drop.append(idx)
                traceback.append(
                    f"{raw_dst} n'est pas facturé car il n'est reconnu ni comme un numéro mobile, "
                    f"ni comme un numéro fixe, ni comme un numéro SVA, ni comme un numéro international"
                )
                logger.warning(f"{raw_dst} non facturé.")
                continue

            cat, key = result

            if cat == 'urgence':
                params['appels_numero_urgence'] += billsec
                params['nb_appels_numero_urgence'] += 1
            elif cat == 'mobile':
                params['appels_mobiles_nationaux'] += billsec
                params['nb_appels_mobiles_nationaux'] += 1
            elif cat == 'fixe':
                params['appels_fixes_nationaux'] += billsec
                params['nb_appels_fixes_nationaux'] += 1
            elif cat == 'sva_gratuit':
                params['appels_sva_gratuits'] += billsec
                params['nb_appels_sva_gratuits'] += 1
            elif cat == 'sva_cout_communication':
                params['appels_sva_cout_communication'] += billsec
                params['nb_appels_sva_cout_communication'] += 1
            elif cat == 'sva_payant':
                store_key = f"{key}__{idx}"
                params['appels_sva_payants'][store_key] = (
                    int(params['appels_sva_payants'].get(store_key, 0)) + billsec
                )
            elif cat == 'international':
                entry = params['appels_internationaux'].setdefault(key, {'duree': 0, 'nb_appels': 0})
                entry['duree'] += billsec
                entry['nb_appels'] += 1

        if rows_to_drop:
            billing_df = billing_df.drop(rows_to_drop)

        out_df = billing_df[['call_date', 'end_date', 'billsec', 'type_appel', 'status', 'src', 'billing_dst', 'path']].copy()
        out_df.columns = ['date_debut', 'date_fin', 'duree', 'type', 'statut', 'appelant', 'destination', 'chemin']

        return params, traceback, out_df, unrecognized

    @staticmethod
    def compute_max_simultaneous(calls_df: pd.DataFrame) -> int:
        """
        Computes the maximum number of simultaneous trunk calls.
        Only answered calls (billsec > 0) of type sortant or entrant are counted,
        since those are the calls that actually occupied an IP channel.
        """
        if calls_df.empty:
            return 1

        trunk_calls = calls_df[
            calls_df['type_appel'].isin(['sortant', 'entrant']) & (calls_df['billsec'] > 0)
        ]
        if trunk_calls.empty:
            return 1

        starts = pd.DataFrame({'time': trunk_calls['call_date'].values, 'delta': 1})
        ends = pd.DataFrame({'time': trunk_calls['end_date'].values, 'delta': -1})

        events = pd.concat([starts, ends], ignore_index=True).sort_values('time')
        max_sim = events['delta'].cumsum().max()

        if pd.isna(max_sim):
            return 1
        return max(1, int(max_sim))
