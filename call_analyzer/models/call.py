from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Call:
    """Représente un appel complet agrégé à partir des événements."""
    start_time: datetime
    uniqueid: str
    source: str
    destination: str
    duration: int
    status: str
    type: str
    is_internal: bool
    is_click_to_call: bool
    final_path: str
    original_caller_name: Optional[str]
    transfers_from: Optional[str] = None
    transfers_to: Optional[str] = None
    forwards_from: Optional[str] = None
    forwards_to: Optional[str] = None
    did: Optional[str] = None
    accountcode: Optional[str] = None
    userfield: Optional[str] = None