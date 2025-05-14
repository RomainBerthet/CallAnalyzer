from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class CallEvent:
    """Représente un événement d'appel individuel dans le système Asterisk."""
    timestamp: datetime
    uniqueid: str
    linkedid: str
    src: str
    dst: str
    channel: str
    dstchannel: str
    disposition: str
    cnum: str
    billsec: int
    sequence: int
    context: str
    lastapp: str
    cnam: Optional[str] = None
    did: Optional[str] = None
    accountcode: Optional[str] = None
    userfield: Optional[str] = None
    amaflags: Optional[str] = None
    duration: Optional[int] = None
    clid: Optional[str] = None