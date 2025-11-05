from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class CallEvent:
    """
    Représente un événement d'appel individuel dans le système Asterisk/FreePBX.

    Contient tous les champs disponibles dans la table CDR pour une analyse complète.
    """
    # Identifiants uniques
    timestamp: datetime  # calldate - Date et heure de l'appel
    uniqueid: str  # Identifiant unique de cet événement
    linkedid: str  # Identifiant de session liant plusieurs événements
    sequence: int  # Numéro de séquence de l'événement

    # Informations d'appel de base
    src: str  # Numéro source (appelant)
    dst: str  # Numéro destination (appelé)
    cnum: str  # Numéro appelant
    channel: str  # Canal source
    dstchannel: str  # Canal destination
    context: str  # Contexte du dialplan

    # État de l'appel
    disposition: str  # Résultat: ANSWERED, NO ANSWER, BUSY, CONGESTION, FAILED
    lastapp: str  # Dernière application exécutée (Dial, Queue, VoiceMail, etc.)

    # Durées
    billsec: int  # Durée facturable en secondes
    duration: Optional[int] = None  # Durée totale en secondes

    # Horodatages détaillés
    start: Optional[datetime] = None  # Heure de début d'appel
    answer: Optional[datetime] = None  # Heure de réponse
    end: Optional[datetime] = None  # Heure de fin

    # Identification de l'appelant
    cnam: Optional[str] = None  # Nom de l'appelant (Caller ID Name)
    clid: Optional[str] = None  # Calling Line ID

    # Routage et facturation
    did: Optional[str] = None  # Direct Inward Dial
    accountcode: Optional[str] = None  # Code compte pour facturation
    peeraccount: Optional[str] = None  # Code compte du canal ponté

    # Flags et données personnalisées
    amaflags: Optional[str] = None  # AMA flags (Automatic Message Accounting)
    userfield: Optional[str] = None  # Champ personnalisé utilisateur

    # Détails de l'application
    lastdata: Optional[str] = None  # Arguments de la dernière application

    def wait_time(self) -> Optional[int]:
        """
        Calcule le temps d'attente avant réponse en secondes.

        Returns:
            Temps d'attente en secondes, ou None si pas applicable
        """
        if self.start and self.answer:
            return int((self.answer - self.start).total_seconds())
        return None

    def is_answered(self) -> bool:
        """Vérifie si l'appel a été répondu."""
        return self.disposition == 'ANSWERED'

    def is_voicemail(self) -> bool:
        """Vérifie si l'appel est allé en messagerie vocale."""
        return 'voicemail' in self.lastapp.lower() if self.lastapp else False

    def is_queue_call(self) -> bool:
        """Vérifie si l'appel provient d'une queue."""
        return 'queue' in self.lastapp.lower() if self.lastapp else False