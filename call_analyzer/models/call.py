from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

@dataclass
class Call:
    """
    Représente un appel complet agrégé à partir des événements CDR.

    Cette classe consolide tous les événements liés (linkedid) pour fournir
    une vue complète de l'appel avec analyse des transferts, renvois et chemins.
    """
    # Identification
    uniqueid: str  # Identifiant unique de l'appel (linkedid)
    start_time: datetime  # Heure de début de l'appel

    # Participants
    source: str  # Numéro source (appelant)
    destination: str  # Numéro destination (appelé)
    original_caller_name: Optional[str]  # Nom de l'appelant

    # État et classification
    status: str  # ANSWERED, NO ANSWER, BUSY, CONGESTION, FAILED
    type: str  # entrant, sortant, interne
    is_internal: bool  # True si appel interne
    is_click_to_call: bool  # True si Click-to-Call

    # Durées et horodatages
    duration: int  # Durée totale en secondes
    answer_time: Optional[datetime] = None  # Heure de réponse
    end_time: Optional[datetime] = None  # Heure de fin
    wait_time: Optional[int] = None  # Temps d'attente avant réponse en secondes
    ring_time: Optional[int] = None  # Temps de sonnerie en secondes

    # Analyse du chemin d'appel
    final_path: str = ""  # Chemin complet de l'appel (A -> B -> C)
    call_path_details: Optional[List[dict]] = None  # Détails du chemin avec métadonnées

    # Transferts et renvois
    transfers_from: Optional[str] = None  # Numéro source du transfert
    transfers_to: Optional[str] = None  # Numéro cible du transfert
    forwards_from: Optional[str] = None  # Numéro source du renvoi
    forwards_to: Optional[str] = None  # Numéro cible du renvoi
    has_transfer: bool = False  # True si l'appel a été transféré
    has_forward: bool = False  # True si l'appel a été renvoyé

    # Applications spéciales
    went_to_voicemail: bool = False  # True si messagerie vocale
    queue_name: Optional[str] = None  # Nom de la queue si applicable
    queue_wait_time: Optional[int] = None  # Temps d'attente en queue

    # Routage et facturation
    did: Optional[str] = None  # Direct Inward Dial
    accountcode: Optional[str] = None  # Code compte
    peeraccount: Optional[str] = None  # Code compte du pair
    userfield: Optional[str] = None  # Champ personnalisé

    # Métriques calculées
    total_participants: int = 0  # Nombre total de participants
    event_count: int = 0  # Nombre d'événements CDR

    def __post_init__(self):
        """Post-initialisation pour calculer les champs dérivés."""
        self.has_transfer = self.transfers_to is not None
        self.has_forward = self.forwards_to is not None

    def sla_compliant(self, threshold_seconds: int = 20) -> bool:
        """
        Vérifie si l'appel respecte le SLA (Service Level Agreement).

        Args:
            threshold_seconds: Seuil en secondes (défaut: 20s)

        Returns:
            True si l'appel a été répondu dans le seuil
        """
        if self.status != 'ANSWERED':
            return False
        if self.wait_time is None:
            return True  # Considéré conforme si pas de wait_time
        return self.wait_time <= threshold_seconds

    def is_missed(self) -> bool:
        """Vérifie si l'appel est manqué."""
        return self.type == 'entrant' and self.status != 'ANSWERED'

    def is_successful_outbound(self) -> bool:
        """Vérifie si c'est un appel sortant abouti."""
        return self.type == 'sortant' and self.status == 'ANSWERED'

    def duration_formatted(self) -> str:
        """
        Retourne la durée au format HH:MM:SS.

        Returns:
            Durée formatée
        """
        hours, remainder = divmod(self.duration, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"