from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict

@dataclass
class CallSegment:
    """
    Représente un segment d'un chemin d'appel avec timing détaillé.
    """
    segment_number: int  # Numéro du segment dans le chemin
    from_party: str  # Qui initie ce segment
    to_party: str  # Qui reçoit ce segment
    application: Optional[str] = None  # Application Asterisk utilisée
    context: str = ""  # Contexte du dialplan
    protocol: Optional[str] = None  # Protocole (PJSIP, SIP, IAX2, Local)

    # Timing
    start_time: Optional[datetime] = None
    answer_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    ring_duration: int = 0  # Temps de sonnerie en secondes
    talk_duration: int = 0  # Temps de conversation en secondes

    # Résultat
    disposition: str = ""  # ANSWERED, NO ANSWER, BUSY, etc.
    answered: bool = False

    # Détails IVR/DTMF
    dtmf_selections: List[str] = field(default_factory=list)  # Touches pressées
    ivr_path: str = ""  # Chemin IVR suivi

    # Détails queue
    queue_name: Optional[str] = None
    queue_position: Optional[int] = None

    def get_total_duration(self) -> int:
        """Durée totale du segment."""
        return self.ring_duration + self.talk_duration

    def __str__(self) -> str:
        """Représentation textuelle du segment."""
        result = f"{self.from_party} → {self.to_party}"
        if self.application:
            result += f" [{self.application}]"
        if self.answered:
            result += f" (ANSWERED, {self.talk_duration}s)"
        else:
            result += f" ({self.disposition})"
        if self.dtmf_selections:
            result += f" DTMF:{','.join(self.dtmf_selections)}"
        return result

@dataclass
class Call:
    """
    Représente un appel complet agrégé à partir des événements CDR.

    Cette classe consolide tous les événements liés (linkedid) pour fournir
    une vue complète de l'appel avec analyse détaillée des transferts, renvois,
    IVR, RingGroups et chemins d'appel avec durées précises.
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

    # Analyse du chemin d'appel DÉTAILLÉE
    final_path: str = ""  # Chemin complet simple (A -> B -> C)
    call_segments: List[CallSegment] = field(default_factory=list)  # Segments détaillés avec durées
    call_path_details: Optional[List[dict]] = None  # Détails legacy du chemin

    # Qui a vraiment répondu
    answering_party: Optional[str] = None  # Extension/numéro qui a répondu
    answering_party_name: Optional[str] = None  # Nom de la personne qui a répondu

    # Transferts et renvois
    transfers_from: Optional[str] = None  # Numéro source du transfert
    transfers_to: Optional[str] = None  # Numéro cible du transfert
    forwards_from: Optional[str] = None  # Numéro source du renvoi
    forwards_to: Optional[str] = None  # Numéro cible du renvoi
    has_transfer: bool = False  # True si l'appel a été transféré
    has_forward: bool = False  # True si l'appel a été renvoyé

    # Applications spéciales
    went_to_voicemail: bool = False  # True si messagerie vocale
    voicemail_box: Optional[str] = None  # Numéro de la boîte vocale

    queue_name: Optional[str] = None  # Nom de la queue si applicable
    queue_wait_time: Optional[int] = None  # Temps d'attente en queue

    # IVR (Interactive Voice Response)
    has_ivr: bool = False  # True si passage par IVR
    ivr_selections: Dict[str, List[str]] = field(default_factory=dict)  # Sélections IVR par étape
    ivr_path: str = ""  # Chemin complet dans l'IVR (ex: "1 -> 2 -> 3")

    # RingGroups détaillés
    ringgroup_used: Optional[str] = None  # Numéro du RingGroup utilisé
    ringgroup_members_tried: List[str] = field(default_factory=list)  # Membres contactés
    ringgroup_answerer: Optional[str] = None  # Membre qui a répondu
    ringgroup_ring_duration: Optional[int] = None  # Temps de sonnerie RingGroup

    # Conférence
    is_conference: bool = False  # True si conférence téléphonique
    conference_id: Optional[str] = None  # ID de la conférence

    # Routage et facturation
    did: Optional[str] = None  # Direct Inward Dial
    accountcode: Optional[str] = None  # Code compte
    peeraccount: Optional[str] = None  # Code compte du pair
    userfield: Optional[str] = None  # Champ personnalisé

    # Métriques calculées
    total_participants: int = 0  # Nombre total de participants
    event_count: int = 0  # Nombre d'événements CDR
    segments_count: int = 0  # Nombre de segments dans le chemin

    def __post_init__(self):
        """Post-initialisation pour calculer les champs dérivés."""
        self.has_transfer = self.transfers_to is not None
        self.has_forward = self.forwards_to is not None
        self.segments_count = len(self.call_segments)

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

    def get_detailed_path_string(self) -> str:
        """
        Retourne une représentation détaillée du chemin avec durées.

        Returns:
            Chemin formaté avec durées
        """
        if not self.call_segments:
            return self.final_path

        path_parts = []
        for segment in self.call_segments:
            part = f"{segment.from_party}→{segment.to_party}"
            if segment.answered:
                part += f"({segment.talk_duration}s)"
            else:
                part += f"(NO ANS)"
            if segment.dtmf_selections:
                part += f"[DTMF:{','.join(segment.dtmf_selections)}]"
            path_parts.append(part)

        return " ⇒ ".join(path_parts)

    def get_ivr_summary(self) -> str:
        """
        Retourne un résumé des interactions IVR.

        Returns:
            Résumé IVR formaté
        """
        if not self.has_ivr or not self.ivr_selections:
            return ""

        selections = []
        for step, choices in self.ivr_selections.items():
            selections.append(f"{step}:{','.join(choices)}")

        return " → ".join(selections)

    def get_ringgroup_summary(self) -> str:
        """
        Retourne un résumé de l'activité RingGroup.

        Returns:
            Résumé RingGroup formaté
        """
        if not self.ringgroup_used:
            return ""

        summary = f"RG{self.ringgroup_used}"
        if self.ringgroup_members_tried:
            summary += f" tried:[{','.join(self.ringgroup_members_tried)}]"
        if self.ringgroup_answerer:
            summary += f" answered:{self.ringgroup_answerer}"
        if self.ringgroup_ring_duration:
            summary += f" ({self.ringgroup_ring_duration}s)"

        return summary