from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, List
import re

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

    # Identification de l'appelant
    cnam: Optional[str] = None  # Nom de l'appelant (Caller ID Name)
    clid: Optional[str] = None  # Calling Line ID
    outbound_cnum: Optional[str] = None  # Numéro appelant sortant
    outbound_cnam: Optional[str] = None  # Nom appelant sortant
    dst_cnam: Optional[str] = None  # Nom du destinataire

    # Routage et facturation
    did: Optional[str] = None  # Direct Inward Dial
    accountcode: Optional[str] = None  # Code compte pour facturation
    peeraccount: Optional[str] = None  # Code compte du canal ponté

    # Flags et données personnalisées
    amaflags: Optional[str] = None  # AMA flags (Automatic Message Accounting)
    userfield: Optional[str] = None  # Champ personnalisé utilisateur

    # Détails de l'application
    lastdata: Optional[str] = None  # Arguments de la dernière application

    # Enregistrement
    recordingfile: Optional[str] = None  # Fichier d'enregistrement de l'appel

    def wait_time(self) -> Optional[int]:
        """
        Calcule le temps d'attente avant réponse en secondes.

        Note: Sans les champs start/answer, cette méthode retourne None.
        Le calcul doit être fait au niveau du Call avec duration - billsec.

        Returns:
            None (calcul fait au niveau Call)
        """
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

    def is_ivr_call(self) -> bool:
        """
        Vérifie si l'événement est lié à un IVR (menu interactif).

        Détecte les applications IVR courantes dans Asterisk.
        """
        if not self.lastapp:
            return False

        ivr_apps = ['ivr', 'background', 'read', 'waitexten', 'directory',
                    'playback', 'gotoif', 'execif', 'menuselect']
        return any(app in self.lastapp.lower() for app in ivr_apps)

    def get_dtmf_selections(self) -> List[str]:
        """
        Extrait les sélections DTMF (touches pressées) depuis lastdata.

        Returns:
            Liste des touches DTMF pressées
        """
        if not self.lastdata:
            return []

        dtmf_selections = []

        # Pattern pour extraire les chiffres DTMF (0-9, *, #)
        # Recherche dans lastdata pour des patterns comme "exten=1", "digit=5", "input=123"
        patterns = [
            r'exten[=:]([0-9*#]+)',
            r'digit[=:]([0-9*#]+)',
            r'input[=:]([0-9*#]+)',
            r'pressed[=:]([0-9*#]+)',
            r'dtmf[=:]([0-9*#]+)',
            r'choice[=:]([0-9*#]+)',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, self.lastdata, re.IGNORECASE)
            dtmf_selections.extend(matches)

        # Si pas de pattern spécifique, chercher des chiffres isolés dans certains contextes
        if not dtmf_selections and self.is_ivr_call():
            # Pour IVR, les chiffres simples dans lastdata sont souvent des sélections
            simple_digits = re.findall(r'\b([0-9*#])\b', self.lastdata)
            dtmf_selections.extend(simple_digits)

        return dtmf_selections

    def get_queue_name(self) -> Optional[str]:
        """
        Extrait le nom de la queue depuis lastdata.

        Returns:
            Nom de la queue ou None
        """
        if not self.is_queue_call() or not self.lastdata:
            return None

        # Format typique: "queue_name,options,url,announceoverride,timeout"
        parts = self.lastdata.split(',')
        return parts[0] if parts else None

    def get_voicemail_box(self) -> Optional[str]:
        """
        Extrait la boîte vocale depuis lastdata.

        Returns:
            Numéro de boîte vocale ou None
        """
        if not self.is_voicemail() or not self.lastdata:
            return None

        # Format typique: "extension@context,options"
        parts = self.lastdata.split(',')
        if parts:
            mailbox = parts[0].split('@')[0]
            return mailbox
        return None

    def get_application_details(self) -> Dict[str, any]:
        """
        Retourne les détails de l'application exécutée.

        Returns:
            Dictionnaire avec les informations de l'application
        """
        details = {
            'application': self.lastapp,
            'is_ivr': self.is_ivr_call(),
            'is_queue': self.is_queue_call(),
            'is_voicemail': self.is_voicemail(),
            'dtmf_selections': self.get_dtmf_selections(),
        }

        if self.is_queue_call():
            details['queue_name'] = self.get_queue_name()

        if self.is_voicemail():
            details['voicemail_box'] = self.get_voicemail_box()

        if self.lastdata:
            details['parameters'] = self.lastdata

        return details

    def is_conference_call(self) -> bool:
        """Vérifie si l'événement est une conférence."""
        if not self.lastapp:
            return False
        return 'confbridge' in self.lastapp.lower() or 'meetme' in self.lastapp.lower()

    def is_attended_transfer(self) -> bool:
        """Vérifie si c'est un transfert accompagné."""
        return self.context == 'from-internal' and 'Local/' in self.channel

    def get_protocol(self) -> Optional[str]:
        """
        Extrait le protocole utilisé depuis le canal.

        Returns:
            Protocole (PJSIP, SIP, IAX2, Local) ou None
        """
        if not self.channel:
            return None

        protocols = ['PJSIP', 'SIP', 'IAX2', 'Local', 'DAHDI']
        for protocol in protocols:
            if protocol in self.channel:
                return protocol

        return None