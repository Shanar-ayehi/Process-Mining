"""
External Cards Service - Servizio per gestire card esterne in HubSpot.

Questo modulo fornisce funzionalità per creare, gestire e sincronizzare
card esterne che mostrano dati Process Mining direttamente nell'interfaccia HubSpot.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
from enum import Enum
import json
import hashlib

from pydantic import BaseModel, Field, validator

from app.core.logger import get_logger
from app.connectors.hubspot_client import HubSpotClient
from app.services.data_service import data_repository

logger = get_logger()


class CardType(str, Enum):
    """Tipi di card supportati."""
    DEAL = "deal"
    CONTACT = "contact"
    COMPANY = "company"


class SyncStatus(str, Enum):
    """Stati di sincronizzazione."""
    SUCCESS = "success"
    ERROR = "error"
    PENDING = "pending"
    STALE = "stale"


class ExternalCardConfig(BaseModel):
    """Configurazione di una card esterna."""
    card_id: str = Field(..., description="ID univoco della card")
    name: str = Field(..., description="Nome visualizzato della card")
    card_type: CardType = Field(..., description="Tipo di card")
    hubspot_object_type: str = Field(..., description="Tipo oggetto HubSpot (contacts, deals, companies)")
    properties_to_display: List[str] = Field(..., description="Proprietà da visualizzare")
    refresh_interval_minutes: int = Field(default=60, ge=5, le=1440, description="Intervallo refresh in minuti")
    is_active: bool = Field(default=True, description="Se la card è attiva")
    webhook_url: Optional[str] = Field(default=None, description="URL webhook per aggiornamenti")
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    
    @validator('card_id')
    def validate_card_id(cls, v):
        if not v or len(v) < 3:
            raise ValueError('card_id deve essere almeno 3 caratteri')
        return v
    
    @validator('hubspot_object_type')
    def validate_hubspot_object_type(cls, v):
        valid_types = ['contacts', 'deals', 'companies']
        if v not in valid_types:
            raise ValueError(f'hubspot_object_type deve essere uno di: {valid_types}')
        return v


class ExternalCardData(BaseModel):
    """Dati di una card esterna per un oggetto specifico."""
    card_id: str = Field(..., description="ID della card")
    object_id: str = Field(..., description="ID oggetto HubSpot")
    data: Dict[str, Any] = Field(..., description="Dati da visualizzare")
    last_sync: datetime = Field(default_factory=datetime.now)
    sync_status: SyncStatus = Field(default=SyncStatus.SUCCESS)
    error_message: Optional[str] = Field(default=None)
    data_hash: Optional[str] = Field(default=None, description="Hash per rilevare cambiamenti")


class ExternalCardService:
    """Servizio per gestire card esterne in HubSpot."""
    
    def __init__(self, hubspot_client: Optional[HubSpotClient] = None):
        """
        Inizializza il servizio.
        
        Args:
            hubspot_client: Client HubSpot opzionale
        """
        self.hubspot = hubspot_client
        self.repository = data_repository
        
        # Configurazione cache
        self.cache_ttl_seconds = 3600  # 1 ora default
        self.max_cache_size = 1000
        
    async def create_card(self, config: ExternalCardConfig) -> bool:
        """
        Crea una nuova card esterna.
        
        Args:
            config: Configurazione della card
            
        Returns:
            bool: True se creazione riuscita
        """
        try:
            logger.info(f"Creazione card esterna: {config.card_id}")
            
            # Valida configurazione
            if not self._validate_card_config(config):
                logger.error(f"Configurazione card non valida: {config.card_id}")
                return False
            
            # Salva configurazione
            config_dict = config.dict()
            config_dict['created_at'] = config.created_at.isoformat()
            config_dict['updated_at'] = config.updated_at.isoformat()
            
            success = await self.repository.save_external_card_config(config.card_id, config_dict)
            
            if success:
                logger.info(f"Card esterna creata con successo: {config.card_id}")
                return True
            else:
                logger.error(f"Errore salvataggio card: {config.card_id}")
                return False
                
        except Exception as e:
            logger.error(f"Errore nella creazione card {config.card_id}: {e}")
            return False
    
    async def get_card_data(self, card_id: str, object_id: str) -> Optional[ExternalCardData]:
        """
        Recupera dati per una card specifica.
        
        Args:
            card_id: ID della card
            object_id: ID oggetto HubSpot
            
        Returns:
            ExternalCardData o None se non trovato
        """
        try:
            logger.info(f"Recupero dati card {card_id} per oggetto {object_id}")
            
            # 1. Prova a recuperare da cache
            cached_data = await self._get_from_cache(card_id, object_id)
            if cached_data:
                logger.debug(f"Dati recuperati da cache per {card_id}:{object_id}")
                return cached_data
            
            # 2. Se non in cache o scaduto, carica da HubSpot
            config = await self.get_card_config(card_id)
            if not config:
                logger.error(f"Configurazione card non trovata: {card_id}")
                return None
            
            # Recupera dati reali da HubSpot
            hubspot_data = await self._fetch_hubspot_data(config, object_id)
            
            if hubspot_data is None:
                logger.warning(f"Nessun dato recuperato da HubSpot per {card_id}:{object_id}")
                return ExternalCardData(
                    card_id=card_id,
                    object_id=object_id,
                    data={},
                    sync_status=SyncStatus.ERROR,
                    error_message="No data available from HubSpot"
                )
            
            # Crea oggetto dati
            card_data = ExternalCardData(
                card_id=card_id,
                object_id=object_id,
                data=hubspot_data,
                sync_status=SyncStatus.SUCCESS,
                data_hash=self._calculate_data_hash(hubspot_data)
            )
            
            # 3. Salva in cache
            await self._save_to_cache(card_id, object_id, card_data)
            
            logger.info(f"Dati card recuperati con successo: {card_id}:{object_id}")
            return card_data
            
        except Exception as e:
            logger.error(f"Errore nel recupero dati card {card_id}:{object_id}: {e}")
            return ExternalCardData(
                card_id=card_id,
                object_id=object_id,
                data={},
                sync_status=SyncStatus.ERROR,
                error_message=str(e)
            )
    
    async def get_card_config(self, card_id: str) -> Optional[ExternalCardConfig]:
        """
        Recupera configurazione di una card.
        
        Args:
            card_id: ID della card
            
        Returns:
            ExternalCardConfig o None se non trovato
        """
        try:
            config_dict = await self.repository.get_external_card_config(card_id)
            if not config_dict:
                return None
            
            # Converti stringhe ISO in datetime
            if 'created_at' in config_dict:
                config_dict['created_at'] = datetime.fromisoformat(config_dict['created_at'])
            if 'updated_at' in config_dict:
                config_dict['updated_at'] = datetime.fromisoformat(config_dict['updated_at'])
            
            return ExternalCardConfig(**config_dict)
            
        except Exception as e:
            logger.error(f"Errore nel recupero configurazione card {card_id}: {e}")
            return None
    
    async def list_cards(self, card_type: Optional[CardType] = None) -> List[ExternalCardConfig]:
        """
        Lista tutte le card configurate.
        
        Args:
            card_type: Filtro per tipo di card (opzionale)
            
        Returns:
            Lista di configurazioni card
        """
        try:
            all_configs = await self.repository.list_external_card_configs()
            
            configs = []
            for config_dict in all_configs:
                try:
                    # Converti datetime
                    if 'created_at' in config_dict:
                        config_dict['created_at'] = datetime.fromisoformat(config_dict['created_at'])
                    if 'updated_at' in config_dict:
                        config_dict['updated_at'] = datetime.fromisoformat(config_dict['updated_at'])
                    
                    config = ExternalCardConfig(**config_dict)
                    
                    # Applica filtro tipo
                    if card_type is None or config.card_type == card_type:
                        configs.append(config)
                        
                except Exception as e:
                    logger.warning(f"Errore parsing configurazione card: {e}")
                    continue
            
            logger.info(f"Recuperate {len(configs)} card configurate")
            return configs
            
        except Exception as e:
            logger.error(f"Errore nel recupero lista card: {e}")
            return []
    
    async def sync_card(self, card_id: str) -> bool:
        """
        Forza sincronizzazione dati card.
        
        Args:
            card_id: ID della card da sincronizzare
            
        Returns:
            bool: True se sincronizzazione riuscita
        """
        try:
            logger.info(f"Sincronizzazione forzata card: {card_id}")
            
            config = await self.get_card_config(card_id)
            if not config:
                logger.error(f"Configurazione card non trovata: {card_id}")
                return False
            
            # Invalida cache per questa card
            await self._invalidate_cache(card_id)
            
            # Recupera dati aggiornati per tutti gli oggetti collegati
            # (implementazione dipende dalla struttura dati)
            
            logger.info(f"Sincronizzazione card completata: {card_id}")
            return True
            
        except Exception as e:
            logger.error(f"Errore nella sincronizzazione card {card_id}: {e}")
            return False
    
    async def delete_card(self, card_id: str) -> bool:
        """
        Elimina una card.
        
        Args:
            card_id: ID della card da eliminare
            
        Returns:
            bool: True se eliminazione riuscita
        """
        try:
            logger.info(f"Eliminazione card: {card_id}")
            
            # Invalida cache
            await self._invalidate_cache(card_id)
            
            # Elimina configurazione
            success = await self.repository.delete_external_card_config(card_id)
            
            if success:
                logger.info(f"Card eliminata con successo: {card_id}")
                return True
            else:
                logger.error(f"Errore eliminazione card: {card_id}")
                return False
                
        except Exception as e:
            logger.error(f"Errore nell'eliminazione card {card_id}: {e}")
            return False
    
    async def get_card_dashboard(self, card_id: str, object_id: str) -> Optional[Dict[str, Any]]:
        """
        Recupera dashboard completo per una card e oggetto.
        
        Args:
            card_id: ID della card
            object_id: ID oggetto HubSpot
            
        Returns:
            Dict con dati dashboard completo
        """
        try:
            logger.info(f"Recupero dashboard card {card_id} per oggetto {object_id}")
            
            # Recupera configurazione card
            config = await self.get_card_config(card_id)
            if not config:
                logger.error(f"Configurazione card non trovata: {card_id}")
                return None
            
            # Recupera dati oggetto principale
            object_data = await self._fetch_hubspot_data(config, object_id)
            if not object_data:
                logger.warning(f"Nessun dato recuperato per {card_id}:{object_id}")
                return None
            
            # Recupera associazioni
            associations = await self._fetch_associations(config, object_id)
            
            # Recupera timeline
            timeline = await self._fetch_timeline(config, object_id)
            
            # Costruisci dashboard
            dashboard = {
                "card_id": card_id,
                "object_id": object_id,
                "object_type": config.hubspot_object_type,
                "object_data": object_data,
                "associations": associations,
                "timeline": timeline,
                "metadata": {
                    "retrieved_at": datetime.now().isoformat(),
                    "card_name": config.name,
                    "refresh_interval": config.refresh_interval_minutes
                }
            }
            
            logger.info(f"Dashboard recuperato con successo: {card_id}:{object_id}")
            return dashboard
            
        except Exception as e:
            logger.error(f"Errore nel recupero dashboard {card_id}:{object_id}: {e}")
            return None
    
    async def get_card_analytics(self, card_id: str, object_id: str) -> Optional[Dict[str, Any]]:
        """
        Recupera analisi Process Mining per una card.
        
        Args:
            card_id: ID della card
            object_id: ID oggetto HubSpot
            
        Returns:
            Dict con analisi Process Mining
        """
        try:
            logger.info(f"Recupero analisi card {card_id} per oggetto {object_id}")
            
            # Recupera configurazione card
            config = await self.get_card_config(card_id)
            if not config:
                logger.error(f"Configurazione card non trovata: {card_id}")
                return None
            
            # Recupera dati oggetto
            object_data = await self._fetch_hubspot_data(config, object_id)
            if not object_data:
                return None
            
            # Calcola analisi in base al tipo di oggetto
            analytics = {}
            
            if config.hubspot_object_type == "deals":
                # Analisi per deal
                analytics = await self._analyze_deal(object_id, object_data)
            elif config.hubspot_object_type == "contacts":
                # Analisi per contatto
                analytics = await self._analyze_contact(object_id, object_data)
            elif config.hubspot_object_type == "companies":
                # Analisi per azienda
                analytics = await self._analyze_company(object_id, object_data)
            
            # Aggiungi metadata
            analytics["metadata"] = {
                "card_id": card_id,
                "object_id": object_id,
                "analyzed_at": datetime.now().isoformat()
            }
            
            logger.info(f"Analisi completata: {card_id}:{object_id}")
            return analytics
            
        except Exception as e:
            logger.error(f"Errore nell'analisi card {card_id}:{object_id}: {e}")
            return None
    
    async def get_card_associations(self, card_id: str, object_id: str) -> Optional[Dict[str, Any]]:
        """
        Recupera associazioni per una card.
        
        Args:
            card_id: ID della card
            object_id: ID oggetto HubSpot
            
        Returns:
            Dict con associazioni
        """
        try:
            logger.info(f"Recupero associazioni card {card_id} per oggetto {object_id}")
            
            # Recupera configurazione card
            config = await self.get_card_config(card_id)
            if not config:
                logger.error(f"Configurazione card non trovata: {card_id}")
                return None
            
            # Recupera associazioni
            associations = await self._fetch_associations(config, object_id)
            
            return {
                "card_id": card_id,
                "object_id": object_id,
                "object_type": config.hubspot_object_type,
                "associations": associations,
                "retrieved_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Errore nel recupero associazioni {card_id}:{object_id}: {e}")
            return None
    
    async def get_card_timeline(self, card_id: str, object_id: str) -> Optional[Dict[str, Any]]:
        """
        Recupera timeline per una card.
        
        Args:
            card_id: ID della card
            object_id: ID oggetto HubSpot
            
        Returns:
            Dict con timeline eventi
        """
        try:
            logger.info(f"Recupero timeline card {card_id} per oggetto {object_id}")
            
            # Recupera configurazione card
            config = await self.get_card_config(card_id)
            if not config:
                logger.error(f"Configurazione card non trovata: {card_id}")
                return None
            
            # Recupera timeline
            timeline = await self._fetch_timeline(config, object_id)
            
            return {
                "card_id": card_id,
                "object_id": object_id,
                "object_type": config.hubspot_object_type,
                "timeline": timeline,
                "retrieved_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Errore nel recupero timeline {card_id}:{object_id}: {e}")
            return None
    
    async def _fetch_associations(self, config: ExternalCardConfig, object_id: str) -> Dict[str, Any]:
        """
        Recupera associazioni da HubSpot.
        
        Args:
            config: Configurazione card
            object_id: ID oggetto
            
        Returns:
            Dict con associazioni
        """
        try:
            if not self.hubspot:
                logger.warning("Client HubSpot non configurato")
                return {}
            
            associations = {}
            
            if config.hubspot_object_type == "deals":
                # Per i deal, recupera contatti e aziende
                deal_assocs = await self.hubspot.get_deal_associations(object_id)
                associations = deal_assocs
            elif config.hubspot_object_type == "contacts":
                # Per i contatti, recupera deal e aziende
                contact_assocs = await self.hubspot.get_associations("contacts", object_id, "deals")
                associations["deals"] = contact_assocs
                
                company_assocs = await self.hubspot.get_associations("contacts", object_id, "companies")
                associations["companies"] = company_assocs
            elif config.hubspot_object_type == "companies":
                # Per le aziende, recupera deal e contatti
                deal_assocs = await self.hubspot.get_associations("companies", object_id, "deals")
                associations["deals"] = deal_assocs
                
                contact_assocs = await self.hubspot.get_associations("companies", object_id, "contacts")
                associations["contacts"] = contact_assocs
            
            return associations
            
        except Exception as e:
            logger.error(f"Errore nel recupero associazioni: {e}")
            return {}
    
    async def _fetch_timeline(self, config: ExternalCardConfig, object_id: str) -> List[Dict[str, Any]]:
        """
        Recupera timeline da HubSpot.
        
        Args:
            config: Configurazione card
            object_id: ID oggetto
            
        Returns:
            Lista di eventi timeline
        """
        try:
            if not self.hubspot:
                logger.warning("Client HubSpot non configurato")
                return []
            
            timeline = []
            
            if config.hubspot_object_type == "deals":
                # Per i deal, usa get_deal_timeline
                timeline = await self.hubspot.get_deal_timeline(object_id)
            else:
                # Per altri tipi, usa get_timeline_events
                timeline = await self.hubspot.get_timeline_events(
                    config.hubspot_object_type, 
                    object_id
                )
            
            return timeline
            
        except Exception as e:
            logger.error(f"Errore nel recupero timeline: {e}")
            return []
    
    async def _analyze_deal(self, deal_id: str, deal_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analizza un deal per Process Mining.
        
        Args:
            deal_id: ID del deal
            deal_data: Dati del deal
            
        Returns:
            Dict con analisi
        """
        try:
            # Recupera timeline per analisi
            timeline = await self.hubspot.get_deal_timeline(deal_id) if self.hubspot else []
            
            # Calcola metriche base
            properties = deal_data.get("properties", {})
            
            analysis = {
                "deal_id": deal_id,
                "deal_name": properties.get("dealname", "Unknown"),
                "amount": properties.get("amount", 0),
                "stage": properties.get("dealstage", "Unknown"),
                "pipeline": properties.get("pipeline", "Unknown"),
                "created_at": properties.get("createdate"),
                "closed_at": properties.get("closedate"),
                "owner": properties.get("hubspot_owner_id"),
                "timeline_events_count": len(timeline),
                "last_activity": self._get_last_activity_date(timeline),
                "stage_duration_days": self._calculate_stage_duration(timeline),
                "engagement_score": self._calculate_engagement_score(timeline)
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"Errore nell'analisi deal {deal_id}: {e}")
            return {"error": str(e)}
    
    async def _analyze_contact(self, contact_id: str, contact_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analizza un contatto.
        
        Args:
            contact_id: ID del contatto
            contact_data: Dati del contatto
            
        Returns:
            Dict con analisi
        """
        try:
            properties = contact_data.get("properties", {})
            
            analysis = {
                "contact_id": contact_id,
                "name": f"{properties.get('firstname', '')} {properties.get('lastname', '')}".strip(),
                "email": properties.get("email"),
                "company": properties.get("company"),
                "created_at": properties.get("createdate"),
                "last_activity": properties.get("lastmodifieddate"),
                "lifecycle_stage": properties.get("lifecyclestage"),
                "lead_status": properties.get("hs_lead_status")
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"Errore nell'analisi contatto {contact_id}: {e}")
            return {"error": str(e)}
    
    async def _analyze_company(self, company_id: str, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analizza un'azienda.
        
        Args:
            company_id: ID dell'azienda
            company_data: Dati dell'azienda
            
        Returns:
            Dict con analisi
        """
        try:
            properties = company_data.get("properties", {})
            
            analysis = {
                "company_id": company_id,
                "name": properties.get("name", "Unknown"),
                "domain": properties.get("domain"),
                "industry": properties.get("industry"),
                "created_at": properties.get("createdate"),
                "last_activity": properties.get("hs_lastmodifieddate"),
                "city": properties.get("city"),
                "country": properties.get("country")
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"Errore nell'analisi azienda {company_id}: {e}")
            return {"error": str(e)}
    
    def _get_last_activity_date(self, timeline: List[Dict]) -> Optional[str]:
        """Recupera data ultima attività dalla timeline."""
        if not timeline:
            return None
        
        # Cerca il timestamp più recente
        latest = None
        for event in timeline:
            timestamp = event.get("timestamp") or event.get("createdAt")
            if timestamp:
                if latest is None or timestamp > latest:
                    latest = timestamp
        
        return latest
    
    def _calculate_stage_duration(self, timeline: List[Dict]) -> Optional[int]:
        """Calcola durata in giorni nella stage corrente."""
        # Implementazione base - da estendere con logica specifica
        return None
    
    def _calculate_engagement_score(self, timeline: List[Dict]) -> int:
        """Calcola score di engagement basato su timeline."""
        if not timeline:
            return 0
        
        # Calcola score basato su numero di eventi
        score = min(len(timeline) * 10, 100)
        return score
    
    async def _fetch_hubspot_data(self, config: ExternalCardConfig, object_id: str) -> Optional[Dict[str, Any]]:
        """
        Recupera dati reali da HubSpot.
        
        Args:
            config: Configurazione card
            object_id: ID oggetto HubSpot
            
        Returns:
            Dizionario con dati recuperati o None
        """
        try:
            if not self.hubspot:
                logger.warning("Client HubSpot non configurato")
                return None
            
            # Recupera dati in base al tipo di oggetto
            if config.hubspot_object_type == 'deals':
                data = await self.hubspot.get_deal(object_id)
            elif config.hubspot_object_type == 'contacts':
                data = await self.hubspot.get_contact(object_id)
            elif config.hubspot_object_type == 'companies':
                data = await self.hubspot.get_company(object_id)
            else:
                logger.error(f"Tipo oggetto non supportato: {config.hubspot_object_type}")
                return None
            
            if not data:
                return None
            
            # Filtra solo le proprietà richieste
            filtered_data = {}
            for prop in config.properties_to_display:
                if prop in data.get('properties', {}):
                    filtered_data[prop] = data['properties'][prop]
            
            # Aggiungi metadati
            filtered_data['_metadata'] = {
                'object_id': object_id,
                'object_type': config.hubspot_object_type,
                'retrieved_at': datetime.now().isoformat(),
                'card_id': config.card_id
            }
            
            return filtered_data
            
        except Exception as e:
            logger.error(f"Errore nel recupero dati HubSpot: {e}")
            return None
    
    async def _get_from_cache(self, card_id: str, object_id: str) -> Optional[ExternalCardData]:
        """Recupera dati dalla cache."""
        try:
            cache_key = f"card:{card_id}:{object_id}"
            cached = await self.repository.get_cached_data(cache_key)
            
            if not cached:
                return None
            
            # Verifica TTL
            cached_time = datetime.fromisoformat(cached.get('cached_at', '2000-01-01'))
            if (datetime.now() - cached_time).total_seconds() > self.cache_ttl_seconds:
                await self.repository.delete_cached_data(cache_key)
                return None
            
            # Converti in ExternalCardData
            data = cached.get('data', {})
            last_sync = datetime.fromisoformat(cached.get('last_sync', datetime.now().isoformat()))
            sync_status = SyncStatus(cached.get('sync_status', 'success'))
            
            return ExternalCardData(
                card_id=card_id,
                object_id=object_id,
                data=data,
                last_sync=last_sync,
                sync_status=sync_status,
                data_hash=cached.get('data_hash')
            )
            
        except Exception as e:
            logger.error(f"Errore nel recupero cache: {e}")
            return None
    
    async def _save_to_cache(self, card_id: str, object_id: str, card_data: ExternalCardData):
        """Salva dati in cache."""
        try:
            cache_key = f"card:{card_id}:{object_id}"
            
            cache_data = {
                'data': card_data.data,
                'last_sync': card_data.last_sync.isoformat(),
                'sync_status': card_data.sync_status.value,
                'data_hash': card_data.data_hash,
                'cached_at': datetime.now().isoformat()
            }
            
            await self.repository.save_cached_data(cache_key, cache_data)
            
        except Exception as e:
            logger.error(f"Errore nel salvataggio cache: {e}")
    
    async def _invalidate_cache(self, card_id: str):
        """Invalida cache per una card."""
        try:
            # Trova tutte le chiavi cache per questa card
            pattern = f"card:{card_id}:*"
            await self.repository.invalidate_cache_pattern(pattern)
            
        except Exception as e:
            logger.error(f"Errore nell'invalidazione cache: {e}")
    
    def _validate_card_config(self, config: ExternalCardConfig) -> bool:
        """Valida configurazione card."""
        try:
            # Verifica campi obbligatori
            if not config.card_id or not config.name:
                return False
            
            if not config.properties_to_display:
                logger.warning(f"Nessuna proprietà specificata per card {config.card_id}")
                return False
            
            # Verifica che le proprietà siano stringhe valide
            for prop in config.properties_to_display:
                if not isinstance(prop, str) or len(prop) == 0:
                    logger.error(f"Proprietà non valida: {prop}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Errore nella validazione configurazione: {e}")
            return False
    
    def _calculate_data_hash(self, data: Dict[str, Any]) -> str:
        """Calcola hash dei dati per rilevare cambiamenti."""
        try:
            data_str = json.dumps(data, sort_keys=True, default=str)
            return hashlib.md5(data_str.encode()).hexdigest()
        except Exception:
            return ""


# Istanza globale del servizio
external_card_service = ExternalCardService()


def get_external_card_service() -> ExternalCardService:
    """Restituisce l'istanza del servizio External Cards."""
    return external_card_service