from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import polars as pl
from app.core.logger import get_logger
from app.core.privacy import privacy_manager
from app.core.hubspot_config import hubspot_config_manager

logger = get_logger()

class HubSpotMapper:
    """Mapper per la trasformazione dei dati HubSpot in formati standard per Process Mining."""
    
    def __init__(self):
        self.config_manager = hubspot_config_manager
        self.data_structure = self.config_manager.get_data_structure()
    
    def map_deal_to_event_log(self, deal_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Trasforma i dati di un deal HubSpot in event log per Process Mining.
        
        Args:
            deal_data: Dati deal da HubSpot con cronologia
            
        Returns:
            Lista di eventi per il log
        """
        events = []
        # ✅ FIX DEFINITIVO: Case ID Unico - GARANTITO che tutti gli eventi di questo deal abbiano lo stesso ID
        # Usiamo direttamente l'id del deal, che è immutabile e unico per tutta la durata del deal
        deal_id = str(deal_data.get('id')).strip()
        
        # Controllo di sicurezza: se per qualche motivo l'id è mancante, fallisco esplicitamente
        if not deal_id or deal_id == 'None':
            logger.error(f"❌ DEAL SENZA ID! Dati ricevuti: {list(deal_data.keys())}")
            raise ValueError("Deal ID obbligatorio mancante")
            
        logger.debug(f"✅ Case ID assegnato: {deal_id} per tutti gli eventi di questo deal")
        deal_properties = deal_data.get('properties', {})
        
        # Estrai la cronologia delle fasi usando la configurazione dinamica
        history_data = deal_data.get(self.data_structure.deal_history_field, {})
        stage_history = history_data.get(self.data_structure.stage_field, [])
        
        # Ordina la cronologia per timestamp (supporta stringhe ISO 8601 e numeri)
        def sort_key(record):
            ts = record.get(self.data_structure.timestamp_field, "0")
            return str(ts)
        
        stage_history.sort(key=sort_key)
        
        for record in stage_history:
            # ✅ FIX: Mappatura automatica fasi - usa direttamente il valore originale
            stage_value = record.get('value', '')
            # Usa direttamente il nome della fase da HubSpot, formattato in Title Case per leggibilità
            activity_name = str(stage_value).strip().replace('_', ' ').title()
            logger.debug(f"Fase rilevata automaticamente: {activity_name}")
            
            # Estrai informazioni aggiuntive
            timestamp = self._parse_timestamp(record.get(self.data_structure.timestamp_field))
            source_id = record.get('sourceId', 'System')
            
            # Crea evento con proprietà configurabili
            event = {
                "case_id": deal_id,
                "activity": activity_name,
                "timestamp": timestamp,
                "resource": privacy_manager.hash_email(source_id),  # Privacy
                "stage_id": stage_value,
                "stage_order": self.config_manager.get_stage_order(stage_value.lower()),
                "is_final_stage": self.config_manager.is_final_stage(stage_value.lower())
            }
            
            # Aggiungi proprietà personalizzate configurabili
            for prop in self.config_manager.config.custom_properties:
                if prop in deal_properties:
                    event[prop] = deal_properties[prop]
            
            events.append(event)
        
        return events
    
    def map_multiple_deals_to_dataframe(self, deals_data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Trasforma multipli deal in un DataFrame Polars.
        
        Args:
            deals_data: Lista di deal da HubSpot
            
        Returns:
            DataFrame Polars pronto per l'analisi
        """
        all_events = []

        # Unwrap automatico wrapper Celery Task Response
        if isinstance(deals_data, dict):
            logger.info("DEBUG MAPPER - Rilevato wrapper Celery, eseguo unwrapping dati...")
            
            # Estrai dati dal wrapper standard create_task_result
            if 'data' in deals_data:
                deals_data = deals_data['data']
                
            # Se data è ancora un dizionario, estrai la lista deal direttamente
            if isinstance(deals_data, dict):
                # Cerca la lista dei deal tra tutte le chiavi comuni
                for key in ['deals', 'deals_data', 'results', 'items']:
                    if key in deals_data and isinstance(deals_data[key], list):
                        logger.info(f"DEBUG MAPPER - Trovata lista deal in chiave: {key}")
                        deals_data = deals_data[key]
                        break

        # DEBUG LOG per identificare problema 0 eventi
        if deals_data and len(deals_data) > 0:
            logger.info(f"DEBUG MAPPER - Ricevuti {len(deals_data)} deal totali")
            logger.info(f"DEBUG MAPPER - Chiavi primo deal: {list(deals_data[0].keys())}")
            logger.info(f"DEBUG MAPPER - propertiesWithHistory presente: {'propertiesWithHistory' in deals_data[0]}")
            if 'propertiesWithHistory' in deals_data[0]:
                logger.info(f"DEBUG MAPPER - Chiavi dentro propertiesWithHistory: {list(deals_data[0]['propertiesWithHistory'].keys())}")
                logger.info(f"DEBUG MAPPER - Numero record history: {len(deals_data[0]['propertiesWithHistory'].get('dealstage', []))}")
        
        for deal in deals_data:
            events = self.map_deal_to_event_log(deal)
            logger.info(f"DEBUG MAPPER - Deal {deal.get('id')} generati {len(events)} eventi")
            all_events.extend(events)
        
        logger.info(f"DEBUG MAPPER - Totale eventi generati: {len(all_events)}")

        if not all_events:
            logger.warning("Nessun evento trovato nei deal")
            return pl.DataFrame()
        
        # Crea DataFrame
        df = pl.DataFrame(all_events)
        
        # Converte timestamp in datetime (Fix per formato ISO con Z finale)
        if 'timestamp' in df.columns:
            df = df.with_columns([
                pl.col("timestamp").str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.fZ", strict=False)
            ])
        
        # Ordina cronologicamente
        df = df.sort(['case_id', 'timestamp'])
        
        logger.info(f"Creato DataFrame con {len(df)} eventi da {len(deals_data)} deal")
        return df
    
    def map_contact_to_entity(self, contact_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mappa i dati contatto per l'integrazione con event log.
        
        Args:
            contact_data: Dati contatto da HubSpot
            
        Returns:
            Dati contatto mappati
        """
        properties = contact_data.get('properties', {})
        contact_id = contact_data.get(self.data_structure.contact_id_field)
        
        # Crea entità base
        entity = {
            "contact_id": contact_id,
            "created_date": self._parse_timestamp(properties.get('createdate')),
            "last_modified": self._parse_timestamp(properties.get('lastmodifieddate'))
        }
        
        # Aggiungi campi privacy con pseudonimizzazione
        for field in self.config_manager.get_privacy_fields():
            if field in properties:
                if field == 'email':
                    entity[field] = privacy_manager.hash_email(properties[field])
                else:
                    entity[field] = privacy_manager.hash_field(properties[field], field)
        
        # Aggiungi altre proprietà configurabili
        for prop in self.config_manager.config.custom_properties:
            if prop in properties and prop not in self.config_manager.get_privacy_fields():
                entity[prop] = properties[prop]
        
        return entity
    
    def map_company_to_entity(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mappa i dati azienda per l'integrazione con event log.
        
        Args:
            company_data: Dati azienda da HubSpot
            
        Returns:
            Dati azienda mappati
        """
        properties = company_data.get('properties', {})
        company_id = company_data.get(self.data_structure.company_id_field)
        
        # Crea entità base
        entity = {
            "company_id": company_id,
            "created_date": self._parse_timestamp(properties.get('createdate')),
            "last_modified": self._parse_timestamp(properties.get('lastmodifieddate'))
        }
        
        # Aggiungi proprietà configurabili
        for prop in self.config_manager.config.custom_properties:
            if prop in properties:
                entity[prop] = properties[prop]
        
        return entity
    
    def _parse_timestamp(self, timestamp: Union[str, int, None]) -> Optional[str]:
        """
        Parsa timestamp in formato ISO string.
        
        Args:
            timestamp: Timestamp da parsare
            
        Returns:
            Timestamp in formato ISO string o None
        """
        if not timestamp:
            return None
        
        try:
            if isinstance(timestamp, str):
                # Già in formato stringa, verifica validità
                datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                return timestamp
            elif isinstance(timestamp, (int, float)):
                # Timestamp in millisecondi
                dt = datetime.fromtimestamp(timestamp / 1000)
                return dt.isoformat()
        except (ValueError, TypeError):
            logger.warning(f"Timestamp non valido: {timestamp}")
            return None
        
        return None

# Creazione istanza globale
hubspot_mapper = HubSpotMapper()