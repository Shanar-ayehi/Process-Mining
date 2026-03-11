from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import polars as pl
from app.core.logger import get_logger
from app.core.privacy import privacy_manager

logger = get_logger()

class HubSpotMapper:
    """Mapper per la trasformazione dei dati HubSpot in formati standard per Process Mining."""
    
    def __init__(self):
        self.stage_mapping = {
            "appointmentscheduled": "Appuntamento Pianificato",
            "qualifiedtobuy": "Qualificato all'Acquisto", 
            "presentationscheduled": "Presentazione Pianificata",
            "decisionmakerboughtin": "Decision Maker Coinvolto",
            "contractsent": "Contratto Inviato",
            "closedwon": "Chiuso Vinto",
            "closedlost": "Chiuso Perso"
        }
    
    def map_deal_to_event_log(self, deal_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Trasforma i dati di un deal HubSpot in event log per Process Mining.
        
        Args:
            deal_data: Dati deal da HubSpot con cronologia
            
        Returns:
            Lista di eventi per il log
        """
        events = []
        deal_id = deal_data.get('id')
        deal_properties = deal_data.get('properties', {})
        
        # Estrai la cronologia delle fasi
        history_data = deal_data.get('propertiesWithHistory', {})
        stage_history = history_data.get('dealstage', [])
        
        # Ordina la cronologia per timestamp
        stage_history.sort(key=lambda x: x.get('timestamp', 0))
        
        for record in stage_history:
            # Mappa il nome della fase
            stage_value = record.get('value', '')
            activity_name = self.stage_mapping.get(stage_value.lower(), f"Fase Sconosciuta: {stage_value}")
            
            # Estrai informazioni aggiuntive
            timestamp = self._parse_timestamp(record.get('timestamp'))
            source_id = record.get('sourceId', 'System')
            
            # Crea evento
            event = {
                "case_id": deal_id,
                "activity": activity_name,
                "timestamp": timestamp,
                "resource": privacy_manager.hash_email(source_id),  # Privacy
                "deal_name": deal_properties.get('dealname', ''),
                "amount": deal_properties.get('amount', ''),
                "pipeline": deal_properties.get('pipeline', ''),
                "stage_id": stage_value
            }
            
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
        
        for deal in deals_data:
            events = self.map_deal_to_event_log(deal)
            all_events.extend(events)
        
        if not all_events:
            logger.warning("Nessun evento trovato nei deal")
            return pl.DataFrame()
        
        # Crea DataFrame
        df = pl.DataFrame(all_events)
        
        # Converte timestamp in datetime
        if 'timestamp' in df.columns:
            df = df.with_columns([
                pl.col('timestamp').str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S.%fZ")
                .fill_null(pl.col('timestamp').str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ"))
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
        
        return {
            "contact_id": contact_data.get('id'),
            "email": privacy_manager.hash_email(properties.get('email', '')),
            "first_name": properties.get('firstname', ''),
            "last_name": properties.get('lastname', ''),
            "company": properties.get('company', ''),
            "job_title": properties.get('jobtitle', ''),
            "created_date": self._parse_timestamp(properties.get('createdate')),
            "last_modified": self._parse_timestamp(properties.get('lastmodifieddate'))
        }
    
    def map_company_to_entity(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mappa i dati azienda per l'integrazione con event log.
        
        Args:
            company_data: Dati azienda da HubSpot
            
        Returns:
            Dati azienda mappati
        """
        properties = company_data.get('properties', {})
        
        return {
            "company_id": company_data.get('id'),
            "name": properties.get('name', ''),
            "domain": properties.get('domain', ''),
            "industry": properties.get('industry', ''),
            "annual_revenue": properties.get('annualrevenue', ''),
            "created_date": self._parse_timestamp(properties.get('createdate')),
            "last_modified": self._parse_timestamp(properties.get('lastmodifieddate'))
        }
    
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
