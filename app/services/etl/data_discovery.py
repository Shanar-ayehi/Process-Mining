from typing import Dict, List, Any, Optional, Union
from pathlib import Path
import json
import yaml
from app.core.logger import get_logger
from app.core.hubspot_config import HubSpotConfigManager, StageMapping, DataStructureConfig
from app.connectors.hubspot_client import HubSpotClient, HubSpotAPIError

logger = get_logger()

class DataStructureAnalyzer:
    """Analizzatore automatico della struttura dei dati HubSpot."""
    
    def __init__(self, hubspot_client: Optional[HubSpotClient] = None):
        """
        Inizializza l'analizzatore della struttura dati.
        
        Args:
            hubspot_client: Client HubSpot (se None, non viene creato automaticamente)
        """
        self.hubspot_client = hubspot_client  # Non creare istanza automatica
    
    def analyze_deal_structure(self, sample_size: int = 10) -> Dict[str, Any]:
        """
        Analizza la struttura reale dei deal HubSpot.
        
        Args:
            sample_size: Numero di deal da analizzare come campione
            
        Returns:
            Struttura analizzata dei deal
        """
        try:
            logger.info(f"Analisi struttura deal (campione: {sample_size})")
            
            # Estrai un campione di deal
            deals_data = self.hubspot_client.get_all_deals_with_history(
                properties_with_history=["dealstage"]
            )[:sample_size]
            
            if not deals_data:
                logger.warning("Nessun deal trovato per l'analisi")
                return {}
            
            # Analizza la struttura
            structure = {
                'deal_fields': set(),
                'properties_with_history': set(),
                'stage_values': set(),
                'timestamp_formats': set(),
                'sample_deal_structure': {}
            }
            
            for deal in deals_data:
                # Analizza campi principali
                if 'properties' in deal:
                    structure['deal_fields'].update(deal['properties'].keys())
                
                # Analizza cronologia
                if 'propertiesWithHistory' in deal:
                    structure['properties_with_history'].update(deal['propertiesWithHistory'].keys())
                    
                    # Analizza valori delle fasi
                    if 'dealstage' in deal['propertiesWithHistory']:
                        for record in deal['propertiesWithHistory']['dealstage']:
                            if 'value' in record:
                                structure['stage_values'].add(record['value'])
                            if 'timestamp' in record:
                                structure['timestamp_formats'].add(type(record['timestamp']).__name__)
            
            # Converti set in liste per JSON serialization
            structure['deal_fields'] = list(structure['deal_fields'])
            structure['properties_with_history'] = list(structure['properties_with_history'])
            structure['stage_values'] = list(structure['stage_values'])
            structure['timestamp_formats'] = list(structure['timestamp_formats'])
            
            # Salva struttura di esempio
            if deals_data:
                structure['sample_deal_structure'] = deals_data[0]
            
            logger.info(f"Struttura deal analizzata: {len(structure['deal_fields'])} campi, {len(structure['stage_values'])} fasi")
            return structure
            
        except HubSpotAPIError as e:
            logger.error(f"Errore nell'analisi struttura deal: {e}")
            raise
        except Exception as e:
            logger.error(f"Errore imprevisto nell'analisi struttura deal: {e}")
            raise
    
    def discover_pipeline_stages(self) -> List[Dict[str, Any]]:
        """
        Scopre automaticamente le fasi delle pipeline disponibili.
        
        Returns:
            Lista delle fasi delle pipeline
        """
        try:
            logger.info("Scoperta automatica fasi pipeline")
            
            stages_data = self.hubspot_client.get_pipeline_stages()
            
            discovered_stages = []
            stage_order = 1
            
            for pipeline in stages_data:
                pipeline_name = pipeline.get('label', 'Unknown Pipeline')
                
                for stage in pipeline.get('stages', []):
                    stage_info = {
                        'stage_id': stage.get('stageId', ''),
                        'display_name': stage.get('label', ''),
                        'pipeline': pipeline_name,
                        'order': stage_order,
                        'is_final': stage.get('probability', '').lower() == 'lose',
                        'metadata': {
                            'stageId': stage.get('stageId', ''),
                            'label': stage.get('label', ''),
                            'displayOrder': stage.get('displayOrder', 0),
                            'probability': stage.get('probability', ''),
                            'pipelineId': pipeline.get('pipelineId', ''),
                            'pipelineLabel': pipeline.get('label', '')
                        }
                    }
                    
                    discovered_stages.append(stage_info)
                    stage_order += 1
            
            logger.info(f"Scoperte {len(discovered_stages)} fasi pipeline")
            return discovered_stages
            
        except HubSpotAPIError as e:
            logger.error(f"Errore nella scoperta fasi pipeline: {e}")
            raise
        except Exception as e:
            logger.error(f"Errore imprevisto nella scoperta fasi pipeline: {e}")
            raise
    
    def discover_available_properties(self) -> Dict[str, Any]:
        """
        Scopre automaticamente le proprietà disponibili.
        
        Returns:
            Dizionario delle proprietà disponibili
        """
        try:
            logger.info("Scoperta proprietà disponibili")
            
            # Per ora restituiamo proprietà comuni, in futuro si potrebbe
            # implementare una chiamata API per ottenere lo schema completo
            common_properties = {
                'deal_properties': [
                    'dealname', 'amount', 'closedate', 'pipeline', 'dealstage',
                    'dealtype', 'hubspot_owner_id', 'createdate', 'lastmodifieddate'
                ],
                'contact_properties': [
                    'email', 'firstname', 'lastname', 'phone', 'mobilephone',
                    'company', 'jobtitle', 'createdate', 'lastmodifieddate'
                ],
                'company_properties': [
                    'name', 'domain', 'industry', 'annualrevenue', 'phone',
                    'address', 'city', 'state', 'country', 'createdate'
                ],
                'history_properties': [
                    'dealstage', 'pipeline', 'hubspot_owner_id'
                ]
            }
            
            logger.info("Proprietà disponibili scoperte")
            return common_properties
            
        except Exception as e:
            logger.error(f"Errore nella scoperta proprietà: {e}")
            raise

class AutoDiscoveryService:
    """Servizio di auto-discovery per la configurazione HubSpot."""
    
    def __init__(self, config_manager: Optional[HubSpotConfigManager] = None,
                 hubspot_client: Optional[HubSpotClient] = None):
        """
        Inizializza il servizio di auto-discovery.
        
        Args:
            config_manager: Gestore configurazione (se None, ne crea uno nuovo)
            hubspot_client: Client HubSpot (se None, ne crea uno nuovo)
        """
        self.config_manager = config_manager or HubSpotConfigManager()
        self.analyzer = DataStructureAnalyzer(hubspot_client)
    
    def run_full_discovery(self) -> Dict[str, Any]:
        """
        Esegue la discovery completa della configurazione HubSpot.
        
        Returns:
            Risultati della discovery
        """
        try:
            logger.info("Inizio discovery completa HubSpot")
            
            discovery_results = {
                'success': True,
                'timestamp': None,
                'deal_structure': {},
                'pipeline_stages': [],
                'available_properties': {},
                'recommended_config': {},
                'validation': {}
            }
            
            # Analisi struttura deal
            discovery_results['deal_structure'] = self.analyzer.analyze_deal_structure()
            
            # Scoperta fasi pipeline
            discovery_results['pipeline_stages'] = self.analyzer.discover_pipeline_stages()
            
            # Scoperta proprietà disponibili
            discovery_results['available_properties'] = self.analyzer.discover_available_properties()
            
            # Genera configurazione raccomandata
            discovery_results['recommended_config'] = self._generate_recommended_config(
                discovery_results['pipeline_stages'],
                discovery_results['available_properties']
            )
            
            # Validazione configurazione
            discovery_results['validation'] = self._validate_discovery(discovery_results)
            
            discovery_results['timestamp'] = self._get_timestamp()
            
            logger.info("Discovery completa HubSpot completata")
            return discovery_results
            
        except Exception as e:
            logger.error(f"Errore nella discovery completa: {e}")
            discovery_results['success'] = False
            discovery_results['error'] = str(e)
            return discovery_results
    
    def _generate_recommended_config(self, pipeline_stages: List[Dict[str, Any]],
                                   available_properties: Dict[str, List[str]]) -> Dict[str, Any]:
        """
        Genera una configurazione raccomandata basata sulla discovery.
        
        Args:
            pipeline_stages: Fasi pipeline scoperte
            available_properties: Proprietà disponibili
            
        Returns:
            Configurazione raccomandata
        """
        # Crea stage mappings
        stage_mappings = []
        for stage in pipeline_stages:
            stage_mappings.append({
                'stage_id': stage['stage_id'],
                'display_name': stage['display_name'],
                'order': stage['order'],
                'is_final': stage['is_final']
            })
        
        # Crea data structure config
        data_structure = {
            'deal_history_field': 'propertiesWithHistory',
            'stage_field': 'dealstage',
            'timestamp_field': 'timestamp',
            'deal_id_field': 'id',
            'contact_id_field': 'id',
            'company_id_field': 'id'
        }
        
        # Proprietà personalizzate
        custom_properties = available_properties.get('deal_properties', [])
        
        # Proprietà richieste
        required_properties = ['dealstage', 'createdate']
        
        # Campi privacy
        privacy_fields = ['email', 'firstname', 'lastname', 'phone']
        
        recommended_config = {
            'version': 'auto_discovered_v1',
            'pipeline_stages': stage_mappings,
            'data_structure': data_structure,
            'custom_properties': custom_properties,
            'required_properties': required_properties,
            'privacy_fields': privacy_fields
        }
        
        return recommended_config
    
    def _validate_discovery(self, discovery_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Valida i risultati della discovery.
        
        Args:
            discovery_results: Risultati della discovery
            
        Returns:
            Report di validazione
        """
        validation = {
            'valid': True,
            'warnings': [],
            'errors': [],
            'stage_count': len(discovery_results.get('pipeline_stages', [])),
            'properties_count': len(discovery_results.get('available_properties', {}).get('deal_properties', []))
        }
        
        # Verifica che ci siano fasi pipeline
        if validation['stage_count'] == 0:
            validation['valid'] = False
            validation['errors'].append("Nessuna fase pipeline scoperta")
        
        # Verifica che ci siano proprietà deal
        if validation['properties_count'] == 0:
            validation['warnings'].append("Nessuna proprietà deal scoperta")
        
        # Verifica che ci siano fasi finali
        final_stages = [s for s in discovery_results.get('pipeline_stages', []) if s.get('is_final', False)]
        if not final_stages:
            validation['warnings'].append("Nessuna fase finale scoperta")
        
        return validation
    
    def save_discovery_results(self, results: Dict[str, Any], 
                              filename: Optional[str] = None) -> Path:
        """
        Salva i risultati della discovery su file.
        
        Args:
            results: Risultati della discovery
            filename: Nome file (se None, genera automaticamente)
            
        Returns:
            Percorso del file salvato
        """
        try:
            if filename is None:
                timestamp = self._get_timestamp()
                filename = f"hubspot_discovery_{timestamp}.yaml"
            
            # Crea directory se non esiste
            discovery_dir = Path(__file__).parent.parent.parent / "data" / "discovery"
            discovery_dir.mkdir(parents=True, exist_ok=True)
            
            filepath = discovery_dir / filename
            
            # Salva in YAML
            with open(filepath, 'w', encoding='utf-8') as f:
                yaml.dump(results, f, default_flow_style=False, allow_unicode=True)
            
            logger.info(f"Risultati discovery salvati in: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Errore nel salvataggio risultati discovery: {e}")
            raise
    
    def apply_discovery_config(self, results: Dict[str, Any]) -> bool:
        """
        Applica la configurazione scoperta.
        
        Args:
            results: Risultati della discovery
            
        Returns:
            True se applicata con successo, False altrimenti
        """
        try:
            recommended_config = results.get('recommended_config', {})
            
            if not recommended_config:
                logger.warning("Nessuna configurazione raccomandata da applicare")
                return False
            
            # Crea nuova configurazione
            from app.core.hubspot_config import HubSpotSchemaConfig, StageMapping, DataStructureConfig
            
            stage_mappings = []
            for stage_data in recommended_config.get('pipeline_stages', []):
                stage_mappings.append(StageMapping(
                    stage_id=stage_data['stage_id'],
                    display_name=stage_data['display_name'],
                    order=stage_data['order'],
                    is_final=stage_data['is_final']
                ))
            
            data_structure = DataStructureConfig(**recommended_config.get('data_structure', {}))
            
            new_config = HubSpotSchemaConfig(
                version=recommended_config.get('version', 'auto_discovered'),
                stage_mappings=stage_mappings,
                data_structure=data_structure,
                custom_properties=recommended_config.get('custom_properties', []),
                required_properties=recommended_config.get('required_properties', []),
                privacy_fields=recommended_config.get('privacy_fields', [])
            )
            
            # Salva la nuova configurazione
            self.config_manager.save_config(new_config)
            
            logger.info("Configurazione discovery applicata con successo")
            return True
            
        except Exception as e:
            logger.error(f"Errore nell'applicazione configurazione discovery: {e}")
            return False
    
    def _get_timestamp(self) -> str:
        """Genera timestamp per i nomi file."""
        from datetime import datetime
        return datetime.now().strftime("%Y%m%d_%H%M%S")

# Istanza globale del servizio di auto-discovery
auto_discovery_service = AutoDiscoveryService()

