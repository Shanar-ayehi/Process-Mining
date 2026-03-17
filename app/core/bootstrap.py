"""
Sistema di bootstrap e auto-discovery per il programma Process Mining.
Questo modulo si occupa di:
- Auto-discovery della configurazione HubSpot
- Setup intelligente delle directory
- Configurazione dinamica del sistema
- Validazione dell'ambiente
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import asyncio
import json
import yaml
from datetime import datetime

from app.core.logger import get_logger
from app.core.config import settings
from app.core.hubspot_config import HubSpotConfigManager, StageMapping, DataStructureConfig
from app.connectors.hubspot_client import HubSpotClient, HubSpotAPIError
from app.connectors.hubspot_mapper import HubSpotMapper

logger = get_logger()

class BootstrapManager:
    """Gestore del bootstrap e auto-discovery del sistema."""
    
    def __init__(self):
        self.config_manager = HubSpotConfigManager()
        self.hubspot_client = None
        self.mapper = HubSpotMapper()
        self.discovery_results = {}
        
    async def bootstrap_system(self) -> Dict[str, Any]:
        """
        Esegue il bootstrap completo del sistema.
        
        Returns:
            Dizionario con i risultati del bootstrap
        """
        logger.info("🚀 Avvio bootstrap sistema Process Mining")
        
        bootstrap_result = {
            'timestamp': datetime.now().isoformat(),
            'success': True,
            'steps': [],
            'warnings': [],
            'errors': [],
            'configuration': {}
        }
        
        try:
            # Step 1: Setup directory
            await self._setup_directories()
            bootstrap_result['steps'].append('directory_setup')
            
            # Step 2: Verifica configurazione esistente
            config_status = await self._check_existing_config()
            bootstrap_result['steps'].append('config_check')
            
            # Step 3: Auto-discovery HubSpot (solo se API key disponibile)
            if settings.hubspot_api_key:
                discovery_result = await self._perform_hubspot_discovery()
                bootstrap_result['steps'].append('hubspot_discovery')
                bootstrap_result['discovery'] = discovery_result
            else:
                bootstrap_result['warnings'].append('HubSpot API key non disponibile, skip auto-discovery')
                logger.warning("HubSpot API key non disponibile, creazione configurazione base")
                await self._create_base_config()
            
            # Step 4: Validazione finale
            validation_result = await self._validate_system()
            bootstrap_result['steps'].append('validation')
            bootstrap_result['validation'] = validation_result
            
            # Step 5: Salva risultati bootstrap
            await self._save_bootstrap_results(bootstrap_result)
            
            logger.info("✅ Bootstrap sistema completato con successo")
            return bootstrap_result
            
        except Exception as e:
            logger.error(f"❌ Errore nel bootstrap sistema: {e}")
            bootstrap_result['success'] = False
            bootstrap_result['errors'].append(str(e))
            return bootstrap_result
    
    async def _setup_directories(self):
        """Setup intelligente delle directory del sistema."""
        logger.info("📁 Setup directory sistema")
        
        directories = [
            settings.data_dir,
            settings.logs_dir,
            settings.raw_data_dir,
            settings.staged_data_dir,
            settings.processed_data_dir,
            settings.warehouse_dir,
            settings.config_path.parent
        ]
        
        created_dirs = []
        
        for directory in directories:
            try:
                directory.mkdir(parents=True, exist_ok=True)
                created_dirs.append(str(directory))
                logger.debug(f"Directory assicurata: {directory}")
            except Exception as e:
                logger.error(f"Errore nella creazione directory {directory}: {e}")
                raise
        
        logger.info(f"✅ Setup directory completato: {len(created_dirs)} directory create/verificate")
    
    async def _check_existing_config(self) -> Dict[str, Any]:
        """Verifica la presenza e validità della configurazione esistente."""
        logger.info("🔍 Verifica configurazione esistente")
        
        config_status = {
            'exists': False,
            'valid': False,
            'needs_update': False,
            'version': None
        }
        
        if self.config_manager.config_path.exists():
            config_status['exists'] = True
            logger.info(f"Configurazione trovata: {self.config_manager.config_path}")
            
            # Validazione configurazione
            validation = self.config_manager.validate_config()
            config_status['valid'] = validation['valid']
            config_status['version'] = self.config_manager.config.version
            
            if not validation['valid']:
                config_status['needs_update'] = True
                logger.warning("Configurazione esistente non valida, necessita aggiornamento")
        else:
            logger.info("Nessuna configurazione esistente trovata")
        
        return config_status
    
    async def _perform_hubspot_discovery(self) -> Dict[str, Any]:
        """Esegue l'auto-discovery della configurazione HubSpot."""
        logger.info("🔎 Avvio auto-discovery HubSpot")
        
        discovery_result = {
            'success': False,
            'timestamp': datetime.now().isoformat(),
            'deal_structure': {},
            'pipeline_stages': [],
            'available_properties': {},
            'recommended_config': {},
            'validation': {}
        }
        
        try:
            # Inizializza client HubSpot
            self.hubspot_client = HubSpotClient()
            
            # Step 1: Analisi struttura deal
            logger.info("📊 Analisi struttura deal HubSpot")
            deal_structure = await self._analyze_deal_structure()
            discovery_result['deal_structure'] = deal_structure
            
            # Step 2: Scoperta pipeline stages
            logger.info("🔄 Scoperta pipeline stages")
            pipeline_stages = await self._discover_pipeline_stages()
            discovery_result['pipeline_stages'] = pipeline_stages
            
            # Step 3: Scoperta proprietà disponibili
            logger.info("📋 Scoperta proprietà disponibili")
            available_properties = await self._discover_available_properties()
            discovery_result['available_properties'] = available_properties
            
            # Step 4: Generazione configurazione raccomandata
            logger.info("⚙️ Generazione configurazione raccomandata")
            recommended_config = self._generate_recommended_config(
                pipeline_stages, available_properties, deal_structure
            )
            discovery_result['recommended_config'] = recommended_config
            
            # Step 5: Applicazione configurazione
            logger.info("💾 Applicazione configurazione scoperta")
            await self._apply_discovered_config(recommended_config)
            
            # Step 6: Validazione finale
            validation = self.config_manager.validate_config()
            discovery_result['validation'] = validation
            discovery_result['success'] = validation['valid']
            
            logger.info("✅ Auto-discovery HubSpot completato")
            return discovery_result
            
        except HubSpotAPIError as e:
            logger.error(f"Errore API HubSpot durante discovery: {e}")
            discovery_result['error'] = str(e)
            return discovery_result
        except Exception as e:
            logger.error(f"Errore generico durante discovery: {e}")
            discovery_result['error'] = str(e)
            return discovery_result
    
    async def _analyze_deal_structure(self) -> Dict[str, Any]:
        """Analizza la struttura reale dei deal HubSpot."""
        try:
            # Estrai un campione di deal per l'analisi
            deals_data = self.hubspot_client.get_all_deals_with_history(
                properties_with_history=["dealstage"]
            )[:5]  # Campione ridotto per velocità
            
            structure = {
                'deal_fields': set(),
                'properties_with_history': set(),
                'stage_values': set(),
                'timestamp_formats': set(),
                'sample_size': len(deals_data)
            }
            
            for deal in deals_data:
                if 'properties' in deal:
                    structure['deal_fields'].update(deal['properties'].keys())
                
                if 'propertiesWithHistory' in deal:
                    structure['properties_with_history'].update(deal['propertiesWithHistory'].keys())
                    
                    if 'dealstage' in deal['propertiesWithHistory']:
                        for record in deal['propertiesWithHistory']['dealstage']:
                            if 'value' in record:
                                structure['stage_values'].add(record['value'])
                            if 'timestamp' in record:
                                structure['timestamp_formats'].add(type(record['timestamp']).__name__)
            
            # Converti set in liste
            structure['deal_fields'] = list(structure['deal_fields'])
            structure['properties_with_history'] = list(structure['properties_with_history'])
            structure['stage_values'] = list(structure['stage_values'])
            structure['timestamp_formats'] = list(structure['timestamp_formats'])
            
            return structure
            
        except Exception as e:
            logger.error(f"Errore nell'analisi struttura deal: {e}")
            return {'error': str(e)}
    
    async def _discover_pipeline_stages(self) -> List[Dict[str, Any]]:
        """Scopre automaticamente le fasi delle pipeline."""
        try:
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
            
            return discovered_stages
            
        except Exception as e:
            logger.error(f"Errore nella scoperta pipeline stages: {e}")
            return []
    
    async def _discover_available_properties(self) -> Dict[str, List[str]]:
        """Scopre le proprietà disponibili (semplificato per velocità)."""
        # Per velocità, restituiamo proprietà comuni che sappiamo essere disponibili
        # In un sistema reale, questa potrebbe essere estesa per interrogare lo schema HubSpot
        return {
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
    
    def _generate_recommended_config(self, pipeline_stages: List[Dict[str, Any]],
                                   available_properties: Dict[str, List[str]],
                                   deal_structure: Dict[str, Any]) -> Dict[str, Any]:
        """Genera una configurazione raccomandata basata sulla discovery."""
        
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
        
        # Proprietà personalizzate basate su ciò che abbiamo trovato
        custom_properties = available_properties.get('deal_properties', [])
        
        # Proprietà richieste
        required_properties = ['dealstage', 'createdate']
        
        # Campi privacy
        privacy_fields = ['email', 'firstname', 'lastname', 'phone']
        
        recommended_config = {
            'version': f'auto_discovered_v{datetime.now().strftime("%Y%m%d")}',
            'pipeline_stages': stage_mappings,
            'data_structure': data_structure,
            'custom_properties': custom_properties,
            'required_properties': required_properties,
            'privacy_fields': privacy_fields,
            'discovery_metadata': {
                'discovery_date': datetime.now().isoformat(),
                'stages_found': len(stage_mappings),
                'properties_found': len(custom_properties),
                'sample_size': deal_structure.get('sample_size', 0)
            }
        }
        
        return recommended_config
    
    async def _apply_discovered_config(self, recommended_config: Dict[str, Any]):
        """Applica la configurazione scoperta."""
        try:
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
            
            logger.info("✅ Configurazione auto-discovered applicata e salvata")
            
        except Exception as e:
            logger.error(f"Errore nell'applicazione configurazione: {e}")
            raise
    
    async def _create_base_config(self):
        """Crea una configurazione base quando HubSpot API non è disponibile."""
        logger.info("⚙️ Creazione configurazione base")
        
        # Usa la configurazione di default esistente
        default_config = self.config_manager._get_default_config()
        self.config_manager.save_config(default_config)
        
        logger.info("✅ Configurazione base creata")
    
    async def _validate_system(self) -> Dict[str, Any]:
        """Valida lo stato finale del sistema."""
        validation = {
            'directories_ok': True,
            'config_valid': True,
            'api_accessible': False,
            'data_quality_ok': True,
            'issues': []
        }
        
        # Verifica directory
        required_dirs = [
            settings.data_dir,
            settings.logs_dir,
            settings.raw_data_dir
        ]
        
        for directory in required_dirs:
            if not directory.exists():
                validation['directories_ok'] = False
                validation['issues'].append(f"Directory mancante: {directory}")
        
        # Verifica configurazione
        config_validation = self.config_manager.validate_config()
        validation['config_valid'] = config_validation['valid']
        if not config_validation['valid']:
            validation['issues'].extend(config_validation.get('errors', []))
        
        # Verifica accesso API (se disponibile)
        if settings.hubspot_api_key:
            try:
                client = HubSpotClient()
                # Prova una chiamata semplice
                client.get_pipeline_stages()
                validation['api_accessible'] = True
            except Exception as e:
                validation['issues'].append(f"API HubSpot non accessibile: {e}")
        
        return validation
    
    async def _save_bootstrap_results(self, bootstrap_result: Dict[str, Any]):
        """Salva i risultati del bootstrap."""
        try:
            bootstrap_dir = settings.data_dir / "bootstrap"
            bootstrap_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"bootstrap_result_{timestamp}.json"
            filepath = bootstrap_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(bootstrap_result, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Risultati bootstrap salvati in: {filepath}")
            
        except Exception as e:
            logger.error(f"Errore nel salvataggio risultati bootstrap: {e}")

# Funzione helper per eseguire il bootstrap
async def run_bootstrap() -> Dict[str, Any]:
    """Esegue il bootstrap del sistema."""
    bootstrap_manager = BootstrapManager()
    return await bootstrap_manager.bootstrap_system()

def run_bootstrap_sync() -> Dict[str, Any]:
    """Esegue il bootstrap in modo sincrono."""
    return asyncio.run(run_bootstrap())

# Creazione istanza globale
bootstrap_manager = BootstrapManager()