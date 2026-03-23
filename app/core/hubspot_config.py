from typing import Dict, List, Any, Optional, Union
from pathlib import Path
import json
import yaml
from dataclasses import dataclass, asdict
from app.core.logger import get_logger
from app.core.database import get_db_connection

logger = get_logger()

@dataclass
class StageMapping:
    """Configurazione per il mapping delle fasi della pipeline."""
    stage_id: str
    display_name: str
    order: int
    is_final: bool = False

@dataclass
class DataStructureConfig:
    """Configurazione per la struttura dei dati HubSpot."""
    deal_history_field: str = "propertiesWithHistory"
    stage_field: str = "dealstage"
    timestamp_field: str = "timestamp"
    deal_id_field: str = "id"
    contact_id_field: str = "id"
    company_id_field: str = "id"

@dataclass
class HubSpotSchemaConfig:
    """Configurazione completa dello schema HubSpot."""
    version: str = "v1"
    stage_mappings: List[StageMapping] = None
    data_structure: DataStructureConfig = None
    custom_properties: List[str] = None
    required_properties: List[str] = None
    privacy_fields: List[str] = None
    
    def __post_init__(self):
        if self.stage_mappings is None:
            self.stage_mappings = []
        if self.data_structure is None:
            self.data_structure = DataStructureConfig()
        if self.custom_properties is None:
            self.custom_properties = []
        if self.required_properties is None:
            self.required_properties = []
        if self.privacy_fields is None:
            self.privacy_fields = []

class HubSpotConfigManager:
    """Gestore della configurazione HubSpot per massima plasticità."""
    
    def __init__(self, config_path: Optional[str] = None, portal_id: Optional[str] = None):
        """
        Inizializza il gestore della configurazione.
        
        Args:
            config_path: Percorso al file di configurazione (opzionale)
            portal_id: ID del portale HubSpot per multi-tenancy (opzionale)
        """
        self.config_path = config_path or Path(__file__).parent.parent / "config" / "hubspot_schema.yaml"
        self.portal_id = portal_id
        self.config = self._load_config()
        
    def _load_config(self) -> HubSpotSchemaConfig:
        """Carica la configurazione da file o crea quella di default."""
        try:
            if self.config_path.exists():
                return self._load_from_file()
            else:
                logger.warning(f"File configurazione non trovato: {self.config_path}")
                return self._get_default_config()
        except Exception as e:
            logger.error(f"Errore nel caricamento configurazione: {e}")
            return self._get_default_config()
    
    def _load_from_file(self) -> HubSpotSchemaConfig:
        """Carica la configurazione da file YAML o JSON."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                if self.config_path.suffix.lower() in ['.yaml', '.yml']:
                    config_data = yaml.safe_load(f)
                elif self.config_path.suffix.lower() == '.json':
                    config_data = json.load(f)
                else:
                    raise ValueError(f"Formato file non supportato: {self.config_path.suffix}")
            
            return self._dict_to_config(config_data)
        except Exception as e:
            logger.error(f"Errore nel parsing del file di configurazione: {e}")
            raise
    
    def _dict_to_config(self, config_data: Dict[str, Any]) -> HubSpotSchemaConfig:
        """Converte un dizionario in oggetto configurazione."""
        hubspot_data = config_data.get('hubspot', {})
        
        # Crea stage mappings
        stage_mappings = []
        for stage_data in hubspot_data.get('pipeline_stages', []):
            stage_mappings.append(StageMapping(
                stage_id=stage_data['stage_id'],
                display_name=stage_data['display_name'],
                order=stage_data['order'],
                is_final=stage_data.get('is_final', False)
            ))
        
        # Crea data structure config
        data_structure_data = hubspot_data.get('data_structure', {})
        data_structure = DataStructureConfig(
            deal_history_field=data_structure_data.get('deal_history_field', 'propertiesWithHistory'),
            stage_field=data_structure_data.get('stage_field', 'dealstage'),
            timestamp_field=data_structure_data.get('timestamp_field', 'timestamp'),
            deal_id_field=data_structure_data.get('deal_id_field', 'id'),
            contact_id_field=data_structure_data.get('contact_id_field', 'id'),
            company_id_field=data_structure_data.get('company_id_field', 'id')
        )
        
        # Crea configurazione finale
        config = HubSpotSchemaConfig(
            version=hubspot_data.get('version', 'v1'),
            stage_mappings=stage_mappings,
            data_structure=data_structure,
            custom_properties=hubspot_data.get('custom_properties', []),
            required_properties=hubspot_data.get('required_properties', []),
            privacy_fields=hubspot_data.get('privacy_fields', [])
        )
        
        return config
    
    def _get_default_config(self) -> HubSpotSchemaConfig:
        """Restituisce la configurazione di default."""
        default_stages = [
            StageMapping("appointmentscheduled", "Appuntamento Pianificato", 1, False),
            StageMapping("qualifiedtobuy", "Qualificato all'Acquisto", 2, False),
            StageMapping("presentationscheduled", "Presentazione Pianificata", 3, False),
            StageMapping("decisionmakerboughtin", "Decision Maker Coinvolto", 4, False),
            StageMapping("contractsent", "Contratto Inviato", 5, False),
            StageMapping("closedwon", "Chiuso Vinto", 6, True),
            StageMapping("closedlost", "Chiuso Perso", 7, True)
        ]
        
        return HubSpotSchemaConfig(
            version="v1",
            stage_mappings=default_stages,
            custom_properties=["dealname", "amount", "pipeline"],
            required_properties=["dealstage", "createdate"],
            privacy_fields=["email", "firstname", "lastname"]
        )
    
    def _init_config_table(self):
        """Inizializza la tabella hubspot_configs nel database se non esiste."""
        try:
            conn = get_db_connection()
            conn.execute("""
                CREATE TABLE IF NOT EXISTS hubspot_configs (
                    portal_id VARCHAR(50) PRIMARY KEY,
                    config_data TEXT NOT NULL,
                    version VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.close()
            logger.debug("Tabella hubspot_configs inizializzata")
        except Exception as e:
            logger.error(f"Errore nell'inizializzazione tabella hubspot_configs: {e}")
            raise
    
    def save_config_to_db(self, config: HubSpotSchemaConfig):
        """Salva la configurazione nel database SQLite per multi-tenancy."""
        if not self.portal_id:
            logger.warning("Portal ID non specificato, skip salvataggio in database")
            return
        
        try:
            self._init_config_table()
            
            # Converte configurazione in JSON
            config_dict = {
                "hubspot": {
                    "version": config.version,
                    "pipeline_stages": [asdict(stage) for stage in config.stage_mappings],
                    "data_structure": asdict(config.data_structure),
                    "custom_properties": config.custom_properties,
                    "required_properties": config.required_properties,
                    "privacy_fields": config.privacy_fields
                }
            }
            config_json = json.dumps(config_dict, ensure_ascii=False)
            
            # Salva nel database
            conn = get_db_connection()
            conn.execute("""
                INSERT OR REPLACE INTO hubspot_configs (portal_id, config_data, version, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (self.portal_id, config_json, config.version))
            conn.close()
            
            logger.info(f"Configurazione salvata nel database per portal_id: {self.portal_id}")
            
        except Exception as e:
            logger.error(f"Errore nel salvataggio configurazione nel database: {e}")
            raise
    
    def load_config_from_db(self) -> Optional[HubSpotSchemaConfig]:
        """Carica la configurazione dal database SQLite per multi-tenancy."""
        if not self.portal_id:
            logger.warning("Portal ID non specificato, skip caricamento da database")
            return None
        
        try:
            self._init_config_table()
            
            conn = get_db_connection()
            result = conn.execute("""
                SELECT config_data FROM hubspot_configs WHERE portal_id = ?
            """, (self.portal_id,)).fetchone()
            conn.close()
            
            if result:
                config_json = result[0]
                config_dict = json.loads(config_json)
                config = self._dict_to_config(config_dict)
                logger.info(f"Configurazione caricata dal database per portal_id: {self.portal_id}")
                return config
            else:
                logger.info(f"Nessuna configurazione trovata nel database per portal_id: {self.portal_id}")
                return None
                
        except Exception as e:
            logger.error(f"Errore nel caricamento configurazione dal database: {e}")
            return None
    
    def save_config(self, config: HubSpotSchemaConfig):
        """Salva la configurazione su file e nel database."""
        try:
            # Crea directory se non esiste
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Converte in dizionario
            config_dict = {
                "hubspot": {
                    "version": config.version,
                    "pipeline_stages": [asdict(stage) for stage in config.stage_mappings],
                    "data_structure": asdict(config.data_structure),
                    "custom_properties": config.custom_properties,
                    "required_properties": config.required_properties,
                    "privacy_fields": config.privacy_fields
                }
            }
            
            # Salva in YAML
            with open(self.config_path, 'w', encoding='utf-8') as f:
                yaml.dump(config_dict, f, default_flow_style=False, allow_unicode=True)
            
            logger.info(f"Configurazione salvata in: {self.config_path}")
            
            # Salva anche nel database se portal_id è specificato
            if self.portal_id:
                self.save_config_to_db(config)
            
        except Exception as e:
            logger.error(f"Errore nel salvataggio configurazione: {e}")
            raise
    
    def add_stage_mapping(self, stage_id: str, display_name: str, order: int, is_final: bool = False):
        """Aggiunge un nuovo mapping di fase."""
        stage = StageMapping(stage_id, display_name, order, is_final)
        self.config.stage_mappings.append(stage)
        self.config.stage_mappings.sort(key=lambda x: x.order)
        logger.info(f"Aggiunta fase: {stage_id} -> {display_name}")
    
    def update_data_structure(self, **kwargs):
        """Aggiorna la struttura dei dati."""
        for key, value in kwargs.items():
            if hasattr(self.config.data_structure, key):
                setattr(self.config.data_structure, key, value)
                logger.info(f"Aggiornato campo dati: {key} = {value}")
    
    def add_custom_property(self, property_name: str):
        """Aggiunge una proprietà personalizzata."""
        if property_name not in self.config.custom_properties:
            self.config.custom_properties.append(property_name)
            logger.info(f"Aggiunta proprietà personalizzata: {property_name}")
    
    def get_stage_mapping(self, stage_id: str) -> Optional[str]:
        """Ottiene il nome visualizzato per una fase."""
        for stage in self.config.stage_mappings:
            if stage.stage_id == stage_id:
                return stage.display_name
        return None
    
    def get_stage_order(self, stage_id: str) -> Optional[int]:
        """Ottiene l'ordine di una fase."""
        for stage in self.config.stage_mappings:
            if stage.stage_id == stage_id:
                return stage.order
        return None
    
    def is_final_stage(self, stage_id: str) -> bool:
        """Verifica se una fase è finale."""
        for stage in self.config.stage_mappings:
            if stage.stage_id == stage_id:
                return stage.is_final
        return False
    
    def get_required_properties(self) -> List[str]:
        """Ottiene le proprietà richieste."""
        return self.config.required_properties
    
    def get_privacy_fields(self) -> List[str]:
        """Ottiene i campi che richiedono privacy."""
        return self.config.privacy_fields
    
    def get_data_structure(self) -> DataStructureConfig:
        """Ottiene la configurazione della struttura dati."""
        return self.config.data_structure
    
    def get_all_stage_ids(self) -> List[str]:
        """Ottiene tutti gli ID delle fasi."""
        return [stage.stage_id for stage in self.config.stage_mappings]
    
    def get_final_stages(self) -> List[str]:
        """Ottiene le fasi finali."""
        return [stage.stage_id for stage in self.config.stage_mappings if stage.is_final]
    
    def validate_config(self) -> Dict[str, Any]:
        """Valida la configurazione corrente."""
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "stage_count": len(self.config.stage_mappings),
            "custom_properties_count": len(self.config.custom_properties)
        }
        
        # Verifica che ci siano fasi definite
        if not self.config.stage_mappings:
            validation_result["valid"] = False
            validation_result["errors"].append("Nessuna fase della pipeline definita")
        
        # Verifica che non ci siano duplicati negli ordini
        orders = [stage.order for stage in self.config.stage_mappings]
        if len(orders) != len(set(orders)):
            validation_result["warnings"].append("Ordini delle fasi non univoci")
        
        # Verifica che ci sia almeno una fase finale
        final_stages = [stage for stage in self.config.stage_mappings if stage.is_final]
        if not final_stages:
            validation_result["warnings"].append("Nessuna fase finale definita")
        
        return validation_result

# Creazione istanza globale
hubspot_config_manager = HubSpotConfigManager()