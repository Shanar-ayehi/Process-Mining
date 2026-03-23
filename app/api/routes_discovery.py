from typing import Dict, List, Any, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from datetime import datetime
import json

from app.services.etl.data_discovery import auto_discovery_service
from app.core.hubspot_config import hubspot_config_manager
from app.core.logger import get_logger

logger = get_logger()

router = APIRouter(prefix="/discovery", tags=["Discovery"])

# Pydantic models
class DiscoveryRequest(BaseModel):
    sample_size: Optional[int] = None
    save_results: bool = True
    apply_config: bool = False

class ConfigUpdateRequest(BaseModel):
    stage_mappings: Optional[List[Dict[str, Any]]] = None
    data_structure: Optional[Dict[str, str]] = None
    custom_properties: Optional[List[str]] = None
    required_properties: Optional[List[str]] = None
    privacy_fields: Optional[List[str]] = None

# Discovery endpoints
@router.post("/run")
async def run_discovery(request: DiscoveryRequest):
    """
    Esegue la discovery automatica della configurazione HubSpot.
    """
    try:
        logger.info("Richiesta discovery automatica")
        
        # Esegui discovery
        results = auto_discovery_service.run_full_discovery()
        
        if not results.get('success', False):
            raise HTTPException(status_code=500, detail=f"Discovery fallita: {results.get('error', 'Errore sconosciuto')}")
        
        # Salva risultati se richiesto
        if request.save_results:
            filepath = auto_discovery_service.save_discovery_results(results)
            results['saved_to'] = str(filepath)
        
        # Applica configurazione se richiesto
        if request.apply_config:
            applied = auto_discovery_service.apply_discovery_config(results)
            results['config_applied'] = applied
        
        results['request_params'] = {
            'sample_size': request.sample_size,
            'save_results': request.save_results,
            'apply_config': request.apply_config
        }
        
        logger.info("Discovery automatica completata")
        return results
        
    except Exception as e:
        logger.error(f"Errore discovery automatica: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/config")
async def get_current_config():
    """
    Ottiene la configurazione HubSpot corrente.
    """
    try:
        config = hubspot_config_manager.config
        validation = hubspot_config_manager.validate_config()
        
        config_data = {
            "config": {
                "version": config.version,
                "stage_mappings": [stage.__dict__ for stage in config.stage_mappings],
                "data_structure": config.data_structure.__dict__,
                "custom_properties": config.custom_properties,
                "required_properties": config.required_properties,
                "privacy_fields": config.privacy_fields
            },
            "validation": validation,
            "timestamp": datetime.now().isoformat()
        }
        
        return config_data
        
    except Exception as e:
        logger.error(f"Errore recupero configurazione: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/config")
async def update_config(request: ConfigUpdateRequest):
    """
    Aggiorna la configurazione HubSpot.
    """
    try:
        logger.info("Richiesta aggiornamento configurazione")
        
        # Aggiorna stage mappings se forniti
        if request.stage_mappings:
            for stage_data in request.stage_mappings:
                hubspot_config_manager.add_stage_mapping(
                    stage_data['stage_id'],
                    stage_data['display_name'],
                    stage_data['order'],
                    stage_data.get('is_final', False)
                )
        
        # Aggiorna data structure se fornito
        if request.data_structure:
            hubspot_config_manager.update_data_structure(**request.data_structure)
        
        # Aggiorna proprietà personalizzate
        if request.custom_properties:
            for prop in request.custom_properties:
                hubspot_config_manager.add_custom_property(prop)
        
        # Aggiorna proprietà richieste
        if request.required_properties:
            # Per ora aggiungiamo, in futuro potremmo sostituire
            for prop in request.required_properties:
                if prop not in hubspot_config_manager.config.required_properties:
                    hubspot_config_manager.config.required_properties.append(prop)
        
        # Aggiorna campi privacy
        if request.privacy_fields:
            # Per ora aggiungiamo, in futuro potremmo sostituire
            for field in request.privacy_fields:
                if field not in hubspot_config_manager.config.privacy_fields:
                    hubspot_config_manager.config.privacy_fields.append(field)
        
        # Salva configurazione aggiornata
        hubspot_config_manager.save_config(hubspot_config_manager.config)
        
        validation = hubspot_config_manager.validate_config()
        
        result = {
            "success": True,
            "message": "Configurazione aggiornata con successo",
            "validation": validation,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info("Configurazione aggiornata")
        return result
        
    except Exception as e:
        logger.error(f"Errore aggiornamento configurazione: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/config/validate")
async def validate_config():
    """
    Valida la configurazione HubSpot corrente.
    """
    try:
        validation = hubspot_config_manager.validate_config()
        
        result = {
            "validation": validation,
            "timestamp": datetime.now().isoformat()
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Errore validazione configurazione: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stages")
async def get_stage_mappings():
    """
    Ottiene tutte le fasi della pipeline configurate.
    """
    try:
        stage_mappings = hubspot_config_manager.config.stage_mappings
        stages_data = []
        
        for stage in stage_mappings:
            stages_data.append({
                "stage_id": stage.stage_id,
                "display_name": stage.display_name,
                "order": stage.order,
                "is_final": stage.is_final
            })
        
        result = {
            "stages": stages_data,
            "count": len(stages_data),
            "timestamp": datetime.now().isoformat()
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Errore recupero fasi pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/properties")
async def get_properties():
    """
    Ottiene le proprietà configurate.
    """
    try:
        config = hubspot_config_manager.config
        
        properties_data = {
            "custom_properties": config.custom_properties,
            "required_properties": config.required_properties,
            "privacy_fields": config.privacy_fields,
            "data_structure": config.data_structure.__dict__,
            "timestamp": datetime.now().isoformat()
        }
        
        return properties_data
        
    except Exception as e:
        logger.error(f"Errore recupero proprietà: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/stages/add")
async def add_stage_mapping(stage_id: str, display_name: str, order: int, is_final: bool = False):
    """
    Aggiunge una nuova fase della pipeline.
    """
    try:
        logger.info(f"Aggiunta fase pipeline: {stage_id} -> {display_name}")
        
        hubspot_config_manager.add_stage_mapping(stage_id, display_name, order, is_final)
        
        # Salva configurazione
        hubspot_config_manager.save_config(hubspot_config_manager.config)
        
        result = {
            "success": True,
            "message": f"Fase {stage_id} aggiunta con successo",
            "stage": {
                "stage_id": stage_id,
                "display_name": display_name,
                "order": order,
                "is_final": is_final
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Errore aggiunta fase pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/stages/{stage_id}")
async def remove_stage_mapping(stage_id: str):
    """
    Rimuove una fase della pipeline.
    """
    try:
        logger.info(f"Rimozione fase pipeline: {stage_id}")
        
        # Filtra la fase da rimuovere
        hubspot_config_manager.config.stage_mappings = [
            stage for stage in hubspot_config_manager.config.stage_mappings
            if stage.stage_id != stage_id
        ]
        
        # Salva configurazione
        hubspot_config_manager.save_config(hubspot_config_manager.config)
        
        result = {
            "success": True,
            "message": f"Fase {stage_id} rimossa con successo",
            "removed_stage_id": stage_id,
            "timestamp": datetime.now().isoformat()
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Errore rimozione fase pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/health")
async def discovery_health_check():
    """
    Health check per il servizio discovery.
    """
    try:
        logger.info("Health check discovery")
        
        # Verifica connessione HubSpot
        try:
            stages = hubspot_config_manager.get_all_stage_ids()
            hubspot_connected = len(stages) > 0
        except Exception:
            hubspot_connected = False
        
        # Verifica configurazione
        config_valid = hubspot_config_manager.validate_config().get("valid", False)
        
        health_status = {
            "status": "healthy" if (hubspot_connected and config_valid) else "degraded",
            "services": {
                "hubspot_connection": "connected" if hubspot_connected else "disconnected",
                "configuration": "valid" if config_valid else "invalid",
                "discovery": "available",
                "auto_discovery": "available"
            },
            "config_stats": {
                "stage_count": len(hubspot_config_manager.get_all_stage_ids()),
                "custom_properties_count": len(hubspot_config_manager.config.custom_properties),
                "privacy_fields_count": len(hubspot_config_manager.config.privacy_fields)
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return health_status
        
    except Exception as e:
        logger.error(f"Errore health check discovery: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/data-structure")
async def get_data_structure():
    """
    Ottiene la struttura dati configurata.
    """
    try:
        data_structure = hubspot_config_manager.get_data_structure()
        
        structure_data = {
            "data_structure": data_structure.__dict__,
            "timestamp": datetime.now().isoformat()
        }
        
        return structure_data
        
    except Exception as e:
        logger.error(f"Errore recupero struttura dati: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/data-structure")
async def update_data_structure(data_structure: Dict[str, str]):
    """
    Aggiorna la struttura dati.
    """
    try:
        logger.info("Aggiornamento struttura dati")
        
        hubspot_config_manager.update_data_structure(**data_structure)
        
        # Salva configurazione
        hubspot_config_manager.save_config(hubspot_config_manager.config)
        
        result = {
            "success": True,
            "message": "Struttura dati aggiornata con successo",
            "data_structure": data_structure,
            "timestamp": datetime.now().isoformat()
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Errore aggiornamento struttura dati: {e}")
        raise HTTPException(status_code=500, detail=str(e))