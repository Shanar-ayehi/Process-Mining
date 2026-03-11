from typing import Dict, List, Any, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from datetime import datetime
import polars as pl

from app.services.etl.data_extraction import data_extraction_service
from app.services.etl.data_transformation import data_transformation_service
from app.tasks.etl_task import (
    extract_deals_task, transform_deals_task, merge_sources_task,
    run_full_etl_pipeline_task, schedule_periodic_extraction_task,
    cleanup_old_data_task
)
from app.core.logger import get_logger

logger = get_logger()

router = APIRouter(prefix="/connector", tags=["Connector"])

# Pydantic models
class ExtractionRequest(BaseModel):
    properties_with_history: Optional[List[str]] = None
    include_contacts: bool = False
    include_companies: bool = False

class TransformationRequest(BaseModel):
    deals_data: List[Dict[str, Any]]
    contacts_data: Optional[List[Dict[str, Any]]] = None
    companies_data: Optional[List[Dict[str, Any]]] = None

class PipelineRequest(BaseModel):
    properties_with_history: Optional[List[str]] = None
    include_contacts: bool = False
    include_companies: bool = False
    schedule_interval: Optional[int] = None

class ScheduleRequest(BaseModel):
    interval_hours: int = 24

class CleanupRequest(BaseModel):
    retention_days: int = 30

# Extraction endpoints
@router.post("/extract/deals")
async def extract_deals(request: ExtractionRequest):
    """
    Estrae deal da HubSpot.
    """
    try:
        logger.info(f"Richiesta estrazione deal: {len(request.properties_with_history) if request.properties_with_history else 0} proprietà")
        
        task = extract_deals_task.delay(
            properties_with_history=request.properties_with_history
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "properties_count": len(request.properties_with_history) if request.properties_with_history else 0,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore estrazione deal: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/extract/contacts")
async def extract_contacts():
    """
    Estrae contatti da HubSpot.
    """
    try:
        logger.info("Richiesta estrazione contatti")
        
        task = data_extraction_service.extract_contacts.delay()
        
        return {
            "task_id": task.id,
            "status": "started",
            "entity_type": "contacts",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore estrazione contatti: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/extract/companies")
async def extract_companies():
    """
    Estrae aziende da HubSpot.
    """
    try:
        logger.info("Richiesta estrazione aziende")
        
        task = data_extraction_service.extract_companies.delay()
        
        return {
            "task_id": task.id,
            "status": "started",
            "entity_type": "companies",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore estrazione aziende: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Transformation endpoints
@router.post("/transform/deals")
async def transform_deals(request: TransformationRequest):
    """
    Trasforma deal in event log.
    """
    try:
        logger.info(f"Richiesta trasformazione deal: {len(request.deals_data)} deal")
        
        task = transform_deals_task.delay(
            deals_data=request.deals_data
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "deals_count": len(request.deals_data),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore trasformazione deal: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/transform/contacts")
async def transform_contacts(contacts_data: List[Dict[str, Any]]):
    """
    Trasforma contatti in entità.
    """
    try:
        logger.info(f"Richiesta trasformazione contatti: {len(contacts_data)} contatti")
        
        task = data_transformation_service.transform_contacts_to_entities.delay(
            contacts_data=contacts_data
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "contacts_count": len(contacts_data),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore trasformazione contatti: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/transform/companies")
async def transform_companies(companies_data: List[Dict[str, Any]]):
    """
    Trasforma aziende in entità.
    """
    try:
        logger.info(f"Richiesta trasformazione aziende: {len(companies_data)} aziende")
        
        task = data_transformation_service.transform_companies_to_entities.delay(
            companies_data=companies_data
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "companies_count": len(companies_data),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore trasformazione aziende: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/transform/merge")
async def merge_sources(request: TransformationRequest):
    """
    Fonde dati da multiple sorgenti.
    """
    try:
        logger.info("Richiesta fusione sorgenti")
        
        task = merge_sources_task.delay(
            event_log_df=None,  # Da implementare caricamento event log
            contacts_data=request.contacts_data,
            companies_data=request.companies_data
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "contacts_count": len(request.contacts_data) if request.contacts_data else 0,
            "companies_count": len(request.companies_data) if request.companies_data else 0,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore fusione sorgenti: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Pipeline endpoints
@router.post("/pipeline/full")
async def run_full_pipeline(request: PipelineRequest):
    """
    Esegue pipeline ETL completa.
    """
    try:
        logger.info("Richiesta pipeline ETL completa")
        
        task = run_full_etl_pipeline_task.delay(
            properties_with_history=request.properties_with_history,
            include_contacts=request.include_contacts,
            include_companies=request.include_companies
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "include_contacts": request.include_contacts,
            "include_companies": request.include_companies,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore pipeline ETL completa: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/pipeline/schedule")
async def schedule_extraction(request: ScheduleRequest):
    """
    Pianifica estrazione periodica.
    """
    try:
        logger.info(f"Richiesta pianificazione estrazione: ogni {request.interval_hours} ore")
        
        task = schedule_periodic_extraction_task.delay(
            interval_hours=request.interval_hours
        )
        
        return {
            "task_id": task.id,
            "status": "scheduled",
            "interval_hours": request.interval_hours,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore pianificazione estrazione: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Data management endpoints
@router.post("/data/cleanup")
async def cleanup_old_data(request: CleanupRequest):
    """
    Pulisce dati vecchi.
    """
    try:
        logger.info(f"Richiesta pulizia dati con retention {request.retention_days} giorni")
        
        task = cleanup_old_data_task.delay(
            retention_days=request.retention_days
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "retention_days": request.retention_days,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore pulizia dati: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Direct endpoints (sincroni)
@router.get("/health")
async def connector_health_check():
    """
    Health check per il servizio connector.
    """
    try:
        logger.info("Health check connector")
        
        # Verifica connessione HubSpot
        try:
            # Placeholder - in produzione si verificherebbe la connessione reale
            hubspot_connected = True
        except Exception:
            hubspot_connected = False
        
        health_status = {
            "status": "healthy" if hubspot_connected else "degraded",
            "services": {
                "hubspot_connection": "connected" if hubspot_connected else "disconnected",
                "extraction": "available",
                "transformation": "available",
                "data_quality": "available"
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return health_status
        
    except Exception as e:
        logger.error(f"Errore health check connector: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/status/last-extraction")
async def get_last_extraction_status():
    """
    Ottiene lo stato dell'ultima estrazione.
    """
    try:
        logger.info("Richiesta stato ultima estrazione")
        
        # Placeholder - in produzione si recupererebbe lo stato reale
        status = {
            "last_extraction": {
                "timestamp": "2024-01-15T10:30:00Z",
                "status": "completed",
                "deals_extracted": 1500,
                "contacts_extracted": 800,
                "companies_extracted": 200
            },
            "next_scheduled": "2024-01-16T10:30:00Z",
            "timestamp": datetime.now().isoformat()
        }
        
        return status
        
    except Exception as e:
        logger.error(f"Errore stato ultima estrazione: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/data/summary")
async def get_data_summary():
    """
    Ottiene un riepilogo dei dati.
    """
    try:
        logger.info("Richiesta riepilogo dati")
        
        # Placeholder - in produzione si calcolerebbe il riepilogo reale
        summary = {
            "data_summary": {
                "total_deals": 5000,
                "total_contacts": 3000,
                "total_companies": 800,
                "event_log_records": 25000,
                "last_updated": "2024-01-15T10:30:00Z"
            },
            "data_quality": {
                "schema_validations": 100,
                "completeness_score": 0.95,
                "consistency_score": 0.92
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return summary
        
    except Exception as e:
        logger.error(f"Errore riepilogo dati: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/properties/available")
async def get_available_properties():
    """
    Ottiene le proprietà disponibili per l'estrazione.
    """
    try:
        logger.info("Richiesta proprietà disponibili")
        
        # Placeholder - in produzione si recupererebbero le proprietà reali da HubSpot
        properties = {
            "deal_properties": [
                "dealname",
                "amount",
                "closedate", 
                "pipeline",
                "dealstage",
                "hubspot_owner_id"
            ],
            "contact_properties": [
                "email",
                "firstname",
                "lastname",
                "phone",
                "company"
            ],
            "company_properties": [
                "name",
                "domain",
                "industry",
                "annualrevenue"
            ],
            "properties_with_history": [
                "dealstage",
                "pipeline",
                "hubspot_owner_id"
            ],
            "timestamp": datetime.now().isoformat()
        }
        
        return properties
        
    except Exception as e:
        logger.error(f"Errore proprietà disponibili: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Task management endpoints
@router.get("/tasks/{task_id}/status")
async def get_task_status(task_id: str):
    """
    Ottiene lo stato di un task connector.
    """
    try:
        from celery.result import AsyncResult
        
        result = AsyncResult(task_id)
        
        status_data = {
            "task_id": task_id,
            "status": result.status,
            "ready": result.ready(),
            "successful": result.successful() if result.ready() else None,
            "result": result.result if result.ready() else None,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"Stato task connector {task_id}: {status_data}")
        return status_data
        
    except Exception as e:
        logger.error(f"Errore stato task connector {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str):
    """
    Cancella un task connector.
    """
    try:
        from celery.result import AsyncResult
        
        result = AsyncResult(task_id)
        result.revoke(terminate=True)
        
        logger.info(f"Task connector {task_id} cancellato")
        return {"task_id": task_id, "cancelled": True, "timestamp": datetime.now().isoformat()}
        
    except Exception as e:
        logger.error(f"Errore cancellazione task connector {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Configuration endpoints
@router.get("/config/hubspot")
async def get_hubspot_config():
    """
    Ottiene la configurazione HubSpot.
    """
    try:
        logger.info("Richiesta configurazione HubSpot")
        
        # Placeholder - in produzione si recupererebbe la configurazione reale
        config = {
            "hubspot_config": {
                "api_key_set": True,
                "base_url": "https://api.hubapi.com",
                "rate_limit": "100 requests/10 seconds",
                "timeout": "30 seconds"
            },
            "extraction_config": {
                "batch_size": 100,
                "max_retries": 3,
                "retry_delay": 60
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return config
        
    except Exception as e:
        logger.error(f"Errore configurazione HubSpot: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/config/hubspot")
async def update_hubspot_config(config: Dict[str, Any]):
    """
    Aggiorna la configurazione HubSpot.
    """
    try:
        logger.info("Richiesta aggiornamento configurazione HubSpot")
        
        # Placeholder - in produzione si aggiornerebbe la configurazione reale
        update_result = {
            "config_updated": True,
            "updated_fields": list(config.keys()),
            "timestamp": datetime.now().isoformat()
        }
        
        return update_result
        
    except Exception as e:
        logger.error(f"Errore aggiornamento configurazione HubSpot: {e}")
        raise HTTPException(status_code=500, detail=str(e))