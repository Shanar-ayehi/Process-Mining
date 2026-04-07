from typing import Dict, List, Any
from fastapi import APIRouter, HTTPException
from datetime import datetime

from app.services.etl.data_transformation import data_transformation_service
from app.tasks.etl_task import (
    run_full_etl_pipeline, schedule_periodic_extraction,
    cleanup_old_data_task, extract_contacts_task, extract_companies_task,
    extract_workflows_task
)
from app.core.logger import get_logger
from app.api.schemas import (
    ExtractionRequestSchema, TransformationRequestSchema, PipelineRequestSchema,
    ScheduleRequestSchema, CleanupRequestSchema
)

logger = get_logger()

router = APIRouter(prefix="/connector", tags=["Connector"])

# Extraction endpoints
@router.post("/extract/deals")
async def extract_deals(request: ExtractionRequestSchema):
    """
    Estrae deal da HubSpot.
    """
    try:
        logger.info(f"Richiesta estrazione deal: {len(request.properties_with_history) if request.properties_with_history else 0} proprietà")
        
        task = extract_deals.delay(
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
        
        task = extract_contacts_task.delay()
        
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
        
        task = extract_companies_task.delay()
        
        return {
            "task_id": task.id,
            "status": "started",
            "entity_type": "companies",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore estrazione aziende: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/workflows")
async def get_workflows():
    """
    Recupera workflow attivi da HubSpot.
    """
    try:
        logger.info("Richiesta workflow HubSpot")
        
        task = extract_workflows_task.delay()
        
        return {
            "task_id": task.id,
            "status": "started",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore recupero workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Transformation endpoints
@router.post("/transform/deals")
async def transform_deals(request: TransformationRequestSchema):
    """
    Trasforma deal in event log.
    """
    try:
        logger.info(f"Richiesta trasformazione deal: {len(request.deals_data)} deal")
        
        task = transform_deals.delay(
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
async def merge_sources(request: TransformationRequestSchema):
    """
    Fonde dati da multiple sorgenti.
    """
    try:
        logger.info("Richiesta fusione sorgenti")
        
        task = merge_sources.delay(
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
async def run_full_pipeline(request: PipelineRequestSchema):
    """
    Esegue pipeline ETL completa.
    """
    try:
        logger.info(f"Tentativo di invio task a Celery per portal_id: {request.portal_id}")
        
        task = run_full_etl_pipeline.delay(
            portal_id=request.portal_id,
            properties_with_history=request.properties_with_history,
            include_contacts=request.include_contacts,
            include_companies=request.include_companies
        )
        
        logger.info(f"Task inviato con successo a Celery con ID: {task.id}")
        
        return {
            "task_id": task.id,
            "status": "started",
            "portal_id": request.portal_id,
            "include_contacts": request.include_contacts,
            "include_companies": request.include_companies,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.exception("CRITICAL: Fallimento nell'invio del task a Celery!")
        raise HTTPException(status_code=500, detail=f"Errore di invio a Celery: {str(e)}")

@router.post("/pipeline/schedule")
async def schedule_extraction(request: ScheduleRequestSchema):
    """
    Pianifica estrazione periodica.
    """
    try:
        logger.info(f"Richiesta pianificazione estrazione: ogni {request.interval_hours} ore")
        
        task = schedule_periodic_extraction.delay(
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
async def cleanup_old_data(request: CleanupRequestSchema):
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
    logger.info("Health check connector")
    return {
        "status": "healthy",
        "services": {
            "extraction": "available",
            "transformation": "available",
        },
        "timestamp": datetime.now().isoformat()
    }

@router.get("/status/last-extraction", status_code=204)
async def get_last_extraction_status():
    """
    Ottiene lo stato dell'ultima estrazione.
    NOTA: Attualmente non implementato, restituisce 204 se non ci sono dati pronti.
    """
    logger.info("Richiesta stato ultima estrazione - Dati non disponibili o implementazione assente.")
    return

@router.get("/data/summary", status_code=204)
async def get_data_summary():
    """
    Ottiene un riepilogo dei dati.
    NOTA: Attualmente non implementato, restituisce 204 se non ci sono dati pronti.
    """
    logger.info("Richiesta riepilogo dati - Dati non disponibili o implementazione assente.")
    return

@router.get("/properties/available", status_code=204)
async def get_available_properties():
    """
    Ottiene le proprietà disponibili per l'estrazione.
    NOTA: Attualmente non implementato, restituisce 204 se non ci sono dati pronti.
    """
    logger.info("Richiesta proprietà disponibili - Dati non disponibili o implementazione assente.")
    return

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
@router.get("/config/hubspot", status_code=204)
async def get_hubspot_config():
    """
    Ottiene la configurazione HubSpot.
    NOTA: Attualmente non implementato, restituisce 204 se non ci sono dati pronti.
    """
    logger.info("Richiesta configurazione HubSpot - Dati non disponibili o implementazione assente.")
    return

@router.put("/config/hubspot", status_code=204)
async def update_hubspot_config(config: Dict[str, Any]):
    """
    Aggiorna la configurazione HubSpot.
    NOTA: Attualmente non implementato, restituisce 204.
    """
    logger.info("Richiesta aggiornamento configurazione HubSpot - non implementato.")
    return