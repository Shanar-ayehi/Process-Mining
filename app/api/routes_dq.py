from fastapi import APIRouter, HTTPException
from datetime import datetime

from app.tasks.dq_task import (
    validate_event_log_schema_task, validate_data_completeness_task,
    validate_data_consistency_task, generate_data_quality_report_task,
    anonymize_dataframe_task, validate_gdpr_compliance_task,
    apply_data_retention_task, generate_privacy_report_task,
    run_full_data_quality_pipeline_task, audit_data_access_task,
    cleanup_data_quality_logs_task
)
from app.core.logger import get_logger
from app.api.schemas import (
    ValidationRequestSchema, PrivacyRequestSchema, RetentionRequestSchema, AuditRequestSchema
)

logger = get_logger()

router = APIRouter(prefix="/data-quality", tags=["Data Quality"])

# Schema validation endpoints
@router.post("/validate/schema")
async def validate_event_log_schema(request: ValidationRequestSchema):
    """
    Valida lo schema dell'event log.
    """
    try:
        logger.info("Richiesta validazione schema event log")
        
        task = validate_event_log_schema_task.delay(
            event_log_data=request.event_log_data
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "validation_type": "schema",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore validazione schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/validate/completeness")
async def validate_data_completeness(request: ValidationRequestSchema):
    """
    Valida la completezza dei dati.
    """
    try:
        logger.info("Richiesta validazione completezza dati")
        
        task = validate_data_completeness_task.delay(
            event_log_data=request.event_log_data
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "validation_type": "completeness",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore validazione completezza: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/validate/consistency")
async def validate_data_consistency(request: ValidationRequestSchema):
    """
    Valida la consistenza dei dati.
    """
    try:
        logger.info("Richiesta validazione consistenza dati")
        
        task = validate_data_consistency_task.delay(
            event_log_data=request.event_log_data
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "validation_type": "consistency",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore validazione consistenza: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Privacy and GDPR endpoints
@router.post("/privacy/anonymize")
async def anonymize_dataframe(request: PrivacyRequestSchema):
    """
    Anonimizza il DataFrame.
    """
    try:
        logger.info("Richiesta anonimizzazione DataFrame")
        
        task = anonymize_dataframe_task.delay(
            event_log_data=request.event_log_data,
            sensitive_columns=request.sensitive_columns
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "anonymization_type": "pseudonymization",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore anonimizzazione: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/privacy/gdpr-compliance")
async def validate_gdpr_compliance(request: PrivacyRequestSchema):
    """
    Valida la compliance GDPR.
    """
    try:
        logger.info("Richiesta validazione compliance GDPR")
        
        task = validate_gdpr_compliance_task.delay(
            event_log_data=request.event_log_data
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "validation_type": "gdpr_compliance",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore validazione GDPR: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/privacy/retention")
async def apply_data_retention(request: RetentionRequestSchema):
    """
    Applica la policy di retention dati.
    """
    try:
        logger.info(f"Richiesta retention dati: {request.retention_days} giorni")
        
        task = apply_data_retention_task.delay(
            data_dir=request.data_dir,
            retention_days=request.retention_days
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "retention_days": request.retention_days,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore retention dati: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/privacy/report")
async def generate_privacy_report():
    """
    Genera report privacy.
    """
    try:
        logger.info("Richiesta generazione report privacy")
        
        task = generate_privacy_report_task.delay()
        
        return {
            "task_id": task.id,
            "status": "started",
            "report_type": "privacy",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore generazione report privacy: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Data quality pipeline endpoints
@router.post("/pipeline/full")
async def run_full_data_quality_pipeline(request: ValidationRequestSchema):
    """
    Esegue pipeline qualità dati completa.
    """
    try:
        logger.info("Richiesta pipeline qualità dati completa")
        
        task = run_full_data_quality_pipeline_task.delay(
            event_log_data=request.event_log_data
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "pipeline_type": "full_data_quality",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore pipeline qualità dati: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/report/generate")
async def generate_data_quality_report(request: ValidationRequestSchema):
    """
    Genera report qualità dati completo.
    """
    try:
        logger.info("Richiesta generazione report qualità dati")
        
        task = generate_data_quality_report_task.delay(
            event_log_data=request.event_log_data
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "report_type": "data_quality",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore generazione report qualità: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Audit endpoints
@router.post("/audit/access")
async def audit_data_access(request: AuditRequestSchema):
    """
    Registra audit accesso dati.
    """
    try:
        logger.info(f"Richiesta audit accesso: {request.operation}")
        
        task = audit_data_access_task.delay(
            operation=request.operation,
            user_id=request.user_id,
            data_description=request.data_description,
            sensitive_data_accessed=request.sensitive_data_accessed
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "audit_operation": request.operation,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore audit accesso: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/audit/cleanup-logs")
async def cleanup_audit_logs(retention_days: int = 30):
    """
    Pulisce log audit vecchi.
    """
    try:
        logger.info(f"Richiesta pulizia log audit: {retention_days} giorni")
        
        task = cleanup_data_quality_logs_task.delay(
            max_age_days=retention_days
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "retention_days": retention_days,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore pulizia log audit: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Direct endpoints (sincroni)
@router.get("/summary", status_code=204)
async def get_data_quality_summary():
    """
    Ottiene un riepilogo qualità dati sincrono.
    NOTA: Attualmente non implementato, restituisce 204 se non ci sono dati pronti.
    """
    logger.info("Richiesta riepilogo qualità dati sincrono - Dati non disponibili o implementazione assente.")
    return

@router.get("/issues/top", status_code=204)
async def get_top_quality_issues():
    """
    Ottiene i principali problemi di qualità.
    NOTA: Attualmente non implementato, restituisce 204 se non ci sono dati pronti.
    """
    logger.info("Richiesta principali problemi qualità - Dati non disponibili o implementazione assente.")
    return

@router.get("/compliance/status", status_code=204)
async def get_compliance_status():
    """
    Ottiene lo stato compliance GDPR.
    NOTA: Attualmente non implementato, restituisce 204 se non ci sono dati pronti.
    """
    logger.info("Richiesta stato compliance GDPR - Dati non disponibili o implementazione assente.")
    return

# Health check endpoint
@router.get("/health")
async def data_quality_health_check():
    """
    Health check per il servizio data quality.
    """
    try:
        logger.info("Health check data quality")
        
        health_status = {
            "status": "healthy",
            "services": {
                "schema_validation": "available",
                "completeness_validation": "available",
                "consistency_validation": "available",
                "privacy_governance": "available",
                "gdpr_compliance": "available"
            },
            "dependencies": {
                "pandera": "available",
                "polars": "available",
                "privacy_manager": "available"
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return health_status
        
    except Exception as e:
        logger.error(f"Errore health check data quality: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Task management endpoints
@router.get("/tasks/{task_id}/status")
async def get_task_status(task_id: str):
    """
    Ottiene lo stato di un task data quality.
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
        
        logger.info(f"Stato task data quality {task_id}: {status_data}")
        return status_data
        
    except Exception as e:
        logger.error(f"Errore stato task data quality {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str):
    """
    Cancella un task data quality.
    """
    try:
        from celery.result import AsyncResult
        
        result = AsyncResult(task_id)
        result.revoke(terminate=True)
        
        logger.info(f"Task data quality {task_id} cancellato")
        return {"task_id": task_id, "cancelled": True, "timestamp": datetime.now().isoformat()}
        
    except Exception as e:
        logger.error(f"Errore cancellazione task data quality {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Configuration endpoints
@router.get("/config/validation", status_code=204)
async def get_validation_config():
    """
    Ottiene la configurazione validazione.
    NOTA: Attualmente non implementato, restituisce 204 se non ci sono dati pronti.
    """
    logger.info("Richiesta configurazione validazione - Dati non disponibili o implementazione assente.")
    return

@router.get("/config/privacy", status_code=204)
async def get_privacy_config():
    """
    Ottiene la configurazione privacy.
    NOTA: Attualmente non implementato, restituisce 204 se non ci sono dati pronti.
    """
    logger.info("Richiesta configurazione privacy - Dati non disponibili o implementazione assente.")
    return