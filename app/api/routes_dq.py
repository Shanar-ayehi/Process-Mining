from typing import Dict, List, Any, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from datetime import datetime
import polars as pl

from app.services.etl.data_quality import data_quality_service
from app.services.etl.privacy_governance import privacy_governance_service
from app.tasks.dq_task import (
    validate_event_log_schema_task, validate_data_completeness_task,
    validate_data_consistency_task, generate_data_quality_report_task,
    anonymize_dataframe_task, validate_gdpr_compliance_task,
    apply_data_retention_task, generate_privacy_report_task,
    run_full_data_quality_pipeline_task, audit_data_access_task,
    cleanup_data_quality_logs_task
)
from app.core.logger import get_logger

logger = get_logger()

router = APIRouter(prefix="/data-quality", tags=["Data Quality"])

# Pydantic models
class ValidationRequest(BaseModel):
    event_log_data: List[Dict[str, Any]]
    validate_schema: bool = True
    validate_completeness: bool = True
    validate_consistency: bool = True

class PrivacyRequest(BaseModel):
    event_log_data: List[Dict[str, Any]]
    sensitive_columns: Optional[List[str]] = None

class RetentionRequest(BaseModel):
    data_dir: Optional[str] = None
    retention_days: int = 30

class AuditRequest(BaseModel):
    operation: str
    user_id: Optional[str] = None
    data_description: Optional[str] = None
    sensitive_data_accessed: bool = False

# Schema validation endpoints
@router.post("/validate/schema")
async def validate_event_log_schema(request: ValidationRequest):
    """
    Valida lo schema dell'event log.
    """
    try:
        logger.info("Richiesta validazione schema event log")
        
        # Converte i dati in DataFrame Polars
        event_log_df = pl.DataFrame(request.event_log_data)
        
        task = validate_event_log_schema_task.delay(
            event_log_df=event_log_df
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
async def validate_data_completeness(request: ValidationRequest):
    """
    Valida la completezza dei dati.
    """
    try:
        logger.info("Richiesta validazione completezza dati")
        
        event_log_df = pl.DataFrame(request.event_log_data)
        
        task = validate_data_completeness_task.delay(
            event_log_df=event_log_df
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
async def validate_data_consistency(request: ValidationRequest):
    """
    Valida la consistenza dei dati.
    """
    try:
        logger.info("Richiesta validazione consistenza dati")
        
        event_log_df = pl.DataFrame(request.event_log_data)
        
        task = validate_data_consistency_task.delay(
            event_log_df=event_log_df
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
async def anonymize_dataframe(request: PrivacyRequest):
    """
    Anonimizza il DataFrame.
    """
    try:
        logger.info("Richiesta anonimizzazione DataFrame")
        
        event_log_df = pl.DataFrame(request.event_log_data)
        
        task = anonymize_dataframe_task.delay(
            event_log_df=event_log_df,
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
async def validate_gdpr_compliance(request: PrivacyRequest):
    """
    Valida la compliance GDPR.
    """
    try:
        logger.info("Richiesta validazione compliance GDPR")
        
        event_log_df = pl.DataFrame(request.event_log_data)
        
        task = validate_gdpr_compliance_task.delay(
            event_log_df=event_log_df
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
async def apply_data_retention(request: RetentionRequest):
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
async def run_full_data_quality_pipeline(request: ValidationRequest):
    """
    Esegue pipeline qualità dati completa.
    """
    try:
        logger.info("Richiesta pipeline qualità dati completa")
        
        event_log_df = pl.DataFrame(request.event_log_data)
        
        task = run_full_data_quality_pipeline_task.delay(
            event_log_df=event_log_df
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
async def generate_data_quality_report(request: ValidationRequest):
    """
    Genera report qualità dati completo.
    """
    try:
        logger.info("Richiesta generazione report qualità dati")
        
        event_log_df = pl.DataFrame(request.event_log_data)
        
        task = generate_data_quality_report_task.delay(
            event_log_df=event_log_df
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
async def audit_data_access(request: AuditRequest):
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
@router.get("/summary")
async def get_data_quality_summary():
    """
    Ottiene un riepilogo qualità dati sincrono.
    """
    try:
        logger.info("Richiesta riepilogo qualità dati sincrono")
        
        # Placeholder - in produzione si calcolerebbe il riepilogo reale
        summary = {
            "quality_summary": {
                "overall_score": 0.92,
                "schema_validations": {
                    "passed": 100,
                    "failed": 0,
                    "success_rate": 1.0
                },
                "completeness": {
                    "completeness_score": 0.95,
                    "missing_values": 15,
                    "critical_missing": 0
                },
                "consistency": {
                    "consistency_score": 0.88,
                    "duplicates_found": 5,
                    "timestamp_issues": 2
                }
            },
            "privacy_compliance": {
                "gdpr_compliance_score": 0.90,
                "pseudonymization_enabled": True,
                "sensitive_columns_found": 3
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return summary
        
    except Exception as e:
        logger.error(f"Errore riepilogo qualità dati: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/issues/top")
async def get_top_quality_issues():
    """
    Ottiene i principali problemi di qualità.
    """
    try:
        logger.info("Richiesta principali problemi qualità")
        
        # Placeholder - in produzione si identificherebbero i problemi reali
        issues = {
            "top_issues": [
                {
                    "issue_type": "missing_timestamps",
                    "severity": "medium",
                    "count": 15,
                    "affected_cases": 12
                },
                {
                    "issue_type": "duplicate_records",
                    "severity": "low",
                    "count": 5,
                    "affected_cases": 5
                },
                {
                    "issue_type": "inconsistent_data_types",
                    "severity": "low",
                    "count": 3,
                    "affected_fields": ["amount", "stage_id"]
                }
            ],
            "total_issues": 23,
            "timestamp": datetime.now().isoformat()
        }
        
        return issues
        
    except Exception as e:
        logger.error(f"Errore principali problemi qualità: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/compliance/status")
async def get_compliance_status():
    """
    Ottiene lo stato compliance GDPR.
    """
    try:
        logger.info("Richiesta stato compliance GDPR")
        
        # Placeholder - in produzione si calcolerebbe lo stato reale
        compliance = {
            "gdpr_compliance": {
                "overall_score": 0.90,
                "compliance_checks": {
                    "data_minimization": True,
                    "purpose_limitation": True,
                    "storage_limitation": True,
                    "integrity_confidentiality": True,
                    "accountability": True
                },
                "sensitive_data": {
                    "columns_found": 3,
                    "pseudonymized": 3,
                    "anonymized": 0
                }
            },
            "privacy_governance": {
                "retention_policy_applied": True,
                "audit_logs_enabled": True,
                "data_subject_rights": "configured"
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return compliance
        
    except Exception as e:
        logger.error(f"Errore stato compliance: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
@router.get("/config/validation")
async def get_validation_config():
    """
    Ottiene la configurazione validazione.
    """
    try:
        logger.info("Richiesta configurazione validazione")
        
        config = {
            "validation_config": {
                "schema_validation": {
                    "required_fields": ["case_id", "activity", "timestamp"],
                    "data_types": {
                        "case_id": "string",
                        "activity": "string", 
                        "timestamp": "datetime"
                    }
                },
                "completeness_validation": {
                    "critical_fields": ["case_id", "activity", "timestamp"],
                    "minimum_completeness": 0.95
                },
                "consistency_validation": {
                    "check_duplicates": True,
                    "check_timestamps": True,
                    "check_sequences": True
                }
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return config
        
    except Exception as e:
        logger.error(f"Errore configurazione validazione: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/config/privacy")
async def get_privacy_config():
    """
    Ottiene la configurazione privacy.
    """
    try:
        logger.info("Richiesta configurazione privacy")
        
        config = {
            "privacy_config": {
                "pseudonymization_enabled": True,
                "sensitive_patterns": [
                    "email",
                    "user",
                    "contact",
                    "name",
                    "phone",
                    "address"
                ],
                "retention_policy": {
                    "default_retention_days": 365,
                    "max_retention_days": 2555,  # 7 anni
                    "automatic_cleanup": True
                },
                "gdpr_compliance": {
                    "data_minimization": True,
                    "purpose_limitation": True,
                    "storage_limitation": True
                }
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return config
        
    except Exception as e:
        logger.error(f"Errore configurazione privacy: {e}")
        raise HTTPException(status_code=500, detail=str(e))