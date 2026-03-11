from typing import Dict, List, Any, Optional
from celery import chain, group
from app.tasks.base_task import (
    celery_app, dq_task, create_task_metadata, create_task_result, 
    validate_task_input, handle_task_error
)
from app.services.etl.data_quality import data_quality_service
from app.services.etl.privacy_governance import privacy_governance_service
from app.core.logger import get_logger

logger = get_logger()

@dq_task()
def validate_event_log_schema_task(self, event_log_df: Any) -> Dict[str, Any]:
    """
    Task per la validazione schema event log.
    
    Args:
        event_log_df: DataFrame event log da validare
        
    Returns:
        Dizionario con risultati validazione schema
    """
    try:
        logger.info("Inizio task validazione schema event log")
        
        validation_report = data_quality_service.validate_event_log_schema(event_log_df)
        
        result = create_task_result(
            success=True,
            data={
                'validation_passed': validation_report['validation_passed'],
                'errors_count': len(validation_report['errors']),
                'warnings_count': len(validation_report['warnings']),
                'validation_report': validation_report,
                'metadata': create_task_metadata('validate_event_log_schema', 
                                               validation_passed=validation_report['validation_passed'])
            }
        )
        
        logger.info(f"Task validazione schema completato: {'PASSATO' if validation_report['validation_passed'] else 'FALLITO'}")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task validazione schema: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@dq_task()
def validate_data_completeness_task(self, event_log_df: Any) -> Dict[str, Any]:
    """
    Task per la validazione completezza dati.
    
    Args:
        event_log_df: DataFrame event log da analizzare
        
    Returns:
        Dizionario con risultati validazione completezza
    """
    try:
        logger.info("Inizio task validazione completezza dati")
        
        completeness_report = data_quality_service.validate_data_completeness(event_log_df)
        
        result = create_task_result(
            success=True,
            data={
                'completeness_score': completeness_report['completeness_score'],
                'missing_values': completeness_report['missing_values'],
                'critical_missing': completeness_report['critical_missing'],
                'completeness_report': completeness_report,
                'metadata': create_task_metadata('validate_data_completeness', 
                                               completeness_score=completeness_report['completeness_score'])
            }
        )
        
        logger.info(f"Task validazione completezza completato: punteggio {completeness_report['completeness_score']:.2f}")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task validazione completezza: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@dq_task()
def validate_data_consistency_task(self, event_log_df: Any) -> Dict[str, Any]:
    """
    Task per la validazione consistenza dati.
    
    Args:
        event_log_df: DataFrame event log da analizzare
        
    Returns:
        Dizionario con risultati validazione consistenza
    """
    try:
        logger.info("Inizio task validazione consistenza dati")
        
        consistency_report = data_quality_service.validate_data_consistency(event_log_df)
        
        result = create_task_result(
            success=True,
            data={
                'consistency_score': consistency_report['consistency_score'],
                'duplicates_found': consistency_report['duplicates_found'],
                'timestamp_issues': consistency_report['timestamp_issues'],
                'consistency_report': consistency_report,
                'metadata': create_task_metadata('validate_data_consistency', 
                                               consistency_score=consistency_report['consistency_score'])
            }
        )
        
        logger.info(f"Task validazione consistenza completato: punteggio {consistency_report['consistency_score']:.2f}")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task validazione consistenza: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@dq_task()
def generate_data_quality_report_task(self, event_log_df: Any) -> Dict[str, Any]:
    """
    Task per la generazione report qualità dati completo.
    
    Args:
        event_log_df: DataFrame event log da analizzare
        
    Returns:
        Dizionario con risultati report qualità
    """
    try:
        logger.info("Inizio task generazione report qualità dati")
        
        quality_report = data_quality_service.generate_data_quality_report(event_log_df)
        
        result = create_task_result(
            success=True,
            data={
                'overall_score': quality_report['overall_score'],
                'schema_validation': quality_report['schema_validation'],
                'completeness_validation': quality_report['completeness_validation'],
                'consistency_validation': quality_report['consistency_validation'],
                'recommendations': quality_report['recommendations'],
                'quality_report': quality_report,
                'metadata': create_task_metadata('generate_data_quality_report', 
                                               overall_score=quality_report['overall_score'])
            }
        )
        
        logger.info(f"Task generazione report qualità completato: punteggio {quality_report['overall_score']:.2f}")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task generazione report qualità: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@dq_task()
def anonymize_dataframe_task(self, event_log_df: Any, 
                           sensitive_columns: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Task per l'anonimizzazione DataFrame.
    
    Args:
        event_log_df: DataFrame da anonimizzare
        sensitive_columns: Colonne sensibili (opzionale)
        
    Returns:
        Dizionario con risultati anonimizzazione
    """
    try:
        logger.info("Inizio task anonimizzazione DataFrame")
        
        anonymized_df = privacy_governance_service.anonymize_dataframe(
            event_log_df, sensitive_columns
        )
        
        result = create_task_result(
            success=True,
            data={
                'anonymized_rows': len(anonymized_df),
                'anonymized_columns': len(sensitive_columns) if sensitive_columns else 'auto-detected',
                'anonymized_df': anonymized_df,  # In produzione si salverebbe altrove
                'metadata': create_task_metadata('anonymize_dataframe', 
                                               anonymized_rows=len(anonymized_df))
            }
        )
        
        logger.info(f"Task anonimizzazione completato: {len(anonymized_df)} righe anonimizzate")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task anonimizzazione: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@dq_task()
def validate_gdpr_compliance_task(self, event_log_df: Any) -> Dict[str, Any]:
    """
    Task per la validazione compliance GDPR.
    
    Args:
        event_log_df: DataFrame da validare
        
    Returns:
        Dizionario con risultati validazione GDPR
    """
    try:
        logger.info("Inizio task validazione compliance GDPR")
        
        gdpr_report = privacy_governance_service.validate_gdpr_compliance(event_log_df)
        
        result = create_task_result(
            success=True,
            data={
                'compliance_score': gdpr_report['compliance_score'],
                'sensitive_columns_found': gdpr_report['sensitive_columns_found'],
                'pseudonymization_status': gdpr_report['pseudonymization_status'],
                'issues': gdpr_report['issues'],
                'gdpr_report': gdpr_report,
                'metadata': create_task_metadata('validate_gdpr_compliance', 
                                               compliance_score=gdpr_report['compliance_score'])
            }
        )
        
        logger.info(f"Task validazione GDPR completato: punteggio {gdpr_report['compliance_score']:.2f}")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task validazione GDPR: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@dq_task()
def apply_data_retention_task(self, data_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Task per l'applicazione policy retention dati.
    
    Args:
        data_dir: Directory su cui applicare retention (opzionale)
        
    Returns:
        Dizionario con risultati retention
    """
    try:
        logger.info("Inizio task applicazione retention dati")
        
        stats = privacy_governance_service.apply_data_retention_policy(data_dir)
        
        result = create_task_result(
            success=True,
            data={
                'files_processed': stats['files_processed'],
                'files_deleted': stats['files_deleted'],
                'size_freed_mb': stats['total_size_freed'] / (1024 * 1024),
                'retention_stats': stats,
                'metadata': create_task_metadata('apply_data_retention', **stats)
            }
        )
        
        logger.info(f"Task retention completato: {stats['files_deleted']} file eliminati")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task retention: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@dq_task()
def generate_privacy_report_task(self) -> Dict[str, Any]:
    """
    Task per la generazione report privacy.
    
    Returns:
        Dizionario con risultati report privacy
    """
    try:
        logger.info("Inizio task generazione report privacy")
        
        privacy_report = privacy_governance_service.generate_privacy_report()
        
        result = create_task_result(
            success=True,
            data={
                'privacy_report': privacy_report,
                'pseudonymization_enabled': privacy_report['pseudonymization_enabled'],
                'data_retention_days': privacy_report['data_retention_days'],
                'directories_monitored': privacy_report['directories_monitored'],
                'metadata': create_task_metadata('generate_privacy_report')
            }
        )
        
        logger.info("Task generazione report privacy completato")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task generazione report privacy: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@dq_task()
def run_full_data_quality_pipeline_task(self, event_log_df: Any) -> Dict[str, Any]:
    """
    Task orchestratore per l'intera pipeline data quality.
    
    Args:
        event_log_df: DataFrame event log da analizzare
        
    Returns:
        Dizionario con risultati pipeline qualità
    """
    try:
        logger.info("Inizio pipeline data quality completa")
        
        # Esegui tutti i controlli qualità
        schema_result = data_quality_service.validate_event_log_schema(event_log_df)
        completeness_result = data_quality_service.validate_data_completeness(event_log_df)
        consistency_result = data_quality_service.validate_data_consistency(event_log_df)
        gdpr_result = privacy_governance_service.validate_gdpr_compliance(event_log_df)
        
        # Calcola punteggio complessivo
        overall_score = (
            (1.0 if schema_result['validation_passed'] else 0.0) +
            completeness_result['completeness_score'] +
            consistency_result['consistency_score'] +
            gdpr_result['compliance_score']
        ) / 4
        
        pipeline_results = {
            'schema_validation': schema_result,
            'completeness_validation': completeness_result,
            'consistency_validation': consistency_result,
            'gdpr_validation': gdpr_result,
            'overall_score': overall_score,
            'pipeline_timestamp': create_task_metadata('full_data_quality_pipeline')['timestamp']
        }
        
        result = create_task_result(
            success=True,
            data={
                'pipeline_results': pipeline_results,
                'overall_score': overall_score,
                'quality_issues': {
                    'schema_errors': len(schema_result['errors']),
                    'completeness_issues': len(completeness_result['critical_missing']),
                    'consistency_issues': len(consistency_result['inconsistencies']),
                    'gdpr_issues': len(gdpr_result['issues'])
                },
                'metadata': create_task_metadata('full_data_quality_pipeline', overall_score=overall_score)
            }
        )
        
        logger.info(f"Pipeline data quality completata: punteggio {overall_score:.2f}")
        return result
        
    except Exception as e:
        logger.error(f"Errore nella pipeline data quality: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@dq_task()
def audit_data_access_task(self, operation: str, 
                          user_id: Optional[str] = None,
                          data_description: Optional[str] = None,
                          sensitive_data_accessed: bool = False) -> Dict[str, Any]:
    """
    Task per l'audit accesso dati.
    
    Args:
        operation: Tipo di operazione
        user_id: ID utente (opzionale)
        data_description: Descrizione dati (opzionale)
        sensitive_data_accessed: Se sono stati accessati dati sensibili
        
    Returns:
        Dizionario con risultati audit
    """
    try:
        logger.info(f"Inizio task audit accesso dati: {operation}")
        
        privacy_governance_service.audit_data_access(
            operation=operation,
            user_id=user_id,
            data_description=data_description,
            sensitive_data_accessed=sensitive_data_accessed
        )
        
        audit_entry = {
            'operation': operation,
            'user_id': user_id,
            'data_description': data_description,
            'sensitive_data_accessed': sensitive_data_accessed,
            'timestamp': create_task_metadata('audit_data_access')['timestamp']
        }
        
        result = create_task_result(
            success=True,
            data={
                'audit_entry': audit_entry,
                'metadata': create_task_metadata('audit_data_access', operation=operation)
            }
        )
        
        logger.info(f"Task audit accesso dati completato: {operation}")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task audit accesso dati: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@dq_task()
def cleanup_data_quality_logs_task(self, max_age_days: int = 30) -> Dict[str, Any]:
    """
    Task per la pulizia log data quality.
    
    Args:
        max_age_days: Età massima log in giorni
        
    Returns:
        Dizionario con risultati pulizia
    """
    try:
        logger.info(f"Inizio task pulizia log data quality (età massima: {max_age_days} giorni)")
        
        # Questa è una placeholder - in produzione si implementerebbe
        # la pulizia effettiva dei log vecchi
        
        cleanup_stats = {
            'max_age_days': max_age_days,
            'logs_processed': 0,
            'logs_deleted': 0,
            'size_freed_mb': 0
        }
        
        result = create_task_result(
            success=True,
            data={
                'cleanup_stats': cleanup_stats,
                'metadata': create_task_metadata('cleanup_data_quality_logs', **cleanup_stats)
            }
        )
        
        logger.info("Task pulizia log data quality completato")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task pulizia log data quality: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

# Task helper per la gestione delle pipeline qualità
@dq_task()
def get_data_quality_status(self, pipeline_id: str) -> Dict[str, Any]:
    """
    Ottiene lo stato di una pipeline data quality.
    
    Args:
        pipeline_id: ID della pipeline
        
    Returns:
        Dizionario con stato pipeline qualità
    """
    try:
        from celery.result import AsyncResult
        
        result = AsyncResult(pipeline_id)
        
        status_data = {
            'pipeline_id': pipeline_id,
            'status': result.status,
            'ready': result.ready(),
            'successful': result.successful() if result.ready() else None,
            'result': result.result if result.ready() else None
        }
        
        logger.info(f"Stato pipeline qualità {pipeline_id}: {status_data}")
        return create_task_result(success=True, data=status_data)
        
    except Exception as e:
        logger.error(f"Errore nel recupero stato pipeline qualità {pipeline_id}: {e}")
        return create_task_result(success=False, error=str(e))

@dq_task()
def cancel_data_quality_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
    """
    Cancella una pipeline data quality in esecuzione.
    
    Args:
        pipeline_id: ID della pipeline da cancellare
        
    Returns:
        Dizionario con risultati cancellazione
    """
    try:
        from celery.result import AsyncResult
        
        result = AsyncResult(pipeline_id)
        result.revoke(terminate=True)
        
        logger.info(f"Pipeline qualità {pipeline_id} cancellata")
        return create_task_result(success=True, data={'pipeline_id': pipeline_id, 'cancelled': True})
        
    except Exception as e:
        logger.error(f"Errore nella cancellazione pipeline qualità {pipeline_id}: {e}")
        return create_task_result(success=False, error=str(e))

# Creazione istanza globale
dq_task_instance = dq_task()