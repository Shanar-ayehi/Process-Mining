from typing import Dict, List, Any, Optional
from celery import chain, group
from app.tasks.base_task import (
    celery_app, etl_task, create_task_metadata, create_task_result, 
    validate_task_input, handle_task_error
)
from app.services.etl.data_extraction import data_extraction_service
from app.services.etl.data_transformation import data_transformation_service
from app.services.etl.data_quality import data_quality_service
from app.services.etl.privacy_governance import privacy_governance_service
from app.core.logger import get_logger

logger = get_logger()

@etl_task()
def extract_deals_task(self, properties_with_history: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Task per l'estrazione deal da HubSpot.
    
    Args:
        properties_with_history: Proprietà di cui estrarre la cronologia
        
    Returns:
        Dizionario con risultati estrazione
    """
    try:
        logger.info("Inizio task estrazione deal")
        
        # Estrai deal con cronologia
        deals_data = data_extraction_service.extract_all_deals_with_history(
            properties_with_history=properties_with_history
        )
        
        result = create_task_result(
            success=True,
            data={
                'deals_count': len(deals_data),
                'metadata': create_task_metadata('extract_deals', deals_count=len(deals_data))
            }
        )
        
        logger.info(f"Task estrazione deal completato: {len(deals_data)} deal")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task estrazione deal: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@etl_task()
def transform_deals_task(self, deals_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Task per la trasformazione deal in event log.
    
    Args:
        deals_data: Dati deal da trasformare
        
    Returns:
        Dizionario con risultati trasformazione
    """
    try:
        logger.info("Inizio task trasformazione deal")
        
        # Trasforma deal in event log
        event_log_df = data_transformation_service.transform_hubspot_deals_to_event_log(deals_data)
        
        result = create_task_result(
            success=True,
            data={
                'events_count': len(event_log_df),
                'cases_count': len(event_log_df['case_id'].unique()) if 'case_id' in event_log_df.columns else 0,
                'metadata': create_task_metadata('transform_deals', events_count=len(event_log_df))
            }
        )
        
        logger.info(f"Task trasformazione deal completato: {len(event_log_df)} eventi")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task trasformazione deal: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@etl_task()
def validate_data_quality_task(self, event_log_df: Any) -> Dict[str, Any]:
    """
    Task per la validazione qualità dati.
    
    Args:
        event_log_df: DataFrame event log da validare
        
    Returns:
        Dizionario con risultati validazione
    """
    try:
        logger.info("Inizio task validazione qualità dati")
        
        # Validazione qualità dati
        quality_report = data_quality_service.generate_data_quality_report(event_log_df)
        
        result = create_task_result(
            success=True,
            data={
                'quality_score': quality_report.get('overall_score', 0),
                'validation_passed': quality_report.get('schema_validation', {}).get('validation_passed', False),
                'metadata': create_task_metadata('validate_data_quality', quality_score=quality_report.get('overall_score', 0))
            }
        )
        
        logger.info(f"Task validazione qualità dati completato: punteggio {quality_report.get('overall_score', 0)}")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task validazione qualità dati: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@etl_task()
def apply_privacy_governance_task(self, event_log_df: Any) -> Dict[str, Any]:
    """
    Task per l'applicazione governance privacy.
    
    Args:
        event_log_df: DataFrame event log da processare
        
    Returns:
        Dizionario con risultati governance
    """
    try:
        logger.info("Inizio task governance privacy")
        
        # Applica pseudonimizzazione
        anonymized_df = privacy_governance_service.anonymize_dataframe(event_log_df)
        
        # Validazione GDPR
        gdpr_report = privacy_governance_service.validate_gdpr_compliance(anonymized_df)
        
        result = create_task_result(
            success=True,
            data={
                'anonymized_rows': len(anonymized_df),
                'gdpr_compliance_score': gdpr_report.get('compliance_score', 0),
                'metadata': create_task_metadata('apply_privacy_governance', compliance_score=gdpr_report.get('compliance_score', 0))
            }
        )
        
        logger.info(f"Task governance privacy completato: compliance {gdpr_report.get('compliance_score', 0)}")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task governance privacy: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@etl_task()
def merge_sources_task(self, event_log_df: Any, 
                      contacts_data: Optional[List[Dict[str, Any]]] = None,
                      companies_data: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """
    Task per la fusione dati da multiple sorgenti.
    
    Args:
        event_log_df: Event log principale
        contacts_data: Dati contatti (opzionale)
        companies_data: Dati aziende (opzionale)
        
    Returns:
        Dizionario con risultati fusione
    """
    try:
        logger.info("Inizio task fusione sorgenti")
        
        # Trasforma entità se presenti
        contacts_entities = None
        companies_entities = None
        
        if contacts_data:
            contacts_entities = data_transformation_service.transform_contacts_to_entities(contacts_data)
        
        if companies_data:
            companies_entities = data_transformation_service.transform_companies_to_entities(companies_data)
        
        # Fonde i dati
        merged_df = data_transformation_service.merge_multiple_sources(
            event_log_df, contacts_entities, companies_entities
        )
        
        result = create_task_result(
            success=True,
            data={
                'merged_rows': len(merged_df),
                'merged_columns': len(merged_df.columns),
                'metadata': create_task_metadata('merge_sources', merged_rows=len(merged_df))
            }
        )
        
        logger.info(f"Task fusione sorgenti completato: {len(merged_df)} righe")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task fusione sorgenti: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@etl_task()
def run_full_etl_pipeline(self, properties_with_history: Optional[List[str]] = None,
                         include_contacts: bool = False,
                         include_companies: bool = False) -> Dict[str, Any]:
    """
    Task orchestratore per l'intera pipeline ETL.
    
    Args:
        properties_with_history: Proprietà di cui estrarre la cronologia
        include_contacts: Se includere l'estrazione contatti
        include_companies: Se includere l'estrazione aziende
        
    Returns:
        Dizionario con risultati pipeline
    """
    try:
        logger.info("Inizio pipeline ETL completa")
        
        # Costruisci pipeline base (Deal -> Transform -> Quality -> Privacy)
        pipeline_tasks = [
            extract_deals_task.s(properties_with_history=properties_with_history),
            transform_deals_task.s(),
            validate_data_quality_task.s(),
            apply_privacy_governance_task.s()
        ]
        
        # Se richiesto, estrai anche contatti e aziende in parallelo
        parallel_extraction = []
        if include_contacts:
            parallel_extraction.append(data_extraction_service.extract_contacts.s())
        
        if include_companies:
            parallel_extraction.append(data_extraction_service.extract_companies.s())
        
        # Crea pipeline completa
        if parallel_extraction:
            # Estrazione parallela + pipeline base
            full_pipeline = chain(
                group(parallel_extraction + [extract_deals_task.s(properties_with_history=properties_with_history)]),
                transform_deals_task.s(),
                validate_data_quality_task.s(),
                apply_privacy_governance_task.s(),
                merge_sources_task.s()
            )
        else:
            # Solo pipeline base
            full_pipeline = chain(*pipeline_tasks)
        
        # Esegui pipeline
        result = full_pipeline.apply_async()
        
        result_data = create_task_metadata('full_etl_pipeline', 
                                         pipeline_id=result.id,
                                         include_contacts=include_contacts,
                                         include_companies=include_companies)
        
        logger.info(f"Pipeline ETL avviata: {result.id}")
        return create_task_result(success=True, data=result_data)
        
    except Exception as e:
        logger.error(f"Errore nella pipeline ETL: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@etl_task()
def schedule_periodic_extraction(self, interval_hours: int = 24) -> Dict[str, Any]:
    """
    Task per la pianificazione di estrazioni periodiche.
    
    Args:
        interval_hours: Intervallo in ore tra le estrazioni
        
    Returns:
        Dizionario con risultati pianificazione
    """
    try:
        logger.info(f"Inizio pianificazione estrazione periodica ogni {interval_hours} ore")
        
        # Questa è una placeholder - in produzione si userebbe Celery Beat
        # per pianificare task periodici
        
        schedule_data = {
            'interval_hours': interval_hours,
            'next_run': None,  # Calcolato in base all'intervallo
            'status': 'scheduled'
        }
        
        result = create_task_metadata('schedule_periodic_extraction', **schedule_data)
        
        logger.info(f"Pianificazione estrazione periodica completata")
        return create_task_result(success=True, data=result)
        
    except Exception as e:
        logger.error(f"Errore nella pianificazione estrazione periodica: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@etl_task()
def cleanup_old_data_task(self, retention_days: int = 30) -> Dict[str, Any]:
    """
    Task per la pulizia dei dati vecchi.
    
    Args:
        retention_days: Giorni di retention per i dati
        
    Returns:
        Dizionario con risultati pulizia
    """
    try:
        logger.info(f"Inizio pulizia dati con retention {retention_days} giorni")
        
        # Applica retention policy
        stats = privacy_governance_service.apply_data_retention_policy()
        
        result = create_task_result(
            success=True,
            data={
                'files_processed': stats.get('files_processed', 0),
                'files_deleted': stats.get('files_deleted', 0),
                'size_freed_mb': stats.get('total_size_freed', 0) / (1024 * 1024),
                'metadata': create_task_metadata('cleanup_old_data', **stats)
            }
        )
        
        logger.info(f"Pulizia dati completata: {stats.get('files_deleted', 0)} file eliminati")
        return result
        
    except Exception as e:
        logger.error(f"Errore nella pulizia dati: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

# Task helper per la gestione delle pipeline
@etl_task()
def get_etl_pipeline_status(self, pipeline_id: str) -> Dict[str, Any]:
    """
    Ottiene lo stato di una pipeline ETL.
    
    Args:
        pipeline_id: ID della pipeline
        
    Returns:
        Dizionario con stato pipeline
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
        
        logger.info(f"Stato pipeline {pipeline_id}: {status_data}")
        return create_task_result(success=True, data=status_data)
        
    except Exception as e:
        logger.error(f"Errore nel recupero stato pipeline {pipeline_id}: {e}")
        return create_task_result(success=False, error=str(e))

@etl_task()
def cancel_etl_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
    """
    Cancella una pipeline ETL in esecuzione.
    
    Args:
        pipeline_id: ID della pipeline da cancellare
        
    Returns:
        Dizionario con risultati cancellazione
    """
    try:
        from celery.result import AsyncResult
        
        result = AsyncResult(pipeline_id)
        result.revoke(terminate=True)
        
        logger.info(f"Pipeline {pipeline_id} cancellata")
        return create_task_result(success=True, data={'pipeline_id': pipeline_id, 'cancelled': True})
        
    except Exception as e:
        logger.error(f"Errore nella cancellazione pipeline {pipeline_id}: {e}")
        return create_task_result(success=False, error=str(e))

# Creazione istanza globale
etl_task_instance = etl_task()