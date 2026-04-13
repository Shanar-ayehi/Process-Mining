from typing import Dict, List, Any, Optional
from celery import chain, group
from app.tasks.base_task import (
    etl_task, create_task_metadata, create_task_result, 
    handle_task_error
)
from app.services.etl.data_extraction import data_extraction_service
from app.services.etl.data_transformation import data_transformation_service
from app.services.etl.data_quality import data_quality_service
from app.services.etl.privacy_governance import privacy_governance_service
from app.core.logger import get_logger
from app.core.database import load_event_log

logger = get_logger()

def _load_event_log_for_portal(portal_id: str) -> Any:
    """
    Carica l'event log per un portal_id specifico.
    
    Args:
        portal_id: ID del portale HubSpot
        
    Returns:
        DataFrame Polars con i dati dell'event log
        
    Raises:
        ValueError: Se non ci sono dati sincronizzati per questo account
    """
    df = load_event_log(portal_id)
    
    if df.is_empty():
        raise ValueError(f"Nessun dato sincronizzato per questo account (portal_id: {portal_id})")
    
    logger.info(f"Caricati {len(df)} record per portal_id: {portal_id}")
    return df

@etl_task(soft_time_limit=3600, time_limit=3660)
def extract_deals_task(self, properties_with_history: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Task per l'estrazione deal da HubSpot.
    
    Args:
        properties_with_history: Proprietà di cui estrarre la cronologia
        
    Returns:
        Dizionario con risultati estrazione
    """
    import asyncio
    from app.core.database import async_session
    from app.services.etl.data_extraction import DataExtractionService
    
    try:
        logger.info("Inizio task estrazione deal")
        
        async def _do_extract():
            async with async_session() as db:
                extraction_service = DataExtractionService(db=db)
                return await extraction_service.extract_deals_with_history(
                    properties_with_history=properties_with_history
                )
        
        # Estrai deal con cronologia
        deals_data = asyncio.run(_do_extract())
        
        result = create_task_result(
            success=True,
            data={
                'deals_count': len(deals_data),
                'deals_data': deals_data,
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
def transform_deals_task(self, extraction_results: Any, portal_id: str = "default") -> Dict[str, Any]:
    portal_id = "event_log" # FORZATO PER IL FRONTEND
    """
    Task per la trasformazione deal in event log.
    
    Args:
        extraction_results: Risultati dall'estrazione (ignorato in modalità locale)
        portal_id: ID del portale HubSpot
        
    Returns:
        Dizionario con risultati trasformazione
    """
    try:
        logger.info("🟢 MODALITÀ LOCALE ONLY: Caricamento dati Mock da file locale")
        
        import json
        from pathlib import Path
        
        # Leggi direttamente file Mock locale ignorando completamente i risultati di estrazione
        mock_file_path = Path(__file__).parent.parent / "data" / "raw" / "mock_deals.json"
        
        with open(mock_file_path, 'r', encoding='utf-8') as f:
            actual_deals = json.load(f)
        
        logger.info(f"✅ Caricati {len(actual_deals)} deal dal file Mock locale")
        
        # Passa i deal puliti al servizio di trasformazione
        event_log_df = data_transformation_service.transform_hubspot_deals_to_event_log(actual_deals)
        
        # ✅ SALVA NEL DATABASE DUCKDB (passaggio mancante che risolve errore 404 API)
        from app.core.database import save_event_log
        save_event_log(event_log_df, portal_id)
        
        logger.info(f"✅ Event log salvato correttamente nel database per portal_id: {portal_id}")
        
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
def validate_data_quality_task(self, portal_id: str) -> Dict[str, Any]:
    """
    Task per la validazione qualità dati.
    
    Args:
        portal_id: ID del portale HubSpot
        
    Returns:
        Dizionario con risultati validazione
    """
    try:
        logger.info(f"Inizio task validazione qualità dati per portal_id: {portal_id}")
        
        try:
            # Carica i dati dal database
            event_log_df = _load_event_log_for_portal(portal_id)
        except ValueError as e:
            logger.warning(f"Dati non disponibili per validazione: {e}")
            return create_task_result(
                success=True,
                data={
                    'quality_score': 0,
                    'validation_passed': False,
                    'message': 'Nessun dato sincronizzato ancora, salto validazione',
                    'metadata': create_task_metadata('validate_data_quality', quality_score=0)
                }
            )
        
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
def apply_privacy_governance_task(self, portal_id: str) -> Dict[str, Any]:
    """
    Task per l'applicazione governance privacy.
    
    Args:
        portal_id: ID del portale HubSpot
        
    Returns:
        Dizionario con risultati governance
    """
    try:
        logger.info(f"Inizio task governance privacy per portal_id: {portal_id}")
        
        try:
            # Carica i dati dal database
            event_log_df = _load_event_log_for_portal(portal_id)
        except ValueError as e:
            logger.warning(f"Dati non disponibili per privacy governance: {e}")
            return create_task_result(
                success=True,
                data={
                    'anonymized_rows': 0,
                    'gdpr_compliance_score': 0,
                    'message': 'Nessun dato sincronizzato ancora, salto governance',
                    'metadata': create_task_metadata('apply_privacy_governance', compliance_score=0)
                }
            )
        
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
def merge_sources_task(self, portal_id: str, 
                      contacts_data: Optional[List[Dict[str, Any]]] = None,
                      companies_data: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """
    Task per la fusione dati da multiple sorgenti.
    
    Args:
        portal_id: ID del portale HubSpot
        contacts_data: Dati contatti (opzionale)
        companies_data: Dati aziende (opzionale)
        
    Returns:
        Dizionario con risultati fusione
    """
    try:
        logger.info(f"Inizio task fusione sorgenti per portal_id: {portal_id}")
        
        try:
            # Carica i dati dal database
            event_log_df = _load_event_log_for_portal(portal_id)
        except ValueError as e:
            logger.warning(f"Dati non disponibili per merge sorgenti: {e}")
            return create_task_result(
                success=True,
                data={
                    'merged_rows': 0,
                    'merged_columns': 0,
                    'message': 'Nessun dato sincronizzato ancora, salto merge',
                    'metadata': create_task_metadata('merge_sources', merged_rows=0)
                }
            )
        
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

@etl_task(soft_time_limit=3600, time_limit=3660)
def run_full_etl_pipeline(self, portal_id: str = "default",
                         properties_with_history: Optional[List[str]] = None,
                         include_contacts: bool = False,
                         include_companies: bool = False) -> Dict[str, Any]:
    """
    Task orchestratore per l'intera pipeline ETL.
    
    Args:
        portal_id: ID del portale HubSpot
        properties_with_history: Proprietà di cui estrarre la cronologia
        include_contacts: Se includere l'estrazione contatti
        include_companies: Se includere l'estrazione aziende
        
    Returns:
        Dizionario con risultati pipeline
    """
    try:
        print(f"🚀🚀🚀 TASK PIPELINE ETL AVVIATO PER PORTALE: {portal_id} 🚀🚀🚀")
        logger.info(f"🚀 Inizio pipeline ETL completa per portal_id: {portal_id}")
        
        # 🟢 MODALITÀ LOCALE ONLY: Nessuna estrazione API HubSpot
        # Bypass completo estrazione remota, utilizzo direttamente file Mock locale
        full_pipeline = chain(
            transform_deals_task.s(None, portal_id),
            validate_data_quality_task.si(portal_id=portal_id),
            apply_privacy_governance_task.si(portal_id=portal_id),
            merge_sources_task.si(portal_id=portal_id)
        )
        
        # Esegui pipeline
        result = full_pipeline.apply_async()
        
        result_data = create_task_metadata('full_etl_pipeline', 
                                         pipeline_id=result.id,
                                         portal_id=portal_id,
                                         include_contacts=include_contacts,
                                         include_companies=include_companies)
        
        print(f"✅ Pipeline ETL avviata con successo! Pipeline ID: {result.id}")
        logger.info(f"Pipeline ETL avviata: {result.id} per portal_id: {portal_id}")
        return create_task_result(success=True, data=result_data)
        
    except Exception as e:
        print(f"❌ ERRORE PIPELINE ETL: {e}")
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
        
        logger.info("Pianificazione estrazione periodica completata")
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

@etl_task()
def extract_contacts_task(self) -> Dict[str, Any]:
    """
    Task per l'estrazione contatti da HubSpot.
    
    Returns:
        Dizionario con risultati estrazione
    """
    import asyncio
    from app.core.database import async_session
    from app.services.etl.data_extraction import DataExtractionService
    
    try:
        logger.info("Inizio task estrazione contatti")
        
        async def _do_extract():
            async with async_session() as db:
                extraction_service = DataExtractionService(db=db)
                return await extraction_service.extract_contacts()
        
        # Estrai contatti
        contacts_data = asyncio.run(_do_extract())
        
        result = create_task_result(
            success=True,
            data={
                'contacts_count': len(contacts_data),
                'metadata': create_task_metadata('extract_contacts', contacts_count=len(contacts_data))
            }
        )
        
        logger.info(f"Task estrazione contatti completato: {len(contacts_data)} contatti")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task estrazione contatti: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))


@etl_task()
def extract_companies_task(self) -> Dict[str, Any]:
    """
    Task per l'estrazione aziende da HubSpot.
    
    Returns:
        Dizionario con risultati estrazione
    """
    import asyncio
    from app.core.database import async_session
    from app.services.etl.data_extraction import DataExtractionService
    
    try:
        logger.info("Inizio task estrazione aziende")
        
        async def _do_extract():
            async with async_session() as db:
                extraction_service = DataExtractionService(db=db)
                return await extraction_service.extract_companies()
        
        # Estrai aziende
        companies_data = asyncio.run(_do_extract())
        
        result = create_task_result(
            success=True,
            data={
                'companies_count': len(companies_data),
                'metadata': create_task_metadata('extract_companies', companies_count=len(companies_data))
            }
        )
        
        logger.info(f"Task estrazione aziende completato: {len(companies_data)} aziende")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task estrazione aziende: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))


@etl_task()
def extract_workflows_task(self) -> Dict[str, Any]:
    """
    Task per l'estrazione workflow da HubSpot.
    
    Returns:
        Dizionario con risultati estrazione
    """
    try:
        logger.info("Inizio task estrazione workflow")
        
        # Estrai workflow
        workflows_data = data_extraction_service.extract_workflows()
        
        result = create_task_result(
            success=True,
            data={
                'workflows_count': len(workflows_data),
                'metadata': create_task_metadata('extract_workflows', workflows_count=len(workflows_data))
            }
        )
        
        logger.info(f"Task estrazione workflow completato: {len(workflows_data)} workflow")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task estrazione workflow: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))


@etl_task()
def monitor_new_files_task(self, raw_data_dir: Optional[str] = None, 
                          lookback_minutes: int = 10) -> Dict[str, Any]:
    """
    Task per il monitoraggio di nuovi file nella directory raw.
    
    Args:
        raw_data_dir: Directory da monitorare (opzionale, usa settings.raw_data_dir se None)
        lookback_minutes: Minuti di lookback per considerare un file come "nuovo"
        
    Returns:
        Dizionario con risultati monitoraggio
    """
    try:
        from pathlib import Path
        from datetime import datetime, timedelta
        from app.core.config import settings
        
        logger.info(f"Inizio monitoraggio nuovi file (lookback: {lookback_minutes} minuti)")
        
        # Determina la directory da monitorare
        if raw_data_dir:
            monitor_dir = Path(raw_data_dir)
        else:
            monitor_dir = settings.raw_data_dir
        
        if not monitor_dir.exists():
            logger.warning(f"Directory di monitoraggio non esistente: {monitor_dir}")
            return create_task_result(
                success=True,
                data={
                    'new_files_found': 0,
                    'files_processed': [],
                    'message': f'Directory {monitor_dir} non esistente'
                }
            )
        
        # Calcola il cutoff time
        cutoff_time = datetime.now() - timedelta(minutes=lookback_minutes)
        
        # Trova file modificati recentemente
        new_files = []
        for file_path in monitor_dir.glob("**/*.json"):
            file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
            if file_mtime > cutoff_time:
                new_files.append(file_path)
        
        if new_files:
            logger.info(f"Trovati {len(new_files)} nuovi file, avvio pipeline ETL")
            
            # Avvia pipeline ETL per ogni nuovo file
            # Per ora avviamo una pipeline generica, ma potremmo personalizzare
            pipeline_result = run_full_etl_pipeline.delay(
                properties_with_history=None,
                include_contacts=False,
                include_companies=False
            )
            
            result = create_task_result(
                success=True,
                data={
                    'new_files_found': len(new_files),
                    'files_processed': [str(f) for f in new_files],
                    'pipeline_id': pipeline_result.id,
                    'message': f'Pipeline ETL avviata per {len(new_files)} nuovi file'
                }
            )
        else:
            logger.info("Nessun nuovo file trovato")
            result = create_task_result(
                success=True,
                data={
                    'new_files_found': 0,
                    'files_processed': [],
                    'message': 'Nessun nuovo file trovato'
                }
            )
        
        return result
        
    except Exception as e:
        logger.error(f"Errore nel monitoraggio nuovi file: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))


# Creazione istanza globale
etl_task_instance = etl_task()
