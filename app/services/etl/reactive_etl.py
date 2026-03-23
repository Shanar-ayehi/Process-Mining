"""
Sistema ETL Reattivo per Process Mining.
Questo modulo implementa un sistema ETL che si attiva automaticamente
quando ci sono nuovi dati disponibili, senza bisogno di setup iniziale.
"""

import asyncio
import time
from typing import Dict, List, Any, Optional, Union, Callable
from datetime import datetime, timedelta
from pathlib import Path
import json
import threading
from dataclasses import dataclass
from enum import Enum

from app.core.logger import get_logger
from app.core.config import settings
from app.services.etl.data_extraction import data_extraction_service
from app.services.etl.data_transformation import data_transformation_service
from app.services.etl.data_quality import data_quality_service
from app.services.etl.privacy_governance import privacy_governance_service
from app.services.data_service import data_repository
from app.core.bootstrap import bootstrap_manager
from app.tasks.etl_task import (
    extract_deals, transform_deals, 
    validate_data_quality, apply_privacy_governance
)

logger = get_logger()

class ETLStatus(Enum):
    """Stati possibili del processo ETL."""
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"

@dataclass
class ETLJob:
    """Rappresenta un job ETL."""
    job_id: str
    name: str
    status: ETLStatus
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    result: Optional[Dict[str, Any]]
    error: Optional[str]
    retry_count: int = 0
    max_retries: int = 3

class ReactiveETLManager:
    """Gestore del sistema ETL reattivo."""
    
    def __init__(self):
        self.jobs: Dict[str, ETLJob] = {}
        self.is_running = False
        self.monitoring_task = None
        self.extraction_task = None
        self.transformation_task = None
        self.quality_task = None
        self.privacy_task = None
        
        # Configurazioni
        self.extraction_interval = 3600  # 1 ora in secondi
        self.monitoring_interval = 300   # 5 minuti in secondi
        self.auto_retry = True
        self.max_concurrent_jobs = 3
        
        # Callbacks per eventi
        self.on_job_start: Optional[Callable] = None
        self.on_job_complete: Optional[Callable] = None
        self.on_job_fail: Optional[Callable] = None
        
    async def start_reactive_etl(self):
        """Avvia il sistema ETL reattivo."""
        logger.info("🚀 Avvio sistema ETL reattivo")
        
        self.is_running = True
        
        # Avvia i task asincroni
        tasks = [
            self._monitoring_loop(),
            self._extraction_loop(),
            self._transformation_loop(),
            self._quality_loop(),
            self._privacy_loop()
        ]
        
        # Esegui tutti i task in parallelo
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def stop_reactive_etl(self):
        """Ferma il sistema ETL reattivo."""
        logger.info("🛑 Arresto sistema ETL reattivo")
        
        self.is_running = False
        
        # Attendi la fine dei task
        await asyncio.sleep(2)
    
    async def _monitoring_loop(self):
        """Loop di monitoraggio per rilevare nuovi dati."""
        while self.is_running:
            try:
                await self._check_for_new_data()
                await asyncio.sleep(self.monitoring_interval)
            except Exception as e:
                logger.error(f"Errore nel monitoring loop: {e}")
                await asyncio.sleep(60)  # Attesa ridotta in caso di errore
    
    async def _extraction_loop(self):
        """Loop per l'estrazione dati usando Celery."""
        while self.is_running:
            try:
                # Avvia task Celery per l'estrazione
                task = extract_deals.delay()
                logger.info(f"Task estrazione avviato: {task.id}")
                
                # Attendi un po' prima del prossimo ciclo
                await asyncio.sleep(self.extraction_interval)
            except Exception as e:
                logger.error(f"Errore nell'extraction loop: {e}")
                await asyncio.sleep(300)  # Attesa di 5 minuti in caso di errore

    async def _transformation_loop(self):
        """Loop per la trasformazione dati usando Celery."""
        while self.is_running:
            try:
                # Avvia task Celery per la trasformazione
                task = transform_deals.delay()
                logger.info(f"Task trasformazione avviato: {task.id}")
                
                # Attendi un po' prima del prossimo ciclo
                await asyncio.sleep(60)  # Controllo ogni minuto
            except Exception as e:
                logger.error(f"Errore nel transformation loop: {e}")
                await asyncio.sleep(120)

    async def _quality_loop(self):
        """Loop per il controllo qualità dati usando Celery."""
        while self.is_running:
            try:
                # Avvia task Celery per il controllo qualità
                task = validate_data_quality.delay()
                logger.info(f"Task controllo qualità avviato: {task.id}")
                
                # Attendi un po' prima del prossimo ciclo
                await asyncio.sleep(300)  # Controllo ogni 5 minuti
            except Exception as e:
                logger.error(f"Errore nel quality loop: {e}")
                await asyncio.sleep(600)  # Attesa di 10 minuti in caso di errore

    async def _privacy_loop(self):
        """Loop per la governance privacy usando Celery."""
        while self.is_running:
            try:
                # Avvia task Celery per la governance privacy
                task = apply_privacy_governance.delay()
                logger.info(f"Task governance privacy avviato: {task.id}")
                
                # Attendi un po' prima del prossimo ciclo
                await asyncio.sleep(3600)  # Controllo ogni ora
            except Exception as e:
                logger.error(f"Errore nel privacy loop: {e}")
                await asyncio.sleep(1800)  # Attesa di 30 minuti in caso di errore
    
    async def _check_for_new_data(self):
        """Controlla se ci sono nuovi dati disponibili."""
        try:
            # Controlla se ci sono file nuovi nella directory raw
            raw_dir = settings.raw_data_dir
            if not raw_dir.exists():
                return
            
            # Controlla i file più recenti
            recent_files = []
            current_time = datetime.now()
            
            for file_path in raw_dir.glob("**/*.json"):
                file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                if current_time - file_time < timedelta(minutes=10):  # File degli ultimi 10 minuti
                    recent_files.append(file_path)
            
            if recent_files:
                logger.info(f"📁 Trovati {len(recent_files)} file recenti, attivazione ETL")
                await self._trigger_etl_pipeline(recent_files)
            
        except Exception as e:
            logger.error(f"Errore nel controllo nuovi dati: {e}")
    
    async def _trigger_etl_pipeline(self, new_files: List[Path]):
        """Attiva la pipeline ETL per i nuovi file."""
        try:
            # Crea un job ETL
            job_id = f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            job = ETLJob(
                job_id=job_id,
                name="Pipeline ETL Completa",
                status=ETLStatus.RUNNING,
                start_time=datetime.now(),
                end_time=None,
                result=None,
                error=None
            )
            
            self.jobs[job_id] = job
            
            if self.on_job_start:
                await self.on_job_start(job)
            
            # Esegui la pipeline ETL
            result = await self._execute_etl_pipeline(new_files)
            
            # Aggiorna lo stato del job
            job.status = ETLStatus.COMPLETED
            job.end_time = datetime.now()
            job.result = result
            
            if self.on_job_complete:
                await self.on_job_complete(job)
            
            logger.info(f"✅ Pipeline ETL {job_id} completata con successo")
            
        except Exception as e:
            logger.error(f"❌ Pipeline ETL fallita: {e}")
            job.status = ETLStatus.FAILED
            job.error = str(e)
            
            if self.on_job_fail:
                await self.on_job_fail(job)
    
    async def _execute_etl_pipeline(self, new_files: List[Path]) -> Dict[str, Any]:
        """Esegue la pipeline ETL completa."""
        pipeline_result = {
            'timestamp': datetime.now().isoformat(),
            'steps': [],
            'success': True,
            'errors': [],
            'data_processed': 0
        }
        
        try:
            # Step 1: Estrazione dati
            logger.info("📥 Inizio estrazione dati")
            extraction_result = await self._extract_data_from_files(new_files)
            pipeline_result['steps'].append('extraction')
            pipeline_result['data_processed'] += extraction_result.get('deals_count', 0)
            
            # Step 2: Trasformazione dati
            logger.info("🔄 Inizio trasformazione dati")
            transformation_result = await self._transform_data(extraction_result.get('deals_data', []))
            pipeline_result['steps'].append('transformation')
            
            # Step 3: Controllo qualità
            logger.info("🔍 Inizio controllo qualità dati")
            quality_result = await self._check_data_quality(transformation_result.get('event_log_df'))
            pipeline_result['steps'].append('quality_check')
            
            # Step 4: Privacy governance
            logger.info("🔒 Inizio governance privacy")
            privacy_result = await self._apply_privacy_governance(transformation_result.get('event_log_df'))
            pipeline_result['steps'].append('privacy_governance')
            
            # Step 5: Salvataggio risultati
            logger.info("💾 Salvataggio risultati")
            await self._save_pipeline_results(pipeline_result)
            
            logger.info("✅ Pipeline ETL completata con successo")
            return pipeline_result
            
        except Exception as e:
            pipeline_result['success'] = False
            pipeline_result['errors'].append(str(e))
            raise
    
    async def _extract_data_from_files(self, file_paths: List[Path]) -> Dict[str, Any]:
        """Estrae dati dai file JSON usando il Data Service Layer."""
        try:
            all_deals = []
            
            for file_path in file_paths:
                with open(file_path, 'r', encoding='utf-8') as f:
                    deals_data = json.load(f)
                    all_deals.extend(deals_data)
            
            # Salva i dati estratti nel database
            if all_deals:
                # Trasforma i dati in event log per salvarli
                event_log_df = data_transformation_service.transform_hubspot_deals_to_event_log(all_deals)
                if event_log_df is not None:
                    await data_repository.save_event_log(event_log_df)
            
            return {
                'deals_count': len(all_deals),
                'deals_data': all_deals,
                'files_processed': len(file_paths)
            }
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione dati: {e}")
            raise
    
    async def _transform_data(self, deals_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Trasforma i dati deal in event log."""
        try:
            if not deals_data:
                return {'event_log_df': None, 'events_count': 0}
            
            # Trasforma i deal in event log
            event_log_df = data_transformation_service.transform_hubspot_deals_to_event_log(deals_data)
            
            return {
                'event_log_df': event_log_df,
                'events_count': len(event_log_df) if event_log_df is not None else 0
            }
            
        except Exception as e:
            logger.error(f"Errore nella trasformazione dati: {e}")
            raise
    
    async def _check_data_quality(self, event_log_df) -> Dict[str, Any]:
        """Controlla la qualità dei dati."""
        try:
            if event_log_df is None:
                return {'quality_score': 0, 'validation_passed': False}
            
            # Validazione qualità dati
            quality_report = data_quality_service.generate_data_quality_report(event_log_df)
            
            return {
                'quality_score': quality_report.get('overall_score', 0),
                'validation_passed': quality_report.get('schema_validation', {}).get('validation_passed', False),
                'quality_report': quality_report
            }
            
        except Exception as e:
            logger.error(f"Errore nel controllo qualità: {e}")
            raise
    
    async def _apply_privacy_governance(self, event_log_df) -> Dict[str, Any]:
        """Applica la governance privacy."""
        try:
            if event_log_df is None:
                return {'privacy_compliance': 0}
            
            # Applica pseudonimizzazione
            anonymized_df = privacy_governance_service.anonymize_dataframe(event_log_df)
            
            # Validazione GDPR
            gdpr_report = privacy_governance_service.validate_gdpr_compliance(anonymized_df)
            
            return {
                'privacy_compliance': gdpr_report.get('compliance_score', 0),
                'anonymized_rows': len(anonymized_df),
                'gdpr_report': gdpr_report
            }
            
        except Exception as e:
            logger.error(f"Errore nella governance privacy: {e}")
            raise
    
    async def _save_pipeline_results(self, pipeline_result: Dict[str, Any]):
        """Salva i risultati della pipeline."""
        try:
            results_dir = settings.processed_data_dir / "etl_results"
            results_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"etl_result_{timestamp}.json"
            filepath = results_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(pipeline_result, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Risultati pipeline salvati in: {filepath}")
            
        except Exception as e:
            logger.error(f"Errore nel salvataggio risultati: {e}")
            raise
    
    async def _run_extraction_job(self):
        """Esegue un job di estrazione dati usando il Data Service Layer."""
        try:
            # Estrai dati da HubSpot se disponibile
            if settings.hubspot_api_key:
                logger.info("📡 Estrazione dati da HubSpot")
                deals_data = await data_extraction_service.extract_all_deals_with_history()
                
                # Salva i dati estratti nel database usando il Data Service Layer
                if deals_data:
                    # Trasforma i dati in event log per salvarli
                    event_log_df = data_transformation_service.transform_hubspot_deals_to_event_log(deals_data)
                    if event_log_df is not None:
                        success = await data_repository.save_event_log(event_log_df)
                        if success:
                            logger.info(f"Dati HubSpot estratti e salvati nel database: {len(deals_data)} deal")
                        else:
                            logger.warning("Errore nel salvataggio dei dati estratti nel database")
                    else:
                        logger.warning("Nessun event log generato dai dati estratti")
                else:
                    logger.info("Nessun dato estratto da HubSpot")
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione dati: {e}")
    
    async def _run_transformation_job(self):
        """Esegue un job di trasformazione dati usando il Data Service Layer."""
        try:
            # Cerca file raw da trasformare
            raw_files = list(settings.raw_data_dir.glob("*.json"))
            
            if raw_files:
                logger.info(f"🔄 Trasformazione {len(raw_files)} file raw")
                
                for file_path in raw_files:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        deals_data = json.load(f)
                    
                    # Trasforma i dati
                    event_log_df = data_transformation_service.transform_hubspot_deals_to_event_log(deals_data)
                    
                    # Salva i dati trasformati nel database usando il Data Service Layer
                    if event_log_df is not None:
                        success = await data_repository.save_event_log(event_log_df)
                        if success:
                            logger.info(f"Dati trasformati salvati nel database")
                        else:
                            logger.warning("Errore nel salvataggio dei dati trasformati nel database")
                    else:
                        logger.warning("Nessun event log generato dalla trasformazione")
            
        except Exception as e:
            logger.error(f"Errore nella trasformazione dati: {e}")
    
    async def _run_quality_job(self):
        """Esegue un job di controllo qualità usando il Data Service Layer."""
        try:
            # Recupera l'event log più recente dal database
            event_log_df = await data_repository.get_latest_event_log()
            
            if not event_log_df.is_empty():
                logger.info("🔍 Controllo qualità su event log dal database")
                
                # Controlla la qualità dei dati
                quality_report = data_quality_service.generate_data_quality_report(event_log_df)
                
                # Salva il report di qualità
                success = await data_repository.save_data_quality_report(quality_report)
                if success:
                    logger.info(f"Controllo qualità completato - punteggio: {quality_report.get('overall_score', 0)}")
                else:
                    logger.warning("Errore nel salvataggio del report di qualità")
            else:
                logger.info("Nessun event log disponibile per il controllo qualità")
            
        except Exception as e:
            logger.error(f"Errore nel controllo qualità: {e}")
    
    async def _run_privacy_job(self):
        """Esegue un job di governance privacy."""
        try:
            # Applica retention policy
            stats = privacy_governance_service.apply_data_retention_policy()
            
            logger.info(f"Governance privacy completata - {stats.get('files_deleted', 0)} file eliminati")
            
        except Exception as e:
            logger.error(f"Errore nella governance privacy: {e}")
    
    def get_job_status(self, job_id: str) -> Optional[ETLJob]:
        """Ottiene lo stato di un job specifico."""
        return self.jobs.get(job_id)
    
    def get_all_jobs(self) -> Dict[str, ETLJob]:
        """Ottiene tutti i job."""
        return self.jobs.copy()
    
    def get_running_jobs(self) -> List[ETLJob]:
        """Ottiene i job in esecuzione."""
        return [job for job in self.jobs.values() if job.status == ETLStatus.RUNNING]
    
    def get_completed_jobs(self) -> List[ETLJob]:
        """Ottiene i job completati."""
        return [job for job in self.jobs.values() if job.status == ETLStatus.COMPLETED]
    
    def get_failed_jobs(self) -> List[ETLJob]:
        """Ottiene i job falliti."""
        return [job for job in self.jobs.values() if job.status == ETLStatus.FAILED]
    
    def retry_failed_job(self, job_id: str) -> bool:
        """Riprova un job fallito."""
        job = self.jobs.get(job_id)
        if job and job.status == ETLStatus.FAILED:
            if job.retry_count < job.max_retries:
                job.retry_count += 1
                job.status = ETLStatus.RUNNING
                job.start_time = datetime.now()
                job.error = None
                logger.info(f"🔄 Riprova job {job_id} (tentativo {job.retry_count})")
                return True
            else:
                logger.warning(f"❌ Job {job_id} ha superato il numero massimo di retry")
                return False
        return False

# Creazione istanza globale
reactive_etl_manager = ReactiveETLManager()

# Funzioni helper per l'uso sincrono
def start_reactive_etl_sync():
    """Avvia il sistema ETL reattivo in modo sincrono."""
    try:
        asyncio.run(reactive_etl_manager.start_reactive_etl())
    except KeyboardInterrupt:
        logger.info("⏹️ Sistema ETL reattivo interrotto dall'utente")
    except Exception as e:
        logger.error(f"❌ Errore nel sistema ETL reattivo: {e}")

def stop_reactive_etl_sync():
    """Ferma il sistema ETL reattivo in modo sincrono."""
    asyncio.run(reactive_etl_manager.stop_reactive_etl())