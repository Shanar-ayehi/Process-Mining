from typing import Dict, Any, Optional, Union, List
from celery import Celery
from celery.app.task import Task
from celery.exceptions import Retry
from datetime import datetime
import logging
from app.core.logger import get_logger
from app.core.config import settings

logger = get_logger()

# Configurazione Celery
celery_app = Celery(
    'process_mining',
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend
)

# Configurazione Celery
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Europe/Rome',
    enable_utc=True,
    task_routes={
        'app.tasks.etl_task.*': {'queue': 'etl'},
        'app.tasks.mining_task.*': {'queue': 'mining'},
        'app.tasks.dq_task.*': {'queue': 'data_quality'},
        'app.tasks.integration_task.*': {'queue': 'integration'},
    },
    task_default_queue='default',
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1
)

class BaseTask(Task):
    """Task base con logica comune per tutti i task Celery."""
    
    def __init__(self):
        super().__init__()
        self.task_logger = None
    
    def on_success(self, retval, task_id, args, kwargs):
        """Chiamato quando il task ha successo."""
        logger.info(f"Task {task_id} completato con successo")
        self._log_task_completion(task_id, "SUCCESS", retval)
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Chiamato quando il task fallisce."""
        logger.error(f"Task {task_id} fallito: {exc}")
        self._log_task_failure(task_id, exc, einfo)
    
    def on_retry(self, exc, task_id, args, kwargs, einfo):
        """Chiamato quando il task viene ritentato."""
        logger.warning(f"Task {task_id} in retry: {exc}")
        self._log_task_retry(task_id, exc, einfo)
    
    def _log_task_completion(self, task_id: str, status: str, result: Any):
        """Logga il completamento del task."""
        log_data = {
            'task_id': task_id,
            'status': status,
            'result': str(result),
            'timestamp': datetime.now().isoformat(),
            'environment': settings.environment
        }
        
        logger.info(f"Task completion: {log_data}")
    
    def _log_task_failure(self, task_id: str, exception: Exception, exc_info: Any):
        """Logga il fallimento del task."""
        log_data = {
            'task_id': task_id,
            'status': 'FAILED',
            'exception': str(exception),
            'exc_info': str(exc_info),
            'timestamp': datetime.now().isoformat(),
            'environment': settings.environment
        }
        
        logger.error(f"Task failure: {log_data}")
    
    def _log_task_retry(self, task_id: str, exception: Exception, exc_info: Any):
        """Logga il retry del task."""
        log_data = {
            'task_id': task_id,
            'status': 'RETRY',
            'exception': str(exception),
            'exc_info': str(exc_info),
            'timestamp': datetime.now().isoformat(),
            'environment': settings.environment
        }
        
        logger.warning(f"Task retry: {log_data}")

def create_task_metadata(task_name: str, **kwargs) -> Dict[str, Any]:
    """Crea metadati standard per i task."""
    return {
        'task_name': task_name,
        'timestamp': datetime.now().isoformat(),
        'environment': settings.environment,
        'version': '1.0.0',
        **kwargs
    }

def handle_task_error(task_id: str, error: Exception, retry_count: int = 0, max_retries: int = 3):
    """
    Gestisce gli errori nei task con logica di retry.
    
    Args:
        task_id: ID del task
        error: Eccezione verificatasi
        retry_count: Contatore retry
        max_retries: Numero massimo di retry
    """
    logger.error(f"Errore nel task {task_id}: {error}")
    
    if retry_count < max_retries:
        logger.info(f"Ritentando task {task_id} (tentativo {retry_count + 1}/{max_retries})")
        raise Retry(f"Task {task_id} failed, retrying", exc=error)
    else:
        logger.error(f"Task {task_id} fallito dopo {max_retries} tentativi")
        raise error

def validate_task_input(input_data: Dict[str, Any], required_fields: List[str]) -> bool:
    """
    Valida l'input di un task.
    
    Args:
        input_data: Dati input del task
        required_fields: Campi richiesti
        
    Returns:
        True se validazione passata, False altrimenti
    """
    missing_fields = [field for field in required_fields if field not in input_data or not input_data[field]]
    
    if missing_fields:
        logger.error(f"Campi richiesti mancanti: {missing_fields}")
        return False
    
    logger.info(f"Validazione input task completata per campi: {required_fields}")
    return True

def create_task_result(success: bool, data: Optional[Any] = None, error: Optional[str] = None) -> Dict[str, Any]:
    """
    Crea il risultato standard di un task.
    
    Args:
        success: Se l'operazione è andata a buon fine
        data: Dati di output
        error: Messaggio di errore (se presente)
        
    Returns:
        Dizionario con risultato task
    """
    result = {
        'success': success,
        'timestamp': datetime.now().isoformat(),
        'environment': settings.environment
    }
    
    if success:
        result['data'] = data
    else:
        result['error'] = error
    
    return result

# Decoratori per task specifici
def etl_task(queue: str = 'etl', **celery_kwargs):
    """Decoratore per task ETL."""
    default_kwargs = {
        'bind': True,
        'base': BaseTask,
        'queue': queue,
        'retry_backoff': True,
        'retry_kwargs': {'max_retries': 3},
        'autoretry_for': (Exception,),
        'default_retry_delay': 60
    }
    default_kwargs.update(celery_kwargs)
    
    return celery_app.task(**default_kwargs)

def mining_task(queue: str = 'mining', **celery_kwargs):
    """Decoratore per task Mining."""
    default_kwargs = {
        'bind': True,
        'base': BaseTask,
        'queue': queue,
        'retry_backoff': True,
        'retry_kwargs': {'max_retries': 2},
        'autoretry_for': (Exception,),
        'default_retry_delay': 120
    }
    default_kwargs.update(celery_kwargs)
    
    return celery_app.task(**default_kwargs)

def dq_task(queue: str = 'data_quality', **celery_kwargs):
    """Decoratore per task Data Quality."""
    default_kwargs = {
        'bind': True,
        'base': BaseTask,
        'queue': queue,
        'retry_backoff': True,
        'retry_kwargs': {'max_retries': 2},
        'autoretry_for': (Exception,),
        'default_retry_delay': 30
    }
    default_kwargs.update(celery_kwargs)
    
    return celery_app.task(**default_kwargs)

def integration_task(queue: str = 'integration', **celery_kwargs):
    """Decoratore per task Integration."""
    default_kwargs = {
        'bind': True,
        'base': BaseTask,
        'queue': queue,
        'retry_backoff': True,
        'retry_kwargs': {'max_retries': 3},
        'autoretry_for': (Exception,),
        'default_retry_delay': 180
    }
    default_kwargs.update(celery_kwargs)
    
    return celery_app.task(**default_kwargs)

# Funzioni di utilità per task
def get_task_status(task_id: str) -> Dict[str, Any]:
    """Ottiene lo stato di un task Celery."""
    try:
        result = celery_app.AsyncResult(task_id)
        
        status_info = {
            'task_id': task_id,
            'status': result.status,
            'ready': result.ready(),
            'successful': result.successful() if result.ready() else None,
            'result': result.result if result.ready() else None,
            'traceback': result.traceback if result.failed() else None
        }
        
        logger.info(f"Stato task {task_id}: {status_info}")
        return status_info
        
    except Exception as e:
        logger.error(f"Errore nel recupero stato task {task_id}: {e}")
        return {'task_id': task_id, 'error': str(e)}

def revoke_task(task_id: str, terminate: bool = True) -> bool:
    """Revoca un task Celery."""
    try:
        celery_app.control.revoke(task_id, terminate=terminate)
        logger.info(f"Task {task_id} revocato con successo")
        return True
    except Exception as e:
        logger.error(f"Errore nella revoca task {task_id}: {e}")
        return False

def get_queue_stats(queue_name: str) -> Dict[str, Any]:
    """Ottiene statistiche su una coda specifica."""
    try:
        # Ottiene informazioni sulla coda
        inspect = celery_app.control.inspect()
        active_tasks = inspect.active()
        scheduled_tasks = inspect.scheduled()
        
        queue_stats = {
            'queue_name': queue_name,
            'active_tasks': len(active_tasks.get(queue_name, [])) if active_tasks else 0,
            'scheduled_tasks': len(scheduled_tasks.get(queue_name, [])) if scheduled_tasks else 0,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Statistiche coda {queue_name}: {queue_stats}")
        return queue_stats
        
    except Exception as e:
        logger.error(f"Errore nel recupero statistiche coda {queue_name}: {e}")
        return {'queue_name': queue_name, 'error': str(e)}

# Task di monitoraggio
@celery_app.task(bind=True, base=BaseTask, queue='monitoring')
def monitor_task_health(self, task_id: str) -> Dict[str, Any]:
    """Monitora lo stato di un task specifico."""
    try:
        status = get_task_status(task_id)
        
        # Logica di alert se necessario
        if status.get('status') == 'FAILURE':
            logger.warning(f"Task {task_id} in stato di fallimento")
        
        return create_task_result(True, status)
        
    except Exception as e:
        logger.error(f"Errore nel monitoraggio task {task_id}: {e}")
        return create_task_result(False, error=str(e))

@celery_app.task(bind=True, base=BaseTask, queue='monitoring')
def cleanup_completed_tasks(self, max_age_hours: int = 24) -> Dict[str, Any]:
    """Pulisce i task completati vecchi."""
    try:
        # Questa è una placeholder - in produzione si potrebbe implementare
        # una logica per pulire i risultati vecchi dal backend
        logger.info(f"Pulizia task completati con età superiore a {max_age_hours} ore")
        
        return create_task_result(True, {'cleaned_up': True})
        
    except Exception as e:
        logger.error(f"Errore nella pulizia task: {e}")
        return create_task_result(False, error=str(e))

# Creazione istanza globale
base_task = BaseTask()