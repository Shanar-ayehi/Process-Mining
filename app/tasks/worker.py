"""
Worker Celery per Process Mining.

Questo modulo configura e avvia il worker Celery per l'elaborazione asincrona.
"""

from app.tasks.base_task import celery_app

# Esporta celery_app per essere utilizzato dal comando celery
__all__ = ['celery_app']

# Configurazione aggiuntiva del worker
celery_app.conf.update(
    # Configurazione worker
    worker_concurrency=4,
    worker_max_tasks_per_child=1000,
    worker_disable_rate_limits=False,
    
    # Configurazione task
    task_soft_time_limit=300,  # 5 minuti
    task_time_limit=600,       # 10 minuti
    
    # Configurazione coda
    task_default_queue='default',
    task_create_missing_queues=True,
    
    # Configurazione monitoraggio
    worker_send_task_events=True,
    task_send_sent_event=True,
)

# Importa tutti i task per registrazione
from app.tasks import etl_task, mining_task, dq_task, integration_task

if __name__ == '__main__':
    celery_app.start()