from pydantic_settings import BaseSettings
from typing import Optional
import os
from pathlib import Path

class Settings(BaseSettings):
    """Configurazione centralizzata per l'applicazione."""
    
    # Environment
    environment: str = "development"
    
    # Paths
    base_dir: Path = Path(__file__).parent.parent
    data_dir: Path = base_dir / "data"
    logs_dir: Path = base_dir / "logs"
    raw_data_dir: Path = data_dir / "raw"
    staged_data_dir: Path = data_dir / "staged"
    processed_data_dir: Path = data_dir / "processed"
    warehouse_dir: Path = data_dir / "warehouse"
    config_path: Path = base_dir / "config" / "hubspot_schema.yaml"
    
    # Database
    db_path: str = str(data_dir / "process_mining.db")
    duckdb_memory_limit: str = "2GB"
    
    # HubSpot API
    hubspot_api_key: Optional[str] = None
    hubspot_api_base_url: str = "https://api.hubapi.com"
    hubspot_rate_limit_delay: float = 0.1  # seconds between requests
    
    # HubSpot OAuth 2.0
    hubspot_client_id: Optional[str] = None
    hubspot_client_secret: Optional[str] = None
    hubspot_redirect_uri: Optional[str] = None
    
    # Celery
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/0"
    
    # Privacy & GDPR
    email_hash_salt: str = "process_mining_salt_2024"
    data_retention_days: int = 365
    pseudonymization_enabled: bool = True
    
    # Mining
    mining_default_variant_threshold: float = 0.05  # 5% threshold for variant filtering
    conformance_checking_enabled: bool = True
    
    # Logging
    log_level: str = "INFO"
    log_rotation_size: str = "10 MB"
    log_compression: str = "zip"
    
    # Bootstrap
    auto_bootstrap: bool = True  # Abilita il bootstrap automatico
    bootstrap_on_startup: bool = True  # Esegui bootstrap all'avvio
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"

# Creazione istanza settings
settings = Settings()

# Creazione directory se non esistono
def setup_directories():
    """Crea le directory necessarie per il funzionamento dell'applicazione."""
    directories = [
        settings.data_dir,
        settings.logs_dir,
        settings.raw_data_dir,
        settings.staged_data_dir,
        settings.processed_data_dir,
        settings.warehouse_dir,
        settings.config_path.parent
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)

# Setup iniziale
setup_directories()

# Bootstrap automatico se abilitato
def bootstrap_system():
    """Esegue il bootstrap automatico del sistema."""
    if settings.auto_bootstrap:
        logger = None
        try:
            from app.core.bootstrap import run_bootstrap_sync
            
            try:
                from app.core.logger import get_logger
                logger = get_logger()
            except ImportError:
                pass
            
            if logger:
                logger.info("🚀 Avvio bootstrap automatico del sistema")
            
            result = run_bootstrap_sync()
            
            if logger:
                if result.get('success', False):
                    logger.info("✅ Bootstrap automatico completato con successo")
                else:
                    logger.warning("⚠️ Bootstrap automatico fallito, continuo con configurazione base")
                    logger.warning(f"Errori: {result.get('errors', [])}")
            
            return result
        except Exception as e:
            if logger:
                logger.error(f"❌ Errore nel bootstrap automatico: {e}")
            return {'success': False, 'error': str(e)}

# Esegui bootstrap all'avvio se richiesto
if settings.bootstrap_on_startup:
    bootstrap_system()
