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
    
    # Database
    db_path: str = str(data_dir / "process_mining.db")
    duckdb_memory_limit: str = "2GB"
    
    # HubSpot API
    hubspot_api_key: Optional[str] = None
    hubspot_api_base_url: str = "https://api.hubapi.com"
    hubspot_rate_limit_delay: float = 0.1  # seconds between requests
    
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
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

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
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)

# Setup iniziale
setup_directories()