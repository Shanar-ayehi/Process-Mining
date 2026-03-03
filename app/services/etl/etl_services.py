# app/services/etl_services.py
import json
import hashlib
import polars as pl
from pathlib import Path
from app.core.logging import get_logger
from app.domain.events import EventLogSchema

logger = get_logger()

def hash_email(email: str) -> str:
    """Anonimizza l'email per conformità GDPR usando hash MD5."""
    if not email:
        return "Unknown"
    return "User_" + hashlib.md5(email.encode('utf-8')).hexdigest()[:8]

def process_hubspot_json(filepath: str) -> pl.DataFrame:
    """Legge JSON, lo appiattisce e ne valida la qualità tramite Pandera."""
    logger.info(f"Inizio elaborazione ETL per il file: {filepath}")
    
    path = Path(filepath)
    if not path.exists():
        logger.error(f"File non trovato: {filepath}")
        raise FileNotFoundError(f"Il file {filepath} non esiste.")

    with open(path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    flat_events = []
    
    # Flattening
    for deal in raw_data:
        deal_id = str(deal.get("deal_id")) # Forza la stringa per sicurezza
        history = deal.get("history", [])
        
        for event in history:
            flat_events.append({
                "case_id": deal_id,
                "activity": event.get("stage"),
                "timestamp": event.get("timestamp"),
                "resource": hash_email(event.get("user_email"))
            })
            
    if not flat_events:
        logger.warning("Nessun evento trovato nel file JSON.")
        return pl.DataFrame()

    # 1. Creazione DataFrame
    df = pl.DataFrame(flat_events).with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ")
    )
    
    # 2. Ordinamento cronologico OBBLIGATORIO per PM4Py
    df = df.sort(["case_id", "timestamp"])
    
    # 3. DATA QUALITY: Validazione Pandera
    try:
        df = EventLogSchema.validate(df)
        logger.info(f"ETL e Validazione Data Quality completati. Generate {len(df)} righe.")
    except Exception as e:
        logger.error(f"Errore di Data Quality: i dati non rispettano il modello! {e}")
        raise
        
    return df