import json
import hashlib
import polars as pl
from pathlib import Path
from app.core.logger import get_logger

logger = get_logger()

def hash_email(email: str) -> str:
    """
    Anonimizza l'email per conformità GDPR usando un hash MD5.
    """
    if not email:
        return "Unknown"
    return "User_" + hashlib.md5(email.encode('utf-8')).hexdigest()[:8]

def process_hubspot_json(filepath: str) -> pl.DataFrame:
    """
    Legge un JSON strutturato (Mock o HubSpot reale), lo appiattisce
    e restituisce un Event Log standard per PM4Py in formato Polars.
    """
    logger.info(f"Inizio elaborazione ETL per il file: {filepath}")
    
    path = Path(filepath)
    if not path.exists():
        logger.error(f"File non trovato: {filepath}")
        raise FileNotFoundError(f"Il file {filepath} non esiste.")

    with open(path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    flat_events = []
    
    # Flattening: Trasformiamo il JSON annidato in una lista di righe piatte
    for deal in raw_data:
        deal_id = deal.get("deal_id")
        history = deal.get("history", [])
        
        for event in history:
            flat_events.append({
                "case_id": deal_id,
                "activity": event.get("stage"),
                "timestamp": event.get("timestamp"),
                "resource": hash_email(event.get("user_email")) # GDPR
            })
            
    if not flat_events:
        logger.warning("Nessun evento trovato nel file JSON.")
        return pl.DataFrame()

    # Creazione del DataFrame Polars e parsing delle date
    # Convertiamo la stringa "2023-10-01T08:00:00Z" in un vero oggetto Datetime
    df = pl.DataFrame(flat_events).with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ")
    )
    
    # Ordiniamo gli eventi cronologicamente per ogni caso (fondamentale per PM4Py!)
    df = df.sort(["case_id", "timestamp"])
    
    logger.info(f"ETL completato con successo. Generate {len(df)} righe.")
    return df