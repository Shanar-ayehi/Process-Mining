import json
import hashlib
import polars as pl
from pathlib import Path
from app.core.logger import get_logger
from app.domain.events import EventLogSchema

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
    Legge un JSON strutturato, lo appiattisce, esegue l'hashing GDPR
    e valida la Data Quality tramite Pandera.
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
        deal_id = str(deal.get("deal_id")) # Cast a stringa per sicurezza
        history = deal.get("history", [])
        
        for event in history:
            flat_events.append({
                "case_id": deal_id,
                "activity": event.get("stage"),
                "timestamp": event.get("timestamp"),
                "resource": hash_email(event.get("user_email")) # Hashing applicato qui
            })
            
    if not flat_events:
        logger.warning("Nessun evento trovato nel file JSON.")
        return pl.DataFrame()

    # Creazione del DataFrame Polars e parsing delle date in Datetime reali
    df = pl.DataFrame(flat_events).with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ")
    )
    
    # Ordiniamo gli eventi cronologicamente per ogni caso (fondamentale per PM4Py!)
    df = df.sort(["case_id", "timestamp"])
    
    # ==========================================
    # DATA QUALITY CHECK (Pandera)
    # ==========================================
    try:
        # Passiamo i dati alla nostra "Dogana"
        df_validated = EventLogSchema.validate(df)
        logger.info("✅ Validazione Data Quality superata con successo.")
    except Exception as e:
        logger.error(f"❌ Errore di Data Quality! I dati non rispettano lo schema: {e}")
        raise # Blocchiamo l'esecuzione, non vogliamo salvare dati "sporchi" nel DB
    
    logger.info(f"ETL completato. Generate {len(df_validated)} righe validate.")
    return df_validated