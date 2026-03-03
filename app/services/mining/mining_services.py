import pm4py
import polars as pl
import pandas as pd
from app.core.logging import get_logger

logger = get_logger()

def _prepare_event_log(df: pl.DataFrame) -> pd.DataFrame:
    """
    Converte il DataFrame Polars in Pandas e lo formatta per PM4Py.
    """
    # Convertiamo Polars in Pandas
    pdf = df.to_pandas()
    
    # Formattiamo indicando a PM4Py le 3 colonne obbligatorie
    formatted_log = pm4py.format_dataframe(
        pdf,
        case_id='case_id',
        activity_key='activity',
        timestamp_key='timestamp'
    )
    return formatted_log

def discover_process_map(df: pl.DataFrame, output_image_path: str = "data/process_map.png"):
    """
    Calcola il Directly-Follows Graph (DFG) e salva un'immagine PNG.
    """
    logger.info("Avvio Process Discovery (DFG)...")
    log = _prepare_event_log(df)

    # L'algoritmo di Discovery
    dfg, start_activities, end_activities = pm4py.discover_dfg(log)

    # Salviamo il grafo come immagine (utile per la dashboard Taipy e per la tesi)
    logger.info(f"Salvataggio mappa del processo in: {output_image_path}")
    pm4py.save_vis_dfg(dfg, start_activities, end_activities, output_image_path)

    return dfg, start_activities, end_activities

def get_process_statistics(df: pl.DataFrame) -> dict:
    """
    Calcola KPI base del processo (Casi, Eventi, Varianti).
    """
    logger.info("Calcolo statistiche del log degli eventi...")
    log = _prepare_event_log(df)

    # Estraiamo le varianti 
    # (Restituisce un dizionario: { 'Percorso': Conteggio_Intero })
    variants = pm4py.get_variants(log)
    
    # FIX: x[1] è già un numero intero, non serve fare len()!
    sorted_variants = sorted(variants.items(), key=lambda x: x[1], reverse=True)
    
    # Prendiamo il percorso più comune (Happy Path)
    top_variant_path = sorted_variants[0][0] if sorted_variants else "N/A"
    # FIX: Anche qui, prendiamo direttamente x[1]
    top_variant_count = sorted_variants[0][1] if sorted_variants else 0

    stats = {
        "casi_totali": len(log['case:concept:name'].unique()), 
        "eventi_totali": len(log),
        "numero_varianti": len(variants),
        "percorso_piu_comune": top_variant_path,
        "frequenza_percorso_comune": top_variant_count
    }
    
    logger.info(f"Statistiche calcolate: {stats}")
    return stats