# app/domain/events.py
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field
import pandera.polars as pa

# ==========================================
# 1. PYDANTIC: Validazione per i singoli record
# ==========================================
class EventRecord(BaseModel):
    case_id: str = Field(..., description="ID univoco dell'opportunità/deal su HubSpot")
    activity: str = Field(..., description="Azione eseguita o fase (es. 'Qualified')")
    timestamp: datetime = Field(..., description="Marca temporale esatta dell'evento")
    resource: Optional[str] = Field(default="System", description="Utente hash o ID di sistema")
    
    # Questo ci tornerà utile in futuro per Analytics/Predittivo
    amount: Optional[float] = Field(default=None, description="Valore economico del deal")

# ==========================================
# 2. PANDERA: Validazione massiva per il Data Lake (Polars)
# ==========================================
class EventLogSchema(pa.DataFrameModel):
    case_id: str = pa.Field(coerce=True)  # Forza la conversione in stringa se arrivano numeri
    activity: str = pa.Field()
    timestamp: datetime = pa.Field()
    resource: str = pa.Field(nullable=True)
    
    class Config:
        # Assicura la coerenza del DataFrame prima di passarlo a DuckDB o PM4Py
        strict = True
        # Qui in futuro potremo aggiungere controlli custom (es. timestamp ordinati per case_id)