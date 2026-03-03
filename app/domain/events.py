# app/domain/events.py
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field
import pandera as pa

# ==========================================
# 1. PYDANTIC: Validazione per i singoli record
# (Utile quando ingeriamo record uno ad uno dalle API)
# ==========================================
class EventRecord(BaseModel):
    case_id: str = Field(..., description="ID univoco dell'opportunità/deal su HubSpot")
    activity: str = Field(..., description="Azione eseguita o fase (es. 'Qualified')")
    timestamp: datetime = Field(..., description="Marca temporale esatta dell'evento")
    resource: Optional[str] = Field(default="System", description="Utente hash o ID di sistema")
    
    # Campi contestuali (Enhancement)
    amount: Optional[float] = Field(default=None, description="Valore economico in quel momento")

# ==========================================
# 2. PANDERA: Validazione massiva per il Data Lake
# (Garantisce la Data Quality prima del salvataggio su DuckDB)
# ==========================================
class EventLogSchema(pa.DataFrameModel):
    case_id: str = pa.Field(coerce=True)
    activity: str = pa.Field(allow_duplicates=True)
    timestamp: pa.DateTime = pa.Field()
    resource: str = pa.Field(nullable=True)
    
    class Config:
        # Assicura che i dati siano ordinati per PM4Py!
        # Questa regola controlla che all'interno di uno stesso case_id, i timestamp siano crescenti
        strict = True