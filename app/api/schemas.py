from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum

# Enums
class ProcessAlgorithm(str, Enum):
    DFG = "dfg"
    ALPHA = "alpha"
    HEURISTIC = "heuristic"
    INDUCTIVE = "inductive"

class ConformanceModelType(str, Enum):
    DFG = "dfg"
    PETRI_NET = "petri_net"
    DECLARE = "declare"

class KPIType(str, Enum):
    PROCESS = "process"
    RESOURCE = "resource"
    ACTIVITY = "activity"
    TREND = "trend"

class TaskStatus(str, Enum):
    PENDING = "pending"
    STARTED = "started"
    SUCCESS = "success"
    FAILURE = "failure"
    RETRY = "retry"
    REVOKED = "revoked"

class EntityType(str, Enum):
    DEAL = "deal"
    CONTACT = "contact"
    COMPANY = "company"

class ValidationType(str, Enum):
    SCHEMA = "schema"
    COMPLETENESS = "completeness"
    CONSISTENCY = "consistency"

class PrivacyOperation(str, Enum):
    ANONYMIZE = "anonymize"
    VALIDATE_GDPR = "validate_gdpr"
    APPLY_RETENTION = "apply_retention"

class ModelType(str, Enum):
    RANDOM_FOREST = "random_forest"
    GRADIENT_BOOSTING = "gradient_boosting"
    NEURAL_NETWORK = "neural_network"

class PredictionType(str, Enum):
    CLASSIFICATION = "classification"
    REGRESSION = "regression"
    CLUSTERING = "clustering"

class ReportType(str, Enum):
    COMPREHENSIVE = "comprehensive"
    SUMMARY = "summary"
    DETAILED = "detailed"

# Base schemas
class EventLogSchema(BaseModel):
    """Schema per event log."""
    case_id: str = Field(..., description="ID del caso")
    activity: str = Field(..., description="Attività eseguita")
    timestamp: datetime = Field(..., description="Timestamp dell'evento")
    resource: Optional[str] = Field(None, description="Risorsa che ha eseguito l'attività")
    deal_name: Optional[str] = Field(None, description="Nome del deal")
    amount: Optional[str] = Field(None, description="Importo")
    pipeline: Optional[str] = Field(None, description="Pipeline")
    stage_id: Optional[str] = Field(None, description="ID stage")
    transformed_at: Optional[str] = Field(None, description="Timestamp trasformazione")
    environment: Optional[str] = Field(None, description="Ambiente")

class TaskResultSchema(BaseModel):
    """Schema per risultati task."""
    success: bool = Field(..., description="Esito del task")
    data: Optional[Dict[str, Any]] = Field(None, description="Dati risultato")
    error: Optional[str] = Field(None, description="Messaggio di errore")
    timestamp: datetime = Field(..., description="Timestamp esecuzione")

class TaskStatusSchema(BaseModel):
    """Schema per stato task."""
    task_id: str = Field(..., description="ID del task")
    status: TaskStatus = Field(..., description="Stato del task")
    ready: bool = Field(..., description="Task completato")
    successful: Optional[bool] = Field(None, description="Task riuscito")
    result: Optional[Any] = Field(None, description="Risultato task")
    timestamp: datetime = Field(..., description="Timestamp controllo")

# Mining schemas
class MiningRequestSchema(BaseModel):
    """Schema per richieste mining."""
    portal_id: str = Field(..., description="ID del portale HubSpot")
    algorithm: ProcessAlgorithm = Field(ProcessAlgorithm.DFG, description="Algoritmo di discovery")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Parametri specifici")
    model_type: ConformanceModelType = Field(ConformanceModelType.DFG, description="Tipo modello")
    calculate_kpis: bool = Field(True, description="Calcolare KPI")
    start_date: Optional[str] = Field(None, description="Data inizio filtro (ISO format)")
    end_date: Optional[str] = Field(None, description="Data fine filtro (ISO format)")

class ConformanceRequestSchema(BaseModel):
    """Schema per richieste conformance."""
    portal_id: str = Field(..., description="ID del portale HubSpot")
    model_type: ConformanceModelType = Field(ConformanceModelType.DFG, description="Tipo modello")
    theoretical_model: Optional[Dict[str, Any]] = Field(None, description="Modello teorico")
    start_date: Optional[str] = Field(None, description="Data inizio filtro (ISO format)")
    end_date: Optional[str] = Field(None, description="Data fine filtro (ISO format)")

class KPIRequestSchema(BaseModel):
    """Schema per richieste KPI."""
    portal_id: str = Field(..., description="ID del portale HubSpot")
    time_window: str = Field("1d", description="Finestra temporale")
    calculate_resource_kpis: bool = Field(True, description="Calcolare KPI risorse")
    calculate_activity_kpis: bool = Field(True, description="Calcolare KPI attività")
    start_date: Optional[str] = Field(None, description="Data inizio filtro (ISO format)")
    end_date: Optional[str] = Field(None, description="Data fine filtro (ISO format)")

class VariantsRequestSchema(BaseModel):
    """Schema per richieste varianti."""
    portal_id: str = Field(..., description="ID del portale HubSpot")
    min_frequency_threshold: float = Field(0.05, description="Soglia frequenza minima")
    start_date: Optional[str] = Field(None, description="Data inizio filtro (ISO format)")
    end_date: Optional[str] = Field(None, description="Data fine filtro (ISO format)")

class DiscoveryResultSchema(BaseModel):
    """Schema per risultati discovery."""
    algorithm: ProcessAlgorithm = Field(..., description="Algoritmo utilizzato")
    discovery_result: Dict[str, Any] = Field(..., description="Risultati discovery")
    statistics: Dict[str, Any] = Field(..., description="Statistiche")

class ConformanceResultSchema(BaseModel):
    """Schema per risultati conformance."""
    model_type: ConformanceModelType = Field(..., description="Tipo modello")
    conformance_result: Dict[str, Any] = Field(..., description="Risultati conformance")
    fitness_precision: Dict[str, float] = Field(..., description="Fitness e precision")

class KPIResultSchema(BaseModel):
    """Schema per risultati KPI."""
    kpi_type: KPIType = Field(..., description="Tipo KPI")
    kpi_data: Dict[str, Any] = Field(..., description="Dati KPI")
    overall_score: float = Field(..., description="Punteggio complessivo")

# Connector schemas
class ExtractionRequestSchema(BaseModel):
    """Schema per richieste estrazione."""
    properties_with_history: Optional[List[str]] = Field(None, description="Proprietà con cronologia")
    include_contacts: bool = Field(False, description="Includere contatti")
    include_companies: bool = Field(False, description="Includere aziende")

class TransformationRequestSchema(BaseModel):
    """Schema per richieste trasformazione."""
    portal_id: str = Field(..., description="ID del portale HubSpot")
    include_contacts: bool = Field(False, description="Includere contatti")
    include_companies: bool = Field(False, description="Includere aziende")
    start_date: Optional[str] = Field(None, description="Data inizio filtro (ISO format)")
    end_date: Optional[str] = Field(None, description="Data fine filtro (ISO format)")

class PipelineRequestSchema(BaseModel):
    """Schema per richieste pipeline."""
    portal_id: str = Field(..., description="ID del portale HubSpot")
    properties_with_history: Optional[List[str]] = Field(None, description="Proprietà con cronologia")
    include_contacts: bool = Field(False, description="Includere contatti")
    include_companies: bool = Field(False, description="Includere aziende")
    schedule_interval: Optional[int] = Field(None, description="Intervallo pianificazione")
    start_date: Optional[str] = Field(None, description="Data inizio filtro (ISO format)")
    end_date: Optional[str] = Field(None, description="Data fine filtro (ISO format)")

class ScheduleRequestSchema(BaseModel):
    """Schema per richieste pianificazione."""
    interval_hours: int = Field(24, description="Intervallo in ore")

class CleanupRequestSchema(BaseModel):
    """Schema per richieste pulizia."""
    retention_days: int = Field(30, description="Giorni di retention")

# Data Quality schemas
class ValidationRequestSchema(BaseModel):
    """Schema per richieste validazione."""
    portal_id: str = Field(..., description="ID del portale HubSpot")
    validate_schema: bool = Field(True, description="Validare schema")
    validate_completeness: bool = Field(True, description="Validare completezza")
    validate_consistency: bool = Field(True, description="Validare consistenza")
    start_date: Optional[str] = Field(None, description="Data inizio filtro (ISO format)")
    end_date: Optional[str] = Field(None, description="Data fine filtro (ISO format)")

class PrivacyRequestSchema(BaseModel):
    """Schema per richieste privacy."""
    portal_id: str = Field(..., description="ID del portale HubSpot")
    sensitive_columns: Optional[List[str]] = Field(None, description="Colonne sensibili")
    start_date: Optional[str] = Field(None, description="Data inizio filtro (ISO format)")
    end_date: Optional[str] = Field(None, description="Data fine filtro (ISO format)")

class RetentionRequestSchema(BaseModel):
    """Schema per richieste retention."""
    data_dir: Optional[str] = Field(None, description="Directory dati")
    retention_days: int = Field(30, description="Giorni di retention")

class AuditRequestSchema(BaseModel):
    """Schema per richieste audit."""
    operation: str = Field(..., description="Operazione")
    user_id: Optional[str] = Field(None, description="ID utente")
    data_description: Optional[str] = Field(None, description="Descrizione dati")
    sensitive_data_accessed: bool = Field(False, description="Accesso dati sensibili")

class QualityReportSchema(BaseModel):
    """Schema per report qualità."""
    overall_score: float = Field(..., description="Punteggio complessivo")
    schema_validation: Dict[str, Any] = Field(..., description="Validazione schema")
    completeness_validation: Dict[str, Any] = Field(..., description="Validazione completezza")
    consistency_validation: Dict[str, Any] = Field(..., description="Validazione consistenza")
    recommendations: List[str] = Field(..., description="Raccomandazioni")

# Analytics schemas
class FeatureEngineeringRequestSchema(BaseModel):
    """Schema per richieste feature engineering."""
    portal_id: str = Field(..., description="ID del portale HubSpot")
    feature_types: List[str] = Field(["temporal", "frequency", "sequence"], description="Tipi di feature")
    include_advanced_features: bool = Field(True, description="Includere features avanzate")
    start_date: Optional[str] = Field(None, description="Data inizio filtro (ISO format)")
    end_date: Optional[str] = Field(None, description="Data fine filtro (ISO format)")

class ModelTrainingRequestSchema(BaseModel):
    """Schema per richieste training modelli."""
    portal_id: str = Field(..., description="ID del portale HubSpot")
    target_variable: str = Field(..., description="Variabile target")
    model_types: List[ModelType] = Field([ModelType.RANDOM_FOREST], description="Tipi di modello")
    hyperparameter_tuning: bool = Field(True, description="Ottimizzazione iperparametri")
    start_date: Optional[str] = Field(None, description="Data inizio filtro (ISO format)")
    end_date: Optional[str] = Field(None, description="Data fine filtro (ISO format)")

class PredictionRequestSchema(BaseModel):
    """Schema per richieste predizioni."""
    portal_id: str = Field(..., description="ID del portale HubSpot")
    model_id: str = Field(..., description="ID modello")
    prediction_type: PredictionType = Field(PredictionType.CLASSIFICATION, description="Tipo predizione")
    start_date: Optional[str] = Field(None, description="Data inizio filtro (ISO format)")
    end_date: Optional[str] = Field(None, description="Data fine filtro (ISO format)")

class AnalyticsReportRequestSchema(BaseModel):
    """Schema per richieste report analytics."""
    portal_id: str = Field(..., description="ID del portale HubSpot")
    report_type: ReportType = Field(ReportType.COMPREHENSIVE, description="Tipo report")
    include_visualizations: bool = Field(True, description="Includere visualizzazioni")
    time_range: Optional[Dict[str, str]] = Field(None, description="Range temporale")
    start_date: Optional[str] = Field(None, description="Data inizio filtro (ISO format)")
    end_date: Optional[str] = Field(None, description="Data fine filtro (ISO format)")

class ModelPerformanceSchema(BaseModel):
    """Schema per performance modelli."""
    model_id: str = Field(..., description="ID modello")
    performance_metrics: Dict[str, float] = Field(..., description="Metriche performance")
    feature_importance: List[Dict[str, Any]] = Field(..., description="Importanza features")
    last_evaluation: datetime = Field(..., description="Ultima valutazione")

class AnalyticsSummarySchema(BaseModel):
    """Schema per riepilogo analytics."""
    total_features_engineered: int = Field(..., description="Features create")
    models_trained: int = Field(..., description="Modelli addestrati")
    predictions_made: int = Field(..., description="Predizioni effettuate")
    key_insights: List[str] = Field(..., description="Insights principali")
    predictions_summary: Dict[str, Any] = Field(..., description="Riepilogo predizioni")

# Integration schemas
class IntegrationRequestSchema(BaseModel):
    """Schema per richieste integrazione."""
    portal_id: str = Field(..., description="ID del portale HubSpot")
    sync_options: Optional[Dict[str, Any]] = Field(None, description="Opzioni sincronizzazione")
    start_date: Optional[str] = Field(None, description="Data inizio filtro (ISO format)")
    end_date: Optional[str] = Field(None, description="Data fine filtro (ISO format)")

class SyncResultSchema(BaseModel):
    """Schema per risultati sincronizzazione."""
    synced_count: int = Field(..., description="Elementi sincronizzati")
    failed_count: int = Field(..., description="Elementi falliti")
    sync_status: str = Field(..., description="Stato sincronizzazione")

class BridgeResultSchema(BaseModel):
    """Schema per risultati bridge."""
    bridge_results: Dict[str, Any] = Field(..., description="Risultati bridge")
    sync_options: Dict[str, Any] = Field(..., description="Opzioni sincronizzazione")

# Health check schemas
class HealthStatusSchema(BaseModel):
    """Schema per stato health check."""
    status: str = Field(..., description="Stato servizio")
    services: Dict[str, str] = Field(..., description="Stato servizi")
    dependencies: Dict[str, str] = Field(..., description="Stato dipendenze")
    timestamp: datetime = Field(..., description="Timestamp controllo")

class ServiceStatusSchema(BaseModel):
    """Schema per stato servizio specifico."""
    service_name: str = Field(..., description="Nome servizio")
    status: str = Field(..., description="Stato")
    version: str = Field(..., description="Versione")
    uptime: str = Field(..., description="Tempo attivo")
    last_check: datetime = Field(..., description="Ultimo controllo")

# Configuration schemas
class ValidationConfigSchema(BaseModel):
    """Schema per configurazione validazione."""
    schema_validation: Dict[str, Any] = Field(..., description="Configurazione schema")
    completeness_validation: Dict[str, Any] = Field(..., description="Configurazione completezza")
    consistency_validation: Dict[str, Any] = Field(..., description="Configurazione consistenza")

class PrivacyConfigSchema(BaseModel):
    """Schema per configurazione privacy."""
    pseudonymization_enabled: bool = Field(..., description="Pseudonimizzazione abilitata")
    sensitive_patterns: List[str] = Field(..., description="Pattern sensibili")
    retention_policy: Dict[str, Any] = Field(..., description="Policy retention")
    gdpr_compliance: Dict[str, bool] = Field(..., description="Compliance GDPR")

class FeatureEngineeringConfigSchema(BaseModel):
    """Schema per configurazione feature engineering."""
    temporal_features: Dict[str, Any] = Field(..., description="Configurazione features temporali")
    frequency_features: Dict[str, Any] = Field(..., description="Configurazione features frequenza")
    sequence_features: Dict[str, Any] = Field(..., description="Configurazione features sequenza")
    advanced_features: Dict[str, Any] = Field(..., description="Configurazione features avanzate")

class PredictiveModelsConfigSchema(BaseModel):
    """Schema per configurazione modelli predittivi."""
    model_types: Dict[str, Any] = Field(..., description="Configurazione tipi modello")
    hyperparameter_tuning: Dict[str, Any] = Field(..., description="Configurazione tuning")
    model_evaluation: Dict[str, Any] = Field(..., description="Configurazione valutazione")

# Discovery schemas
class DiscoveryRequestSchema(BaseModel):
    """Schema per richieste discovery."""
    sample_size: Optional[int] = Field(None, description="Dimensione campione")
    save_results: bool = Field(True, description="Salvare risultati")
    apply_config: bool = Field(False, description="Applicare configurazione")

class ConfigUpdateRequestSchema(BaseModel):
    """Schema per richieste aggiornamento configurazione."""
    stage_mappings: Optional[List[Dict[str, Any]]] = Field(None, description="Mapping fasi pipeline")
    data_structure: Optional[Dict[str, str]] = Field(None, description="Struttura dati")
    custom_properties: Optional[List[str]] = Field(None, description="Proprietà personalizzate")
    required_properties: Optional[List[str]] = Field(None, description="Proprietà richieste")
    privacy_fields: Optional[List[str]] = Field(None, description="Campi privacy")

# Response schemas
class APIResponseSchema(BaseModel):
    """Schema base per risposte API."""
    success: bool = Field(..., description="Esito operazione")
    message: str = Field(..., description="Messaggio risposta")
    data: Optional[Any] = Field(None, description="Dati risposta")
    timestamp: datetime = Field(default_factory=datetime.now, description="Timestamp risposta")

class ErrorResponseSchema(BaseModel):
    """Schema per errori API."""
    error: str = Field(..., description="Messaggio errore")
    error_code: str = Field(..., description="Codice errore")
    details: Optional[Dict[str, Any]] = Field(None, description="Dettagli errore")
    timestamp: datetime = Field(default_factory=datetime.now, description="Timestamp errore")

# Validation schemas
class EventLogValidationSchema(BaseModel):
    """Schema per validazione event log."""
    total_rows: int = Field(..., description="Numero totale righe")
    validation_passed: bool = Field(..., description="Validazione superata")
    errors: List[str] = Field(..., description="Errori validazione")
    warnings: List[str] = Field(..., description="Avvisi validazione")
    statistics: Dict[str, Any] = Field(..., description="Statistiche validazione")

class DataQualityValidationSchema(BaseModel):
    """Schema per validazione qualità dati."""
    completeness_score: float = Field(..., description="Punteggio completezza")
    consistency_score: float = Field(..., description="Punteggio consistenza")
    quality_issues: List[str] = Field(..., description="Problemi qualità")
    missing_values: Dict[str, int] = Field(..., description="Valori mancanti")

class GDPRComplianceSchema(BaseModel):
    """Schema per compliance GDPR."""
    compliance_score: float = Field(..., description="Punteggio compliance")
    pseudonymization_enabled: bool = Field(..., description="Pseudonimizzazione abilitata")
    sensitive_columns_found: List[str] = Field(..., description="Colonne sensibili trovate")
    issues: List[str] = Field(..., description="Problemi compliance")

# Simulation schemas
class SimulationRequestSchema(BaseModel):
    """Schema per richieste What-If Simulation."""
    portal_id: str = Field(..., description="ID del portale HubSpot")
    num_cases: int = Field(100, description="Numero di casi da simulare", ge=1, le=10000)
    modifications: Optional[Dict[str, Dict[str, float]]] = Field(
        None, 
        description="Modifiche da applicare (es. {'Activity': {'time_multiplier': 0.8}})"
    )
    seed: Optional[int] = Field(42, description="Seed per riproducibilità")
    start_date: Optional[str] = Field(None, description="Data inizio filtro (ISO format)")
    end_date: Optional[str] = Field(None, description="Data fine filtro (ISO format)")

# Utility validators
@validator('timestamp', pre=True)
def validate_timestamp(cls, v):
    """Validatore timestamp."""
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace('Z', '+00:00'))
        except ValueError:
            raise ValueError("Formato timestamp non valido")
    return v

@validator('case_id', 'activity')
def validate_required_fields(cls, v):
    """Validatore campi obbligatori."""
    if not v or not v.strip():
        raise ValueError("Campo obbligatorio non può essere vuoto")
    return v.strip()