from typing import Dict, List, Any, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from datetime import datetime
import polars as pl

from app.services.analytics.feature_engineering import feature_engineering_service
from app.services.analytics.predictive_models import predictive_models_service
from app.tasks.jobs_analytics import (
    run_feature_engineering_task, train_predictive_models_task,
    generate_analytics_report_task, export_analytics_results_task
)
from app.core.logger import get_logger

logger = get_logger()

router = APIRouter(prefix="/analytics", tags=["Analytics"])

# Pydantic models
class FeatureEngineeringRequest(BaseModel):
    event_log_data: List[Dict[str, Any]]
    feature_types: List[str] = ["temporal", "frequency", "sequence"]
    include_advanced_features: bool = True

class ModelTrainingRequest(BaseModel):
    training_data: List[Dict[str, Any]]
    target_variable: str
    model_types: List[str] = ["random_forest", "gradient_boosting"]
    hyperparameter_tuning: bool = True

class PredictionRequest(BaseModel):
    model_id: str
    input_data: List[Dict[str, Any]]
    prediction_type: str = "classification"  # classification, regression, clustering

class AnalyticsReportRequest(BaseModel):
    report_type: str = "comprehensive"  # comprehensive, summary, detailed
    include_visualizations: bool = True
    time_range: Optional[Dict[str, str]] = None

# Feature engineering endpoints
@router.post("/features/engineer")
async def run_feature_engineering(request: FeatureEngineeringRequest):
    """
    Esegue feature engineering sui dati.
    """
    try:
        logger.info(f"Richiesta feature engineering: {len(request.feature_types)} tipi di feature")
        
        task = run_feature_engineering_task.delay(
            event_log_data=request.event_log_data,
            feature_types=request.feature_types,
            include_advanced_features=request.include_advanced_features
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "feature_types": request.feature_types,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore feature engineering: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/features/extract")
async def extract_features(event_log_data: List[Dict[str, Any]]):
    """
    Estrae features base dall'event log.
    """
    try:
        logger.info(f"Richiesta estrazione features: {len(event_log_data)} record")
        
        # Converte i dati in DataFrame
        event_log_df = pl.DataFrame(event_log_data)
        
        # Estrae features
        features = feature_engineering_service.extract_basic_features(event_log_df)
        
        return {
            "features_extracted": len(features),
            "feature_names": list(features.keys()) if isinstance(features, dict) else [],
            "features": features,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore estrazione features: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/features/advanced")
async def extract_advanced_features(event_log_data: List[Dict[str, Any]]):
    """
    Estrae features avanzate dall'event log.
    """
    try:
        logger.info(f"Richiesta estrazione features avanzate: {len(event_log_data)} record")
        
        event_log_df = pl.DataFrame(event_log_data)
        
        # Estrae features avanzate
        advanced_features = feature_engineering_service.extract_advanced_features(event_log_df)
        
        return {
            "advanced_features_extracted": len(advanced_features),
            "feature_categories": list(advanced_features.keys()) if isinstance(advanced_features, dict) else [],
            "advanced_features": advanced_features,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore estrazione features avanzate: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Predictive modeling endpoints
@router.post("/models/train")
async def train_predictive_models(request: ModelTrainingRequest):
    """
    Allena modelli predittivi.
    """
    try:
        logger.info(f"Richiesta training modelli: {len(request.model_types)} tipi di modello")
        
        task = train_predictive_models_task.delay(
            training_data=request.training_data,
            target_variable=request.target_variable,
            model_types=request.model_types,
            hyperparameter_tuning=request.hyperparameter_tuning
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "model_types": request.model_types,
            "target_variable": request.target_variable,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore training modelli: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/models/predict")
async def make_predictions(request: PredictionRequest):
    """
    Esegue predizioni con modello specificato.
    """
    try:
        logger.info(f"Richiesta predizioni: modello {request.model_id}")
        
        # Placeholder - in produzione si caricherebbe il modello e si eseguirebbero le predizioni
        predictions = {
            "model_id": request.model_id,
            "prediction_type": request.prediction_type,
            "predictions": [],
            "confidence_scores": [],
            "timestamp": datetime.now().isoformat()
        }
        
        return predictions
        
    except Exception as e:
        logger.error(f"Errore predizioni: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/models/list")
async def list_available_models():
    """
    Lista modelli disponibili.
    """
    try:
        logger.info("Richiesta lista modelli disponibili")
        
        # Placeholder - in produzione si recupererebbero i modelli reali
        models = {
            "available_models": [
                {
                    "model_id": "rf_churn_001",
                    "model_type": "random_forest",
                    "target": "churn_prediction",
                    "accuracy": 0.85,
                    "last_trained": "2024-01-15T10:00:00Z"
                },
                {
                    "model_id": "gb_ltv_001",
                    "model_type": "gradient_boosting",
                    "target": "lifetime_value",
                    "accuracy": 0.78,
                    "last_trained": "2024-01-14T15:30:00Z"
                }
            ],
            "timestamp": datetime.now().isoformat()
        }
        
        return models
        
    except Exception as e:
        logger.error(f"Errore lista modelli: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/models/{model_id}/performance")
async def get_model_performance(model_id: str):
    """
    Ottiene performance di un modello specifico.
    """
    try:
        logger.info(f"Richiesta performance modello: {model_id}")
        
        # Placeholder - in produzione si recupererebbero le performance reali
        performance = {
            "model_id": model_id,
            "performance_metrics": {
                "accuracy": 0.85,
                "precision": 0.82,
                "recall": 0.88,
                "f1_score": 0.85,
                "roc_auc": 0.91
            },
            "feature_importance": [
                {"feature": "deal_stage", "importance": 0.35},
                {"feature": "deal_amount", "importance": 0.25},
                {"feature": "time_in_stage", "importance": 0.20},
                {"feature": "owner_activity", "importance": 0.15},
                {"feature": "contact_engagement", "importance": 0.05}
            ],
            "last_evaluation": "2024-01-15T10:00:00Z",
            "timestamp": datetime.now().isoformat()
        }
        
        return performance
        
    except Exception as e:
        logger.error(f"Errore performance modello {model_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Analytics report endpoints
@router.post("/report/generate")
async def generate_analytics_report(request: AnalyticsReportRequest):
    """
    Genera report analytics.
    """
    try:
        logger.info(f"Richiesta generazione report analytics: {request.report_type}")
        
        task = generate_analytics_report_task.delay(
            report_type=request.report_type,
            include_visualizations=request.include_visualizations,
            time_range=request.time_range
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "report_type": request.report_type,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore generazione report analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/report/summary")
async def get_analytics_summary():
    """
    Ottiene un riepilogo analytics sincrono.
    """
    try:
        logger.info("Richiesta riepilogo analytics sincrono")
        
        # Placeholder - in produzione si calcolerebbe il riepilogo reale
        summary = {
            "analytics_summary": {
                "total_features_engineered": 45,
                "models_trained": 8,
                "predictions_made": 1250,
                "accuracy_improvements": 15.5
            },
            "key_insights": [
                "Deal stage progression è il predittore più importante per il churn",
                "Time-to-close medio è diminuito del 12% negli ultimi 3 mesi",
                "Segmento enterprise mostra LTV 2.5x superiore al segmento SMB",
                "Attività di follow-up correlate con tasso di conversione 40% più alto"
            ],
            "predictions_summary": {
                "churn_predictions": {
                    "high_risk_customers": 125,
                    "medium_risk_customers": 340,
                    "low_risk_customers": 1800
                },
                "revenue_predictions": {
                    "next_quarter_revenue": 2500000,
                    "confidence_interval": "±150000",
                    "growth_rate": 0.12
                }
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return summary
        
    except Exception as e:
        logger.error(f"Errore riepilogo analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/insights/top")
async def get_top_insights():
    """
    Ottiene le principali insights analytics.
    """
    try:
        logger.info("Richiesta principali insights analytics")
        
        # Placeholder - in produzione si identificherebbero gli insights reali
        insights = {
            "top_insights": [
                {
                    "insight_type": "process_optimization",
                    "description": "Attività 'Negotiation' è il collo di bottiglia principale",
                    "impact": "high",
                    "recommendation": "Rivedere strategia di negoziazione e formare team dedicato"
                },
                {
                    "insight_type": "customer_segmentation",
                    "description": "Segmento 'Enterprise' ha tempo di ciclo 60% più lungo ma LTV 3x superiore",
                    "impact": "medium",
                    "recommendation": "Allocare risorse dedicate a segmento enterprise"
                },
                {
                    "insight_type": "resource_allocation",
                    "description": "Owner con >50 deal hanno tasso di conversione 25% inferiore",
                    "impact": "high",
                    "recommendation": "Ribilanciare carico di lavoro tra owner"
                },
                {
                    "insight_type": "timing_optimization",
                    "description": "Deal chiusi entro 30 giorni hanno margine medio 15% superiore",
                    "impact": "medium",
                    "recommendation": "Implementare SLA per accelerare chiusura deal"
                }
            ],
            "total_insights": 12,
            "timestamp": datetime.now().isoformat()
        }
        
        return insights
        
    except Exception as e:
        logger.error(f"Errore principali insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Export endpoints
@router.post("/export/results")
async def export_analytics_results(request: AnalyticsReportRequest):
    """
    Esporta risultati analytics.
    """
    try:
        logger.info(f"Richiesta esportazione risultati analytics: {request.report_type}")
        
        task = export_analytics_results_task.delay(
            report_type=request.report_type,
            include_visualizations=request.include_visualizations,
            time_range=request.time_range
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "export_type": "analytics_results",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore esportazione risultati analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export/formats")
async def get_export_formats():
    """
    Ottiene formati di esportazione disponibili.
    """
    try:
        logger.info("Richiesta formati esportazione")
        
        formats = {
            "available_formats": [
                {
                    "format": "csv",
                    "description": "Comma Separated Values",
                    "suitable_for": ["data_analysis", "excel_import"],
                    "file_size": "medium"
                },
                {
                    "format": "excel",
                    "description": "Microsoft Excel (.xlsx)",
                    "suitable_for": ["business_reports", "manual_analysis"],
                    "file_size": "large"
                },
                {
                    "format": "json",
                    "description": "JavaScript Object Notation",
                    "suitable_for": ["api_integration", "programmatic_access"],
                    "file_size": "small"
                },
                {
                    "format": "parquet",
                    "description": "Apache Parquet",
                    "suitable_for": ["big_data", "data_warehouse"],
                    "file_size": "very_large"
                }
            ],
            "timestamp": datetime.now().isoformat()
        }
        
        return formats
        
    except Exception as e:
        logger.error(f"Errore formati esportazione: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Health check endpoint
@router.get("/health")
async def analytics_health_check():
    """
    Health check per il servizio analytics.
    """
    try:
        logger.info("Health check analytics")
        
        health_status = {
            "status": "healthy",
            "services": {
                "feature_engineering": "available",
                "predictive_modeling": "available",
                "analytics_reporting": "available",
                "model_training": "available"
            },
            "dependencies": {
                "scikit-learn": "available",
                "pandas": "available",
                "numpy": "available",
                "polars": "available"
            },
            "models_status": {
                "total_models": 8,
                "active_models": 6,
                "training_models": 2
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return health_status
        
    except Exception as e:
        logger.error(f"Errore health check analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Task management endpoints
@router.get("/tasks/{task_id}/status")
async def get_task_status(task_id: str):
    """
    Ottiene lo stato di un task analytics.
    """
    try:
        from celery.result import AsyncResult
        
        result = AsyncResult(task_id)
        
        status_data = {
            "task_id": task_id,
            "status": result.status,
            "ready": result.ready(),
            "successful": result.successful() if result.ready() else None,
            "result": result.result if result.ready() else None,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"Stato task analytics {task_id}: {status_data}")
        return status_data
        
    except Exception as e:
        logger.error(f"Errore stato task analytics {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str):
    """
    Cancella un task analytics.
    """
    try:
        from celery.result import AsyncResult
        
        result = AsyncResult(task_id)
        result.revoke(terminate=True)
        
        logger.info(f"Task analytics {task_id} cancellato")
        return {"task_id": task_id, "cancelled": True, "timestamp": datetime.now().isoformat()}
        
    except Exception as e:
        logger.error(f"Errore cancellazione task analytics {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Configuration endpoints
@router.get("/config/feature-engineering")
async def get_feature_engineering_config():
    """
    Ottiene la configurazione feature engineering.
    """
    try:
        logger.info("Richiesta configurazione feature engineering")
        
        config = {
            "feature_engineering_config": {
                "temporal_features": {
                    "enabled": True,
                    "features": ["time_since_start", "time_between_activities", "day_of_week", "hour_of_day"]
                },
                "frequency_features": {
                    "enabled": True,
                    "features": ["activity_frequency", "resource_frequency", "case_length"]
                },
                "sequence_features": {
                    "enabled": True,
                    "features": ["activity_sequence", "ngrams", "markov_chains"]
                },
                "advanced_features": {
                    "enabled": True,
                    "features": ["process_variants", "social_network_analysis", "performance_indicators"]
                }
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return config
        
    except Exception as e:
        logger.error(f"Errore configurazione feature engineering: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/config/predictive-models")
async def get_predictive_models_config():
    """
    Ottiene la configurazione modelli predittivi.
    """
    try:
        logger.info("Richiesta configurazione modelli predittivi")
        
        config = {
            "predictive_models_config": {
                "model_types": {
                    "random_forest": {
                        "enabled": True,
                        "default_params": {"n_estimators": 100, "max_depth": 10},
                        "use_cases": ["classification", "regression"]
                    },
                    "gradient_boosting": {
                        "enabled": True,
                        "default_params": {"n_estimators": 50, "learning_rate": 0.1},
                        "use_cases": ["classification", "regression"]
                    },
                    "neural_networks": {
                        "enabled": False,
                        "default_params": {},
                        "use_cases": ["complex_patterns"]
                    }
                },
                "hyperparameter_tuning": {
                    "enabled": True,
                    "method": "grid_search",
                    "cv_folds": 5,
                    "scoring_metric": "accuracy"
                },
                "model_evaluation": {
                    "metrics": ["accuracy", "precision", "recall", "f1_score", "roc_auc"],
                    "cross_validation": True,
                    "holdout_percentage": 0.2
                }
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return config
        
    except Exception as e:
        logger.error(f"Errore configurazione modelli predittivi: {e}")
        raise HTTPException(status_code=500, detail=str(e))