from typing import Dict, Any
from fastapi import APIRouter, HTTPException
from datetime import datetime

from app.services.mining.discovery_service import discovery_service
from app.services.etl.data_discovery import auto_discovery_service
from app.core.hubspot_config import hubspot_config_manager
from app.tasks.mining_task import (
    discover_process_model_task, discover_variants_task, discover_performance_dfg_task,
    check_conformance_task, detect_deviations_task, calculate_process_kpis_task,
    calculate_resource_kpis_task, calculate_activity_kpis_task, calculate_trend_kpis_task,
    run_full_mining_analysis_task, generate_mining_report_task
)
from app.core.logger import get_logger
from app.api.schemas import (
    MiningRequestSchema, ConformanceRequestSchema, KPIRequestSchema, VariantsRequestSchema,
    DiscoveryRequestSchema, ConfigUpdateRequestSchema
)

logger = get_logger()

router = APIRouter(prefix="/mining", tags=["Mining"])

# Discovery endpoints
@router.post("/discover/process-model")
async def discover_process_model(request: MiningRequestSchema):
    """
    Scopre il modello di processo con algoritmo specificato.
    """
    try:
        logger.info(f"Richiesta discovery modello processo: {request.algorithm}")
        
        # Esegui discovery in background
        task = discover_process_model_task.delay(
            event_log_df=None,  # Da implementare caricamento event log
            algorithm=request.algorithm,
            parameters=request.parameters or {}
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "algorithm": request.algorithm,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore discovery modello processo: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/discover/variants")
async def discover_process_variants(request: VariantsRequestSchema):
    """
    Scopre le varianti del processo.
    """
    try:
        logger.info(f"Richiesta discovery varianti: threshold {request.min_frequency_threshold}")
        
        task = discover_variants_task.delay(
            event_log_df=None,  # Da implementare caricamento event log
            min_frequency_threshold=request.min_frequency_threshold
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "threshold": request.min_frequency_threshold,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore discovery varianti: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/discover/performance-dfg")
async def discover_performance_dfg():
    """
    Scopre il DFG con informazioni di performance.
    """
    try:
        logger.info("Richiesta discovery performance DFG")
        
        task = discover_performance_dfg_task.delay(
            event_log_df=None  # Da implementare caricamento event log
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore discovery performance DFG: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/discover/dfg-with-automations/{portal_id}")
async def get_dfg_with_automations(portal_id: str, include_performance: bool = False):
    """
    Ottiene il DFG con le automazioni HubSpot mappate sui nodi.
    
    Args:
        portal_id: ID del portale HubSpot
        include_performance: Se includere le metriche di performance
    """
    try:
        logger.info(f"Richiesta DFG con automazioni per portal_id: {portal_id}")
        
        # Carica event log
        from app.core.database import load_event_log
        table_name = f"event_log_{portal_id}"
        event_log_df = load_event_log(portal_id=portal_id, table_name=table_name)
        
        if event_log_df.is_empty():
            raise HTTPException(status_code=404, detail=f"Nessun dato trovato per portal_id: {portal_id}")
        
        # Carica workflow più recenti
        from app.core.config import settings
        import json
        import glob
        
        workflows_dir = settings.raw_data_dir
        workflow_files = sorted(glob.glob(str(workflows_dir / "hubspot_workflows_*.json")))
        
        workflows = []
        if workflow_files:
            latest_workflow_file = workflow_files[-1]
            with open(latest_workflow_file, 'r', encoding='utf-8') as f:
                workflows = json.load(f)
            logger.info(f"Caricati {len(workflows)} workflow da {latest_workflow_file}")
        
        # Esegui discovery con mapping workflow
        if include_performance:
            result = discovery_service.discover_performance_dfg(event_log_df, workflows=workflows)
        else:
            result = discovery_service.discover_dfg(event_log_df, workflows=workflows)
        
        # Restituisci solo graph_data per il frontend
        return {
            "portal_id": portal_id,
            "graph_data": result.get('graph_data', {}),
            "statistics": result.get('statistics', {}),
            "workflows_mapped": len(workflows),
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore DFG con automazioni: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Conformance checking endpoints
@router.post("/conformance/check")
async def check_conformance(request: ConformanceRequestSchema):
    """
    Esegue conformance checking.
    """
    try:
        logger.info(f"Richiesta conformance checking: {request.model_type}")
        
        task = check_conformance_task.delay(
            event_log_df=None,  # Da implementare caricamento event log
            model_type=request.model_type,
            theoretical_model=request.theoretical_model
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "model_type": request.model_type,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore conformance checking: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/conformance/detect-deviations")
async def detect_deviations(conformance_result: Dict[str, Any]):
    """
    Rileva pattern di deviazione.
    """
    try:
        logger.info("Richiesta rilevamento deviazioni")
        
        task = detect_deviations_task.delay(
            event_log_df=None,  # Da implementare caricamento event log
            conformance_result=conformance_result
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore rilevamento deviazioni: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# KPI endpoints
@router.post("/kpis/process")
async def calculate_process_kpis():
    """
    Calcola KPI principali del processo.
    """
    try:
        logger.info("Richiesta calcolo KPI processo")
        
        task = calculate_process_kpis_task.delay(
            event_log_df=None  # Da implementare caricamento event log
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore calcolo KPI processo: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/kpis/resource")
async def calculate_resource_kpis():
    """
    Calcola KPI per risorsa.
    """
    try:
        logger.info("Richiesta calcolo KPI risorsa")
        
        task = calculate_resource_kpis_task.delay(
            event_log_df=None  # Da implementare caricamento event log
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore calcolo KPI risorsa: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/kpis/activity")
async def calculate_activity_kpis():
    """
    Calcola KPI per attività.
    """
    try:
        logger.info("Richiesta calcolo KPI attività")
        
        task = calculate_activity_kpis_task.delay(
            event_log_df=None  # Da implementare caricamento event log
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore calcolo KPI attività: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/kpis/trend")
async def calculate_trend_kpis(request: KPIRequestSchema):
    """
    Calcola KPI di trend temporale.
    """
    try:
        logger.info(f"Richiesta calcolo KPI trend: {request.time_window}")
        
        task = calculate_trend_kpis_task.delay(
            event_log_df=None,  # Da implementare caricamento event log
            time_window=request.time_window
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "time_window": request.time_window,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore calcolo KPI trend: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Analysis endpoints
@router.post("/analysis/full")
async def run_full_mining_analysis(request: MiningRequestSchema):
    """
    Esegue analisi mining completa.
    """
    try:
        logger.info("Richiesta analisi mining completa")
        
        task = run_full_mining_analysis_task.delay(
            event_log_df=None,  # Da implementare caricamento event log
            discovery_algorithms=[request.algorithm],
            conformance_model_type=request.model_type,
            calculate_kpis=request.calculate_kpis
        )
        
        return {
            "task_id": task.id,
            "status": "started",
            "algorithms": [request.algorithm],
            "calculate_kpis": request.calculate_kpis,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore analisi mining completa: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/report/generate")
async def generate_mining_report(analysis_results: Dict[str, Any]):
    """
    Genera report mining completo.
    """
    try:
        logger.info("Richiesta generazione report mining")
        
        task = generate_mining_report_task.delay(analysis_results=analysis_results)
        
        return {
            "task_id": task.id,
            "status": "started",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore generazione report mining: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Direct endpoints (sincroni)
@router.get("/kpis/summary", status_code=204)
async def get_kpi_summary():
    """
    Ottiene un riepilogo KPI sincrono.
    NOTA: Attualmente non implementato, restituisce 204 se non ci sono dati pronti.
    """
    logger.info("Richiesta riepilogo KPI sincrono - Dati non disponibili o implementazione assente.")
    return

@router.get("/conformance/fitness", status_code=204)
async def get_conformance_fitness():
    """
    Ottiene fitness e precision sincroni.
    NOTA: Attualmente non implementato, restituisce 204 se non ci sono dati pronti.
    """
    logger.info("Richiesta fitness e precision sincroni - Dati non disponibili o implementazione assente.")
    return

@router.get("/variants/top", status_code=204)
async def get_top_variants():
    """
    Ottiene le varianti più comuni.
    NOTA: Attualmente non implementato, restituisce 204 se non ci sono dati pronti.
    """
    logger.info("Richiesta varianti più comuni - Dati non disponibili o implementazione assente.")
    return

# Health check endpoint
@router.get("/health")
async def mining_health_check():
    """
    Health check per il servizio mining.
    """
    try:
        logger.info("Health check mining")
        
        health_status = {
            "status": "healthy",
            "services": {
                "discovery": "available",
                "conformance": "available", 
                "kpi": "available",
                "pm4py": "available"
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return health_status
        
    except Exception as e:
        logger.error(f"Errore health check mining: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Task management endpoints
@router.get("/tasks/{task_id}/status")
async def get_task_status(task_id: str):
    """
    Ottiene lo stato di un task mining.
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
        
        logger.info(f"Stato task mining {task_id}: {status_data}")
        return status_data
        
    except Exception as e:
        logger.error(f"Errore stato task mining {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str):
    """
    Cancella un task mining.
    """
    try:
        from celery.result import AsyncResult
        
        result = AsyncResult(task_id)
        result.revoke(terminate=True)
        
        logger.info(f"Task mining {task_id} cancellato")
        return {"task_id": task_id, "cancelled": True, "timestamp": datetime.now().isoformat()}
        
    except Exception as e:
        logger.error(f"Errore cancellazione task mining {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# HUBSPOT CONFIGURATION ENDPOINTS (ex routes_discovery)
# =============================================================================

@router.post("/config/run")
async def run_discovery(request: DiscoveryRequestSchema):
    """
    Esegue la discovery automatica della configurazione HubSpot.
    """
    try:
        logger.info("Richiesta discovery automatica")
        
        # Esegui discovery
        results = auto_discovery_service.run_full_discovery()
        
        if not results.get('success', False):
            raise HTTPException(status_code=500, detail=f"Discovery fallita: {results.get('error', 'Errore sconosciuto')}")
        
        # Salva risultati se richiesto
        if request.save_results:
            filepath = auto_discovery_service.save_discovery_results(results)
            results['saved_to'] = str(filepath)
        
        # Applica configurazione se richiesto
        if request.apply_config:
            applied = auto_discovery_service.apply_discovery_config(results)
            results['config_applied'] = applied
        
        results['request_params'] = {
            'sample_size': request.sample_size,
            'save_results': request.save_results,
            'apply_config': request.apply_config
        }
        
        logger.info("Discovery automatica completata")
        return results
        
    except Exception as e:
        logger.error(f"Errore discovery automatica: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/config/current")
async def get_current_config():
    """
    Ottiene la configurazione HubSpot corrente.
    """
    try:
        config = hubspot_config_manager.config
        validation = hubspot_config_manager.validate_config()
        
        config_data = {
            "config": {
                "version": config.version,
                "stage_mappings": [stage.__dict__ for stage in config.stage_mappings],
                "data_structure": config.data_structure.__dict__,
                "custom_properties": config.custom_properties,
                "required_properties": config.required_properties,
                "privacy_fields": config.privacy_fields
            },
            "validation": validation,
            "timestamp": datetime.now().isoformat()
        }
        
        return config_data
        
    except Exception as e:
        logger.error(f"Errore recupero configurazione: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/config/update")
async def update_config(request: ConfigUpdateRequestSchema):
    """
    Aggiorna la configurazione HubSpot.
    """
    try:
        logger.info("Richiesta aggiornamento configurazione")
        
        # Aggiorna stage mappings se forniti
        if request.stage_mappings:
            for stage_data in request.stage_mappings:
                hubspot_config_manager.add_stage_mapping(
                    stage_data['stage_id'],
                    stage_data['display_name'],
                    stage_data['order'],
                    stage_data.get('is_final', False)
                )
        
        # Aggiorna data structure se fornito
        if request.data_structure:
            hubspot_config_manager.update_data_structure(**request.data_structure)
        
        # Aggiorna proprietà personalizzate
        if request.custom_properties:
            for prop in request.custom_properties:
                hubspot_config_manager.add_custom_property(prop)
        
        # Aggiorna proprietà richieste
        if request.required_properties:
            for prop in request.required_properties:
                if prop not in hubspot_config_manager.config.required_properties:
                    hubspot_config_manager.config.required_properties.append(prop)
        
        # Aggiorna campi privacy
        if request.privacy_fields:
            for field in request.privacy_fields:
                if field not in hubspot_config_manager.config.privacy_fields:
                    hubspot_config_manager.config.privacy_fields.append(field)
        
        # Salva configurazione aggiornata
        hubspot_config_manager.save_config(hubspot_config_manager.config)
        
        validation = hubspot_config_manager.validate_config()
        
        result = {
            "success": True,
            "message": "Configurazione aggiornata con successo",
            "validation": validation,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info("Configurazione aggiornata")
        return result
        
    except Exception as e:
        logger.error(f"Errore aggiornamento configurazione: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/config/validate")
async def validate_config():
    """
    Valida la configurazione HubSpot corrente.
    """
    try:
        validation = hubspot_config_manager.validate_config()
        
        result = {
            "validation": validation,
            "timestamp": datetime.now().isoformat()
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Errore validazione configurazione: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/config/stages")
async def get_stage_mappings():
    """
    Ottiene tutte le fasi della pipeline configurate.
    """
    try:
        stage_mappings = hubspot_config_manager.config.stage_mappings
        stages_data = []
        
        for stage in stage_mappings:
            stages_data.append({
                "stage_id": stage.stage_id,
                "display_name": stage.display_name,
                "order": stage.order,
                "is_final": stage.is_final
            })
        
        result = {
            "stages": stages_data,
            "count": len(stages_data),
            "timestamp": datetime.now().isoformat()
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Errore recupero fasi pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/config/properties")
async def get_properties():
    """
    Ottiene le proprietà configurate.
    """
    try:
        config = hubspot_config_manager.config
        
        properties_data = {
            "custom_properties": config.custom_properties,
            "required_properties": config.required_properties,
            "privacy_fields": config.privacy_fields,
            "data_structure": config.data_structure.__dict__,
            "timestamp": datetime.now().isoformat()
        }
        
        return properties_data
        
    except Exception as e:
        logger.error(f"Errore recupero proprietà: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/config/stages/add")
async def add_stage_mapping(stage_id: str, display_name: str, order: int, is_final: bool = False):
    """
    Aggiunge una nuova fase della pipeline.
    """
    try:
        logger.info(f"Aggiunta fase pipeline: {stage_id} -> {display_name}")
        
        hubspot_config_manager.add_stage_mapping(stage_id, display_name, order, is_final)
        
        # Salva configurazione
        hubspot_config_manager.save_config(hubspot_config_manager.config)
        
        result = {
            "success": True,
            "message": f"Fase {stage_id} aggiunta con successo",
            "stage": {
                "stage_id": stage_id,
                "display_name": display_name,
                "order": order,
                "is_final": is_final
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Errore aggiunta fase pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/config/stages/{stage_id}")
async def remove_stage_mapping(stage_id: str):
    """
    Rimuove una fase della pipeline.
    """
    try:
        logger.info(f"Rimozione fase pipeline: {stage_id}")
        
        # Filtra la fase da rimuovere
        hubspot_config_manager.config.stage_mappings = [
            stage for stage in hubspot_config_manager.config.stage_mappings
            if stage.stage_id != stage_id
        ]
        
        # Salva configurazione
        hubspot_config_manager.save_config(hubspot_config_manager.config)
        
        result = {
            "success": True,
            "message": f"Fase {stage_id} rimossa con successo",
            "removed_stage_id": stage_id,
            "timestamp": datetime.now().isoformat()
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Errore rimozione fase pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/config/data-structure")
async def get_data_structure():
    """
    Ottiene la struttura dati configurata.
    """
    try:
        data_structure = hubspot_config_manager.get_data_structure()
        
        structure_data = {
            "data_structure": data_structure.__dict__,
            "timestamp": datetime.now().isoformat()
        }
        
        return structure_data
        
    except Exception as e:
        logger.error(f"Errore recupero struttura dati: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/config/data-structure")
async def update_data_structure(data_structure: Dict[str, str]):
    """
    Aggiorna la struttura dati.
    """
    try:
        logger.info("Aggiornamento struttura dati")
        
        hubspot_config_manager.update_data_structure(**data_structure)
        
        # Salva configurazione
        hubspot_config_manager.save_config(hubspot_config_manager.config)
        
        result = {
            "success": True,
            "message": "Struttura dati aggiornata con successo",
            "data_structure": data_structure,
            "timestamp": datetime.now().isoformat()
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Errore aggiornamento struttura dati: {e}")
        raise HTTPException(status_code=500, detail=str(e))
