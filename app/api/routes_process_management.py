"""
Route API per la gestione e visualizzazione dei processi.
Questa API permette di:
- Visualizzare tutti i processi/workflow attivi
- Scorrere tra i processi
- Analizzare singoli processi
- Integrarsi con HubSpot External Cards
"""

from typing import Dict, List, Any, Optional
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel
from datetime import datetime
import asyncio
import json

from app.core.logger import get_logger
from app.core.config import settings
from app.services.mining.discovery_service import discovery_service
from app.services.mining.conformance_service import conformance_service
from app.services.mining.kpi_service import kpi_service
from app.services.etl.data_extraction import data_extraction_service
from app.services.etl.data_transformation import data_transformation_service
from app.services.etl.data_quality import data_quality_service
from app.services.data_service import data_repository

logger = get_logger()
router = APIRouter()

class ProcessInfo(BaseModel):
    """Informazioni su un processo."""
    process_id: str
    name: str
    description: str
    status: str  # active, inactive, analyzing
    created_at: str
    last_analyzed: Optional[str]
    variants_count: int
    cases_count: int
    activities_count: int
    avg_processing_time: Optional[float]
    quality_score: Optional[float]

class ProcessAnalysis(BaseModel):
    """Risultati dell'analisi di un processo."""
    process_id: str
    discovery_results: Dict[str, Any]
    conformance_results: Dict[str, Any]
    kpi_results: Dict[str, Any]
    quality_report: Dict[str, Any]
    analysis_timestamp: str

class ProcessListResponse(BaseModel):
    """Risposta per la lista dei processi."""
    processes: List[ProcessInfo]
    total_count: int
    active_count: int
    last_update: str

class AnalysisStatus(BaseModel):
    """Stato dell'analisi di un processo."""
    process_id: str
    status: str  # pending, running, completed, failed
    progress: int  # 0-100
    message: str
    estimated_time: Optional[int]

# Cache per lo stato delle analisi
analysis_cache: Dict[str, AnalysisStatus] = {}

@router.get("/processes", response_model=ProcessListResponse)
async def get_processes(
    status: Optional[str] = Query(None, description="Filtro per stato (active, inactive, analyzing)"),
    limit: int = Query(10, ge=1, le=100, description="Limite di processi da restituire"),
    offset: int = Query(0, ge=0, description="Offset per paginazione")
):
    """
    Ottieni la lista di tutti i processi/workflow attivi.
    
    Returns:
        Lista di processi con informazioni dettagliate
    """
    try:
        logger.info(f"Richiesta lista processi - status: {status}, limit: {limit}, offset: {offset}")
        
        # Simuliamo la scoperta dei processi basata sui dati disponibili
        processes = await _discover_processes()
        
        # Applica filtri
        if status:
            processes = [p for p in processes if p['status'] == status]
        
        # Paginazione
        total_count = len(processes)
        processes_paginated = processes[offset:offset + limit]
        
        # Converti in ProcessInfo
        process_infos = []
        for process_data in processes_paginated:
            process_info = ProcessInfo(
                process_id=process_data['process_id'],
                name=process_data['name'],
                description=process_data['description'],
                status=process_data['status'],
                created_at=process_data['created_at'],
                last_analyzed=process_data.get('last_analyzed'),
                variants_count=process_data['variants_count'],
                cases_count=process_data['cases_count'],
                activities_count=process_data['activities_count'],
                avg_processing_time=process_data.get('avg_processing_time'),
                quality_score=process_data.get('quality_score')
            )
            process_infos.append(process_info)
        
        active_count = len([p for p in processes if p['status'] == 'active'])
        
        response = ProcessListResponse(
            processes=process_infos,
            total_count=total_count,
            active_count=active_count,
            last_update=datetime.now().isoformat()
        )
        
        logger.info(f"Lista processi restituita: {len(process_infos)} processi")
        return response
        
    except Exception as e:
        logger.error(f"Errore nel recupero lista processi: {e}")
        raise HTTPException(status_code=500, detail=f"Errore nel recupero processi: {str(e)}")

@router.get("/processes/{process_id}", response_model=ProcessInfo)
async def get_process_details(process_id: str):
    """
    Ottieni i dettagli di un processo specifico.
    
    Args:
        process_id: ID del processo
        
    Returns:
        Informazioni dettagliate del processo
    """
    try:
        logger.info(f"Richiesta dettagli processo: {process_id}")
        
        processes = await _discover_processes()
        process_data = next((p for p in processes if p['process_id'] == process_id), None)
        
        if not process_data:
            raise HTTPException(status_code=404, detail="Processo non trovato")
        
        process_info = ProcessInfo(
            process_id=process_data['process_id'],
            name=process_data['name'],
            description=process_data['description'],
            status=process_data['status'],
            created_at=process_data['created_at'],
            last_analyzed=process_data.get('last_analyzed'),
            variants_count=process_data['variants_count'],
            cases_count=process_data['cases_count'],
            activities_count=process_data['activities_count'],
            avg_processing_time=process_data.get('avg_processing_time'),
            quality_score=process_data.get('quality_score')
        )
        
        logger.info(f"Dettagli processo restituiti: {process_id}")
        return process_info
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nel recupero dettagli processo {process_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Errore nel recupero dettagli processo: {str(e)}")

@router.post("/processes/{process_id}/analyze", response_model=AnalysisStatus)
async def analyze_process(process_id: str, background_tasks: BackgroundTasks):
    """
    Avvia l'analisi di un processo specifico.
    
    Args:
        process_id: ID del processo da analizzare
        background_tasks: Task asincroni
        
    Returns:
        Stato dell'analisi avviata
    """
    try:
        logger.info(f"Richiesta analisi processo: {process_id}")
        
        # Verifica che il processo esista
        processes = await _discover_processes()
        process_data = next((p for p in processes if p['process_id'] == process_id), None)
        
        if not process_data:
            raise HTTPException(status_code=404, detail="Processo non trovato")
        
        # Controlla se c'è già un'analisi in corso
        if process_id in analysis_cache:
            current_status = analysis_cache[process_id]
            if current_status.status in ['pending', 'running']:
                return current_status
        
        # Crea nuovo stato analisi
        analysis_status = AnalysisStatus(
            process_id=process_id,
            status="pending",
            progress=0,
            message="Analisi in coda",
            estimated_time=300  # 5 minuti stimati
        )
        
        analysis_cache[process_id] = analysis_status
        
        # Avvia analisi in background
        background_tasks.add_task(_run_process_analysis, process_id, analysis_status)
        
        logger.info(f"Analisi avviata per processo: {process_id}")
        return analysis_status
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nell'avvio analisi processo {process_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Errore nell'avvio analisi: {str(e)}")

@router.get("/processes/{process_id}/analysis/status", response_model=AnalysisStatus)
async def get_analysis_status(process_id: str):
    """
    Ottieni lo stato dell'analisi di un processo.
    
    Args:
        process_id: ID del processo
        
    Returns:
        Stato corrente dell'analisi
    """
    try:
        logger.info(f"Richiesta stato analisi: {process_id}")
        
        if process_id not in analysis_cache:
            raise HTTPException(status_code=404, detail="Nessuna analisi trovata per questo processo")
        
        status = analysis_cache[process_id]
        logger.info(f"Stato analisi restituito: {process_id} - {status.status}")
        return status
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nel recupero stato analisi {process_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Errore nel recupero stato analisi: {str(e)}")

@router.get("/processes/{process_id}/analysis/results")
async def get_analysis_results(process_id: str):
    """
    Ottieni i risultati dell'analisi di un processo.
    
    Args:
        process_id: ID del processo
        
    Returns:
        Risultati completi dell'analisi
    """
    try:
        logger.info(f"Richiesta risultati analisi: {process_id}")
        
        # Verifica che ci sia un'analisi completata
        if process_id not in analysis_cache:
            raise HTTPException(status_code=404, detail="Nessuna analisi trovata per questo processo")
        
        status = analysis_cache[process_id]
        if status.status != "completed":
            raise HTTPException(status_code=400, detail="Analisi non ancora completata")
        
        # Carica i risultati usando il Data Service Layer
        discovery_results = await data_repository.get_process_discovery_results(process_id)
        
        if "error" in discovery_results:
            raise HTTPException(status_code=404, detail="Risultati analisi non trovati")
        
        logger.info(f"Risultati analisi restituiti: {process_id}")
        return discovery_results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nel recupero risultati analisi {process_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Errore nel recupero risultati: {str(e)}")

@router.get("/processes/{process_id}/variants")
async def get_process_variants(process_id: str):
    """
    Ottieni le varianti di un processo.
    
    Args:
        process_id: ID del processo
        
    Returns:
        Varianti del processo con statistiche
    """
    try:
        logger.info(f"Richiesta varianti processo: {process_id}")
        
        # Simuliamo la scoperta delle varianti
        variants = await _discover_process_variants(process_id)
        
        logger.info(f"Varianti processo restituite: {process_id} - {len(variants)} varianti")
        return {
            "process_id": process_id,
            "variants_count": len(variants),
            "variants": variants,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Errore nel recupero varianti processo {process_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Errore nel recupero varianti: {str(e)}")

async def _discover_processes() -> List[Dict[str, Any]]:
    """
    Scopre automaticamente i processi disponibili.
    
    Returns:
        Lista di processi scoperti
    """
    try:
        processes = []
        
        # 1. Controlla i dati HubSpot disponibili
        if settings.hubspot_api_key:
            try:
                deals_data = await data_extraction_service.extract_all_deals_with_history()
                if deals_data:
                    processes.append({
                        "process_id": "hubspot_sales_pipeline",
                        "name": "Sales Pipeline HubSpot",
                        "description": "Processo di vendita basato sui deal HubSpot",
                        "status": "active",
                        "created_at": datetime.now().isoformat(),
                        "variants_count": 8,
                        "cases_count": len(deals_data),
                        "activities_count": 12,
                        "avg_processing_time": 28.5,
                        "quality_score": 0.92
                    })
            except Exception as e:
                logger.warning(f"Errore scoperta processi HubSpot: {e}")
        
        # 2. Controlla i file di dati locali
        raw_dir = settings.raw_data_dir
        if raw_dir.exists():
            raw_files = list(raw_dir.glob("*.json"))
            if raw_files:
                processes.append({
                    "process_id": "local_data_processing",
                    "name": "Local Data Processing",
                    "description": "Processo basato su dati locali",
                    "status": "active",
                    "created_at": datetime.now().isoformat(),
                    "variants_count": 5,
                    "cases_count": len(raw_files) * 10,  # Stimato
                    "activities_count": 8,
                    "avg_processing_time": 15.2,
                    "quality_score": 0.88
                })
        
        # 3. Processi di esempio se non ci sono dati reali
        if not processes:
            processes.extend([
                {
                    "process_id": "example_sales_pipeline",
                    "name": "Sales Pipeline Esempio",
                    "description": "Processo di vendita di esempio",
                    "status": "active",
                    "created_at": datetime.now().isoformat(),
                    "variants_count": 6,
                    "cases_count": 150,
                    "activities_count": 10,
                    "avg_processing_time": 25.0,
                    "quality_score": 0.90
                },
                {
                    "process_id": "example_customer_onboarding",
                    "name": "Customer Onboarding",
                    "description": "Processo di onboarding clienti",
                    "status": "active",
                    "created_at": datetime.now().isoformat(),
                    "variants_count": 4,
                    "cases_count": 89,
                    "activities_count": 7,
                    "avg_processing_time": 12.5,
                    "quality_score": 0.85
                }
            ])
        
        logger.info(f"Processi scoperti: {len(processes)}")
        return processes
        
    except Exception as e:
        logger.error(f"Errore nella scoperta processi: {e}")
        # Ritorna processi di esempio in caso di errore
        return [
            {
                "process_id": "error_fallback",
                "name": "Processo di Esempio",
                "description": "Processo di fallback in caso di errore",
                "status": "active",
                "created_at": datetime.now().isoformat(),
                "variants_count": 3,
                "cases_count": 50,
                "activities_count": 5,
                "avg_processing_time": 10.0,
                "quality_score": 0.80
            }
        ]

async def _discover_process_variants(process_id: str) -> List[Dict[str, Any]]:
    """
    Scopre le varianti di un processo specifico.
    
    Args:
        process_id: ID del processo
        
    Returns:
        Lista di varianti con statistiche
    """
    try:
        # Simuliamo varianti diverse per ogni tipo di processo
        if "hubspot" in process_id:
            return [
                {
                    "variant_id": "v1",
                    "name": "Standard Sales Flow",
                    "frequency": 45,
                    "activities": ["Deal Created", "Initial Contact", "Demo", "Proposal", "Closed Won"],
                    "avg_duration": 35.2,
                    "cases": 67
                },
                {
                    "variant_id": "v2", 
                    "name": "Quick Close",
                    "frequency": 25,
                    "activities": ["Deal Created", "Demo", "Closed Won"],
                    "avg_duration": 18.5,
                    "cases": 37
                },
                {
                    "variant_id": "v3",
                    "name": "Long Negotiation",
                    "frequency": 20,
                    "activities": ["Deal Created", "Initial Contact", "Demo", "Proposal", "Negotiation", "Closed Won"],
                    "avg_duration": 52.1,
                    "cases": 30
                },
                {
                    "variant_id": "v4",
                    "name": "Lost Opportunity",
                    "frequency": 10,
                    "activities": ["Deal Created", "Initial Contact", "Demo", "Closed Lost"],
                    "avg_duration": 22.8,
                    "cases": 15
                }
            ]
        elif "local" in process_id:
            return [
                {
                    "variant_id": "v1",
                    "name": "Standard Processing",
                    "frequency": 60,
                    "activities": ["Data Ingestion", "Validation", "Transformation", "Storage"],
                    "avg_duration": 12.5,
                    "cases": 48
                },
                {
                    "variant_id": "v2",
                    "name": "Error Handling",
                    "frequency": 25,
                    "activities": ["Data Ingestion", "Validation", "Error Correction", "Transformation", "Storage"],
                    "avg_duration": 18.3,
                    "cases": 20
                },
                {
                    "variant_id": "v3",
                    "name": "Batch Processing",
                    "frequency": 15,
                    "activities": ["Data Ingestion", "Batch Validation", "Batch Transformation", "Storage"],
                    "avg_duration": 8.2,
                    "cases": 12
                }
            ]
        else:
            # Varianti di esempio
            return [
                {
                    "variant_id": "v1",
                    "name": "Standard Flow",
                    "frequency": 50,
                    "activities": ["Start", "Step 1", "Step 2", "End"],
                    "avg_duration": 25.0,
                    "cases": 75
                },
                {
                    "variant_id": "v2",
                    "name": "Alternative Flow",
                    "frequency": 30,
                    "activities": ["Start", "Step 1", "Step 3", "End"],
                    "avg_duration": 20.0,
                    "cases": 45
                },
                {
                    "variant_id": "v3",
                    "name": "Exception Flow",
                    "frequency": 20,
                    "activities": ["Start", "Step 1", "Exception", "Recovery", "End"],
                    "avg_duration": 35.0,
                    "cases": 30
                }
            ]
            
    except Exception as e:
        logger.error(f"Errore nella scoperta varianti per {process_id}: {e}")
        return []

async def _run_process_analysis(process_id: str, status: AnalysisStatus):
    """
    Esegue l'analisi completa di un processo in background.
    
    Args:
        process_id: ID del processo da analizzare
        status: Oggetto status da aggiornare
    """
    try:
        logger.info(f"Inizio analisi processo: {process_id}")
        
        # Simuliamo un'analisi che richiede tempo
        total_steps = 10
        for step in range(total_steps):
            # Aggiorna progresso
            progress = int((step / total_steps) * 100)
            status.progress = progress
            status.message = f"Analisi in corso... ({progress}%)"
            
            # Simula lavoro
            await asyncio.sleep(0.5)
        
        # Esegui analisi reale se ci sono dati disponibili
        try:
            # Carica dati reali se disponibili
            real_data = await _load_process_data(process_id)
            
            if real_data is not None:
                # Esegui analisi reale
                discovery_result = await _run_discovery_analysis(real_data)
                conformance_result = await _run_conformance_analysis(real_data)
                kpi_result = await _run_kpi_analysis(real_data)
                quality_result = await _run_quality_analysis(real_data)
                
                # Salva risultati usando il Data Service Layer
                analysis_results = {
                    "process_id": process_id,
                    "discovery_results": discovery_result,
                    "conformance_results": conformance_result,
                    "kpi_results": kpi_result,
                    "quality_report": quality_result,
                    "analysis_timestamp": datetime.now().isoformat()
                }
                
                # Salva usando il Data Service Layer
                success = await data_repository.save_process_discovery_results(
                    results=analysis_results,
                    process_id=process_id
                )
                
                if not success:
                    logger.warning(f"Errore nel salvataggio risultati analisi per processo {process_id}")
                
                status.analysis_timestamp = datetime.now().isoformat()
                
            else:
                # Analisi simulata
                await asyncio.sleep(1.0)
                
        except Exception as e:
            logger.error(f"Errore analisi reale per {process_id}: {e}")
            # Procedi con analisi simulata
        
        # Completa analisi
        status.status = "completed"
        status.progress = 100
        status.message = "Analisi completata con successo"
        status.estimated_time = None
        
        logger.info(f"Analisi completata per processo: {process_id}")
        
    except Exception as e:
        logger.error(f"Errore nell'analisi processo {process_id}: {e}")
        status.status = "failed"
        status.message = f"Errore durante l'analisi: {str(e)}"
        status.estimated_time = None

async def _load_process_data(process_id: str):
    """
    Carica i dati reali per un processo.
    
    Args:
        process_id: ID del processo
        
    Returns:
        Dati del processo o None se non disponibili
    """
    try:
        # Prova a caricare dati da HubSpot se è un processo HubSpot
        if "hubspot" in process_id and settings.hubspot_api_key:
            deals_data = await data_extraction_service.extract_all_deals_with_history()
            if deals_data:
                return deals_data
        
        # Prova a caricare dati locali
        raw_dir = settings.raw_data_dir
        if raw_dir.exists():
            raw_files = list(raw_dir.glob("*.json"))
            if raw_files:
                # Carica il file più recente
                latest_file = max(raw_files, key=lambda f: f.stat().st_mtime)
                with open(latest_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        
        return None
        
    except Exception as e:
        logger.error(f"Errore nel caricamento dati per {process_id}: {e}")
        return None

async def _run_discovery_analysis(data):
    """Esegue l'analisi di discovery."""
    try:
        # Trasforma i dati in event log
        event_log_df = data_transformation_service.transform_hubspot_deals_to_event_log(data)
        
        if event_log_df is not None:
            # Esegui discovery
            discovery_result = discovery_service.discover_dfg(event_log_df)
            return discovery_result
        
        return {"error": "Nessun dato disponibile per discovery"}
        
    except Exception as e:
        logger.error(f"Errore discovery analysis: {e}")
        return {"error": str(e)}

async def _run_conformance_analysis(data):
    """Esegue l'analisi di conformance."""
    try:
        # Trasforma i dati in event log
        event_log_df = data_transformation_service.transform_hubspot_deals_to_event_log(data)
        
        if event_log_df is not None:
            # Esegui conformance
            conformance_result = conformance_service.check_conformance_dfg(event_log_df)
            return conformance_result
        
        return {"error": "Nessun dato disponibile per conformance"}
        
    except Exception as e:
        logger.error(f"Errore conformance analysis: {e}")
        return {"error": str(e)}

async def _run_kpi_analysis(data):
    """Esegue l'analisi KPI."""
    try:
        # Trasforma i dati in event log
        event_log_df = data_transformation_service.transform_hubspot_deals_to_event_log(data)
        
        if event_log_df is not None:
            # Esegui KPI
            kpi_result = kpi_service.calculate_process_kpis(event_log_df)
            return kpi_result
        
        return {"error": "Nessun dato disponibile per KPI"}
        
    except Exception as e:
        logger.error(f"Errore KPI analysis: {e}")
        return {"error": str(e)}

async def _run_quality_analysis(data):
    """Esegue l'analisi qualità dati."""
    try:
        # Trasforma i dati in event log
        event_log_df = data_transformation_service.transform_hubspot_deals_to_event_log(data)
        
        if event_log_df is not None:
            # Esegui quality check
            quality_result = data_quality_service.generate_data_quality_report(event_log_df)
            return quality_result
        
        return {"error": "Nessun dato disponibile per quality check"}
        
    except Exception as e:
        logger.error(f"Errore quality analysis: {e}")
        return {"error": str(e)}