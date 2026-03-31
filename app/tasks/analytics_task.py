from typing import Dict, List, Any, Optional
from datetime import datetime
import polars as pl
from app.tasks.base_task import (
    celery_app, BaseTask, create_task_metadata, create_task_result,
    handle_task_error
)
from app.services.analytics.simulation_service import simulation_service
from app.services.mining.discovery_service import discovery_service
from app.core.logger import get_logger
from app.core.database import load_event_log

logger = get_logger()

def _load_event_log_for_portal(portal_id: str) -> Any:
    """
    Carica l'event log per un portal_id specifico.
    
    Args:
        portal_id: ID del portale HubSpot
        
    Returns:
        DataFrame Polars con i dati dell'event log
        
    Raises:
        ValueError: Se non ci sono dati sincronizzati per questo account
    """
    table_name = f"event_log_{portal_id}"
    df = load_event_log(table_name=table_name)
    
    if df.is_empty():
        raise ValueError(f"Nessun dato sincronizzato per questo account (portal_id: {portal_id})")
    
    logger.info(f"Caricati {len(df)} record per portal_id: {portal_id}")
    return df


@celery_app.task(bind=True, base=BaseTask, queue='mining')
def run_simulation_task(
    self,
    portal_id: str,
    num_cases: int = 100,
    modifications: Optional[Dict[str, Dict[str, float]]] = None,
    seed: int = 42,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict[str, Any]:
    """
    Task per eseguire What-If Analysis (simulazione processo).
    
    Args:
        portal_id: ID del portale HubSpot
        num_cases: Numero di casi da simulare
        modifications: Modifiche da applicare (es. {"Activity": {"time_multiplier": 0.8}})
        seed: Seed per riproducibilità
        start_date: Data inizio filtro (opzionale)
        end_date: Data fine filtro (opzionale)
        
    Returns:
        Dizionario con risultati simulazione
    """
    try:
        logger.info(f"Inizio task simulazione What-If per portal_id: {portal_id} ({num_cases} casi)")
        
        # Carica i dati dal database
        event_log_df = _load_event_log_for_portal(portal_id)
        
        # Applica filtri data se specificati
        if start_date:
            event_log_df = event_log_df.filter(
                pl.col("timestamp") >= pl.lit(start_date)
            )
        if end_date:
            event_log_df = event_log_df.filter(
                pl.col("timestamp") <= pl.lit(end_date)
            )
        
        # Scopri DFG e Performance
        dfg_result = discovery_service.discover_dfg(event_log_df)
        performance_result = discovery_service.discover_performance_dfg(event_log_df)
        
        # Estrai componenti necessari
        dfg = dfg_result.get('dfg', {})
        performance_dfg = performance_result.get('performance_dfg', {})
        start_activities = dfg_result.get('start_activities', {})
        end_activities = dfg_result.get('end_activities', {})
        
        # Esegui simulazione
        # SimPy è sincrono, quindi lo eseguiamo direttamente
        simulation_result = simulation_service.simulate_process(
            dfg=dfg,
            performance_dfg=performance_dfg,
            start_activities=start_activities,
            end_activities=end_activities,
            num_cases=num_cases,
            modifications=modifications
        )
        
        # Aggiungi seed ai risultati
        simulation_result['seed'] = seed
        simulation_result['portal_id'] = portal_id
        
        result = create_task_result(
            success=True,
            data={
                'portal_id': portal_id,
                'simulation_result': simulation_result,
                'metadata': create_task_metadata(
                    'run_simulation',
                    portal_id=portal_id,
                    num_cases=num_cases,
                    modifications=modifications or {}
                )
            }
        )
        
        logger.info(f"Task simulazione completato per portal_id: {portal_id}: {simulation_result.get('metrics', {})}")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task simulazione: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))


@celery_app.task(bind=True, base=BaseTask, queue='mining')
def compare_scenarios_task(
    self,
    portal_id: str,
    scenarios: List[Dict[str, Any]],
    num_cases: int = 100
) -> Dict[str, Any]:
    """
    Task per confrontare più scenari What-If.
    
    Args:
        portal_id: ID del portale HubSpot
        scenarios: Lista di scenari (ognuno con modifiche diverse)
        num_cases: Numero di casi per scenario
        
    Returns:
        Dizionario con confronto scenari
    """
    try:
        logger.info(f"Inizio task confronto scenari per portal_id: {portal_id} ({len(scenarios)} scenari)")
        
        # Carica i dati dal database
        event_log_df = _load_event_log_for_portal(portal_id)
        
        # Scopri DFG e Performance (una volta sola)
        dfg_result = discovery_service.discover_dfg(event_log_df)
        performance_result = discovery_service.discover_performance_dfg(event_log_df)
        
        dfg = dfg_result.get('dfg', {})
        performance_dfg = performance_result.get('performance_dfg', {})
        start_activities = dfg_result.get('start_activities', {})
        end_activities = dfg_result.get('end_activities', {})
        
        # Esegui simulazione per ogni scenario
        scenario_results = []
        for i, scenario in enumerate(scenarios):
            scenario_name = scenario.get('name', f'Scenario_{i+1}')
            modifications = scenario.get('modifications', {})
            
            logger.info(f"Esecuzione scenario: {scenario_name}")
            
            simulation_result = simulation_service.simulate_process(
                dfg=dfg,
                performance_dfg=performance_dfg,
                start_activities=start_activities,
                end_activities=end_activities,
                num_cases=num_cases,
                modifications=modifications
            )
            
            scenario_results.append({
                'scenario_name': scenario_name,
                'modifications': modifications,
                'metrics': simulation_result.get('metrics', {})
            })
        
        # Calcola confronto
        comparison = {
            'portal_id': portal_id,
            'num_scenarios': len(scenarios),
            'num_cases_per_scenario': num_cases,
            'scenarios': scenario_results,
            'timestamp': datetime.now().isoformat()
        }
        
        # Trova miglior scenario
        if scenario_results:
            best_scenario = min(
                scenario_results,
                key=lambda s: s['metrics'].get('avg_cycle_time', float('inf'))
            )
            comparison['best_scenario'] = best_scenario['scenario_name']
            comparison['best_avg_cycle_time'] = best_scenario['metrics'].get('avg_cycle_time', 0)
        
        result = create_task_result(
            success=True,
            data={
                'portal_id': portal_id,
                'comparison': comparison,
                'metadata': create_task_metadata(
                    'compare_scenarios',
                    portal_id=portal_id,
                    num_scenarios=len(scenarios)
                )
            }
        )
        
        logger.info(f"Task confronto scenari completato per portal_id: {portal_id}")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task confronto scenari: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))