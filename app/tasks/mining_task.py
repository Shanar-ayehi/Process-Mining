from typing import Dict, List, Any, Optional, Union
from celery import chain, group
from app.tasks.base_task import (
    celery_app, mining_task, create_task_metadata, create_task_result, 
    validate_task_input, handle_task_error
)
from app.services.mining.discovery_service import discovery_service
from app.services.mining.conformance_service import conformance_service
from app.services.mining.kpi_service import kpi_service
from app.core.logger import get_logger

logger = get_logger()

@mining_task()
def discover_process_model_task(self, event_log_df: Any, 
                               algorithm: str = 'dfg',
                               parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Task per la scoperta del modello di processo.
    
    Args:
        event_log_df: DataFrame event log
        algorithm: Algoritmo di discovery ('dfg', 'alpha', 'heuristic', 'inductive')
        parameters: Parametri specifici per l'algoritmo
        
    Returns:
        Dizionario con risultati discovery
    """
    try:
        logger.info(f"Inizio task discovery modello processo ({algorithm})")
        
        # Seleziona algoritmo
        if algorithm == 'dfg':
            result_data = discovery_service.discover_dfg(event_log_df, parameters.get('output_image_path'))
        elif algorithm == 'alpha':
            result_data = discovery_service.discover_alpha_miner(event_log_df)
        elif algorithm == 'heuristic':
            result_data = discovery_service.discover_heuristic_miner(
                event_log_df, 
                parameters.get('dependency_threshold', 0.5)
            )
        elif algorithm == 'inductive':
            result_data = discovery_service.discover_inductive_miner(event_log_df)
        else:
            raise ValueError(f"Algoritmo non supportato: {algorithm}")
        
        result = create_task_result(
            success=True,
            data={
                'algorithm': algorithm,
                'discovery_result': result_data,
                'metadata': create_task_metadata('discover_process_model', algorithm=algorithm)
            }
        )
        
        logger.info(f"Task discovery modello processo completato ({algorithm})")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task discovery modello processo: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@mining_task()
def discover_variants_task(self, event_log_df: Any, 
                          min_frequency_threshold: float = 0.05) -> Dict[str, Any]:
    """
    Task per la scoperta delle varianti del processo.
    
    Args:
        event_log_df: DataFrame event log
        min_frequency_threshold: Soglia minima di frequenza
        
    Returns:
        Dizionario con risultati varianti
    """
    try:
        logger.info(f"Inizio task scoperta varianti (threshold: {min_frequency_threshold})")
        
        variants_result = discovery_service.discover_variants(
            event_log_df, min_frequency_threshold
        )
        
        result = create_task_result(
            success=True,
            data={
                'variants_count': len(variants_result['variants']),
                'covered_cases': variants_result['statistics']['covered_cases'],
                'coverage_percentage': variants_result['statistics']['coverage_percentage'],
                'variants_result': variants_result,
                'metadata': create_task_metadata('discover_variants', variants_count=len(variants_result['variants']))
            }
        )
        
        logger.info(f"Task scoperta varianti completato: {len(variants_result['variants'])} varianti")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task scoperta varianti: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@mining_task()
def discover_performance_dfg_task(self, event_log_df: Any) -> Dict[str, Any]:
    """
    Task per la scoperta del DFG con informazioni di performance.
    
    Args:
        event_log_df: DataFrame event log
        
    Returns:
        Dizionario con risultati performance DFG
    """
    try:
        logger.info("Inizio task scoperta performance DFG")
        
        performance_result = discovery_service.discover_performance_dfg(event_log_df)
        
        result = create_task_result(
            success=True,
            data={
                'performance_dfg': performance_result['performance_dfg'],
                'avg_duration_mean': performance_result['statistics'].get('avg_duration_mean', 0),
                'fastest_transition': performance_result['statistics'].get('fastest_transition'),
                'slowest_transition': performance_result['statistics'].get('slowest_transition'),
                'metadata': create_task_metadata('discover_performance_dfg')
            }
        )
        
        logger.info("Task scoperta performance DFG completato")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task scoperta performance DFG: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@mining_task()
def check_conformance_task(self, event_log_df: Any,
                          model_type: str = 'dfg',
                          theoretical_model: Optional[Any] = None) -> Dict[str, Any]:
    """
    Task per il conformance checking.
    
    Args:
        event_log_df: DataFrame event log
        model_type: Tipo di modello ('dfg', 'petri_net', 'declare')
        theoretical_model: Modello teorico (opzionale)
        
    Returns:
        Dizionario con risultati conformance
    """
    try:
        logger.info(f"Inizio task conformance checking ({model_type})")
        
        # Seleziona tipo di conformance checking
        if model_type == 'dfg':
            conformance_result = conformance_service.check_conformance_dfg(
                event_log_df, theoretical_model
            )
        elif model_type == 'petri_net':
            conformance_result = conformance_service.check_conformance_petri_net(
                event_log_df, theoretical_model
            )
        elif model_type == 'declare':
            conformance_result = conformance_service.check_conformance_declare(
                event_log_df, theoretical_model
            )
        else:
            raise ValueError(f"Tipo di modello non supportato: {model_type}")
        
        # Calcola fitness e precision
        fitness_precision = conformance_service.calculate_fitness_precision(
            event_log_df, model_type
        )
        
        result = create_task_result(
            success=True,
            data={
                'model_type': model_type,
                'conformance_result': conformance_result,
                'fitness_precision': fitness_precision,
                'metadata': create_task_metadata('check_conformance', model_type=model_type)
            }
        )
        
        logger.info(f"Task conformance checking completato ({model_type})")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task conformance checking: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@mining_task()
def detect_deviations_task(self, event_log_df: Any, 
                          conformance_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Task per il rilevamento pattern di deviazione.
    
    Args:
        event_log_df: DataFrame event log
        conformance_result: Risultati conformance checking
        
    Returns:
        Dizionario con risultati deviazioni
    """
    try:
        logger.info("Inizio task rilevamento deviazioni")
        
        deviation_patterns = conformance_service.detect_deviation_patterns(
            event_log_df, conformance_result
        )
        
        result = create_task_result(
            success=True,
            data={
                'deviating_cases_count': len(deviation_patterns['deviating_cases']),
                'common_deviation_types': deviation_patterns['common_deviation_types'],
                'root_cause_analysis': deviation_patterns['root_cause_analysis'],
                'deviation_patterns': deviation_patterns,
                'metadata': create_task_metadata('detect_deviations', deviating_cases=len(deviation_patterns['deviating_cases']))
            }
        )
        
        logger.info(f"Task rilevamento deviazioni completato: {len(deviation_patterns['deviating_cases'])} casi devianti")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task rilevamento deviazioni: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@mining_task()
def calculate_process_kpis_task(self, event_log_df: Any) -> Dict[str, Any]:
    """
    Task per il calcolo KPI principali del processo.
    
    Args:
        event_log_df: DataFrame event log
        
    Returns:
        Dizionario con risultati KPI
    """
    try:
        logger.info("Inizio task calcolo KPI processo")
        
        process_kpis = kpi_service.calculate_process_kpis(event_log_df)
        
        result = create_task_result(
            success=True,
            data={
                'overall_score': process_kpis['overall_score'],
                'basic_kpis': process_kpis['basic_kpis'],
                'performance_kpis': process_kpis['performance_kpis'],
                'quality_kpis': process_kpis['quality_kpis'],
                'efficiency_kpis': process_kpis['efficiency_kpis'],
                'variability_kpis': process_kpis['variability_kpis'],
                'process_kpis': process_kpis,
                'metadata': create_task_metadata('calculate_process_kpis', overall_score=process_kpis['overall_score'])
            }
        )
        
        logger.info(f"Task calcolo KPI processo completato: punteggio {process_kpis['overall_score']}")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task calcolo KPI processo: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@mining_task()
def calculate_resource_kpis_task(self, event_log_df: Any) -> Dict[str, Any]:
    """
    Task per il calcolo KPI per risorsa.
    
    Args:
        event_log_df: DataFrame event log
        
    Returns:
        Dizionario con risultati KPI risorsa
    """
    try:
        logger.info("Inizio task calcolo KPI risorsa")
        
        resource_kpis = kpi_service.calculate_resource_kpis(event_log_df)
        
        result = create_task_result(
            success=True,
            data={
                'resources_count': len(resource_kpis),
                'resource_kpis': resource_kpis,
                'metadata': create_task_metadata('calculate_resource_kpis', resources_count=len(resource_kpis))
            }
        )
        
        logger.info(f"Task calcolo KPI risorsa completato: {len(resource_kpis)} risorse")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task calcolo KPI risorsa: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@mining_task()
def calculate_activity_kpis_task(self, event_log_df: Any) -> Dict[str, Any]:
    """
    Task per il calcolo KPI per attività.
    
    Args:
        event_log_df: DataFrame event log
        
    Returns:
        Dizionario con risultati KPI attività
    """
    try:
        logger.info("Inizio task calcolo KPI attività")
        
        activity_kpis = kpi_service.calculate_activity_kpis(event_log_df)
        
        result = create_task_result(
            success=True,
            data={
                'activities_count': len(activity_kpis),
                'activity_kpis': activity_kpis,
                'metadata': create_task_metadata('calculate_activity_kpis', activities_count=len(activity_kpis))
            }
        )
        
        logger.info(f"Task calcolo KPI attività completato: {len(activity_kpis)} attività")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task calcolo KPI attività: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@mining_task()
def calculate_trend_kpis_task(self, event_log_df: Any, 
                             time_window: str = '1d') -> Dict[str, Any]:
    """
    Task per il calcolo KPI di trend temporale.
    
    Args:
        event_log_df: DataFrame event log
        time_window: Finestra temporale ('1d', '1w', '1m')
        
    Returns:
        Dizionario con risultati KPI trend
    """
    try:
        logger.info(f"Inizio task calcolo KPI trend ({time_window})")
        
        trend_kpis = kpi_service.calculate_trend_kpis(event_log_df, time_window)
        
        result = create_task_result(
            success=True,
            data={
                'time_window': time_window,
                'data_points': trend_kpis['data_points'],
                'cases_trend': trend_kpis['cases_trend'],
                'activities_trend': trend_kpis['activities_trend'],
                'resources_trend': trend_kpis['resources_trend'],
                'summary': trend_kpis['summary'],
                'trend_kpis': trend_kpis,
                'metadata': create_task_metadata('calculate_trend_kpis', time_window=time_window, data_points=trend_kpis['data_points'])
            }
        )
        
        logger.info(f"Task calcolo KPI trend completato: {trend_kpis['data_points']} periodi")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task calcolo KPI trend: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@mining_task()
def run_full_mining_analysis_task(self, event_log_df: Any,
                                discovery_algorithms: List[str] = ['dfg'],
                                conformance_model_type: str = 'dfg',
                                calculate_kpis: bool = True) -> Dict[str, Any]:
    """
    Task orchestratore per l'intera analisi mining.
    
    Args:
        event_log_df: DataFrame event log
        discovery_algorithms: Algoritmi di discovery da eseguire
        conformance_model_type: Tipo di modello per conformance checking
        calculate_kpis: Se calcolare i KPI
        
    Returns:
        Dizionario con risultati analisi completa
    """
    try:
        logger.info("Inizio analisi mining completa")
        
        analysis_results = {
            'discovery_results': {},
            'conformance_results': {},
            'kpis_results': {},
            'metadata': create_task_metadata('full_mining_analysis')
        }
        
        # Discovery
        for algorithm in discovery_algorithms:
            try:
                discovery_result = discovery_service.discover_dfg(event_log_df)
                analysis_results['discovery_results'][algorithm] = discovery_result
            except Exception as e:
                logger.warning(f"Errore discovery {algorithm}: {e}")
        
        # Conformance checking
        try:
            conformance_result = conformance_service.check_conformance_dfg(event_log_df)
            analysis_results['conformance_results'] = conformance_result
        except Exception as e:
            logger.warning(f"Errore conformance checking: {e}")
        
        # KPI calculation
        if calculate_kpis:
            try:
                process_kpis = kpi_service.calculate_process_kpis(event_log_df)
                resource_kpis = kpi_service.calculate_resource_kpis(event_log_df)
                activity_kpis = kpi_service.calculate_activity_kpis(event_log_df)
                
                analysis_results['kpis_results'] = {
                    'process_kpis': process_kpis,
                    'resource_kpis': resource_kpis,
                    'activity_kpis': activity_kpis
                }
            except Exception as e:
                logger.warning(f"Errore calcolo KPI: {e}")
        
        result = create_task_result(
            success=True,
            data={
                'analysis_results': analysis_results,
                'discovery_algorithms': discovery_algorithms,
                'conformance_model_type': conformance_model_type,
                'kpis_calculated': calculate_kpis,
                'metadata': create_task_metadata('full_mining_analysis', **analysis_results['metadata'])
            }
        )
        
        logger.info("Analisi mining completa completata")
        return result
        
    except Exception as e:
        logger.error(f"Errore nell'analisi mining completa: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@mining_task()
def generate_mining_report_task(self, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Task per la generazione di un report mining completo.
    
    Args:
        analysis_results: Risultati dell'analisi mining
        
    Returns:
        Dizionario con report generato
    """
    try:
        logger.info("Inizio generazione report mining")
        
        # Estrai informazioni principali
        discovery_summary = {
            'algorithms_used': list(analysis_results.get('discovery_results', {}).keys()),
            'models_discovered': len(analysis_results.get('discovery_results', {}))
        }
        
        conformance_summary = {
            'conformance_checked': 'conformance_results' in analysis_results,
            'fitness': analysis_results.get('conformance_results', {}).get('fitness_precision', {}).get('fitness', 0)
        }
        
        kpis_summary = {
            'kpis_calculated': 'kpis_results' in analysis_results,
            'overall_score': analysis_results.get('kpis_results', {}).get('process_kpis', {}).get('overall_score', 0)
        }
        
        report = {
            'report_timestamp': create_task_metadata('mining_report')['timestamp'],
            'discovery_summary': discovery_summary,
            'conformance_summary': conformance_summary,
            'kpis_summary': kpis_summary,
            'detailed_results': analysis_results,
            'recommendations': self._generate_recommendations(analysis_results)
        }
        
        result = create_task_result(
            success=True,
            data={
                'report': report,
                'report_summary': {
                    'discovery_algorithms': discovery_summary['algorithms_used'],
                    'conformance_fitness': conformance_summary['fitness'],
                    'overall_kpi_score': kpis_summary['overall_score']
                },
                'metadata': create_task_metadata('generate_mining_report')
            }
        )
        
        logger.info("Generazione report mining completata")
        return result
        
    except Exception as e:
        logger.error(f"Errore nella generazione report mining: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

def _generate_recommendations(self, analysis_results: Dict[str, Any]) -> List[str]:
    """Genera raccomandazioni basate sui risultati dell'analisi."""
    recommendations = []
    
    # Raccomandazioni basate su KPI
    kpis = analysis_results.get('kpis_results', {}).get('process_kpis', {})
    overall_score = kpis.get('overall_score', 0)
    
    if overall_score < 0.5:
        recommendations.append("Processo con bassa qualità: revisione completa necessaria")
    elif overall_score < 0.7:
        recommendations.append("Processo accettabile ma migliorabile")
    elif overall_score < 0.9:
        recommendations.append("Buon processo, piccoli miglioramenti possibili")
    else:
        recommendations.append("Eccellente qualità del processo")
    
    # Raccomandazioni basate su conformance
    conformance = analysis_results.get('conformance_results', {})
    fitness = conformance.get('fitness_precision', {}).get('fitness', 1.0)
    
    if fitness < 0.8:
        recommendations.append("Bassa fitness: modello teorico non rappresenta bene il processo reale")
    
    return recommendations

# Task helper per la gestione delle analisi
@mining_task()
def get_mining_analysis_status(self, analysis_id: str) -> Dict[str, Any]:
    """
    Ottiene lo stato di un'analisi mining.
    
    Args:
        analysis_id: ID dell'analisi
        
    Returns:
        Dizionario con stato analisi
    """
    try:
        from celery.result import AsyncResult
        
        result = AsyncResult(analysis_id)
        
        status_data = {
            'analysis_id': analysis_id,
            'status': result.status,
            'ready': result.ready(),
            'successful': result.successful() if result.ready() else None,
            'result': result.result if result.ready() else None
        }
        
        logger.info(f"Stato analisi {analysis_id}: {status_data}")
        return create_task_result(success=True, data=status_data)
        
    except Exception as e:
        logger.error(f"Errore nel recupero stato analisi {analysis_id}: {e}")
        return create_task_result(success=False, error=str(e))

# Creazione istanza globale
mining_task_instance = mining_task()