import pm4py
import polars as pl
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime
from app.core.logger import get_logger
from app.core.config import settings

logger = get_logger()

class ConformanceService:
    """Servizio per il Conformance Checking con PM4Py."""
    
    def __init__(self):
        self.default_variant_threshold = settings.mining_default_variant_threshold
    
    def check_conformance_dfg(self, df: pl.DataFrame, 
                            theoretical_dfg: Optional[Dict] = None,
                            start_activities: Optional[Dict] = None,
                            end_activities: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Esegue conformance checking tra event log e DFG teorico.
        
        Args:
            df: DataFrame Polars con event log
            theoretical_dfg: DFG teorico (se None, usa DFG scoperto)
            start_activities: Attività di inizio teoriche
            end_activities: Attività di fine teoriche
            
        Returns:
            Dizionario con risultati conformance
        """
        logger.info("Avvio Conformance Checking (DFG)...")
        
        try:
            log = self._prepare_event_log(df)
            
            # Se non fornito, scopri il DFG teorico
            if theoretical_dfg is None:
                theoretical_dfg, start_activities, end_activities = pm4py.discover_dfg(log)
            
            # Esegui conformance checking
            aligned_traces = pm4py.conformance_diagnostics_alignments_tbr(
                log, theoretical_dfg, start_activities, end_activities
            )
            
            # Calcola metriche di conformità
            conformance_metrics = self._calculate_conformance_metrics(aligned_traces)
            
            result = {
                'theoretical_dfg': theoretical_dfg,
                'start_activities': start_activities,
                'end_activities': end_activities,
                'aligned_traces': aligned_traces,
                'conformance_metrics': conformance_metrics,
                'conformance_timestamp': datetime.now().isoformat()
            }
            
            logger.info("Conformance Checking (DFG) completato")
            return result
            
        except Exception as e:
            logger.error(f"Errore in Conformance Checking (DFG): {e}")
            raise
    
    def check_conformance_petri_net(self, df: pl.DataFrame, 
                                  petri_net: Optional[Any] = None,
                                  initial_marking: Optional[Any] = None,
                                  final_marking: Optional[Any] = None) -> Dict[str, Any]:
        """
        Esegue conformance checking tra event log e rete di Petri.
        
        Args:
            df: DataFrame Polars con event log
            petri_net: Rete di Petri teorica
            initial_marking: Marcatura iniziale
            final_marking: Marcatura finale
            
        Returns:
            Dizionario con risultati conformance
        """
        logger.info("Avvio Conformance Checking (Petri Net)...")
        
        try:
            log = self._prepare_event_log(df)
            
            # Se non fornita, scopri la rete di Petri
            if petri_net is None:
                petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(log)
            
            # Esegui conformance checking
            aligned_traces = pm4py.conformance_diagnostics_alignments_tbr(
                log, petri_net, initial_marking, final_marking
            )
            
            # Calcola metriche di conformità
            conformance_metrics = self._calculate_conformance_metrics(aligned_traces)
            
            result = {
                'petri_net': petri_net,
                'initial_marking': initial_marking,
                'final_marking': final_marking,
                'aligned_traces': aligned_traces,
                'conformance_metrics': conformance_metrics,
                'conformance_timestamp': datetime.now().isoformat()
            }
            
            logger.info("Conformance Checking (Petri Net) completato")
            return result
            
        except Exception as e:
            logger.error(f"Errore in Conformance Checking (Petri Net): {e}")
            raise
    
    def check_conformance_declare(self, df: pl.DataFrame, 
                                declare_model: Optional[Any] = None) -> Dict[str, Any]:
        """
        Esegue conformance checking con modello DECLARE.
        
        Args:
            df: DataFrame Polars con event log
            declare_model: Modello DECLARE (se None, viene scoperto)
            
        Returns:
            Dizionario con risultati conformance
        """
        logger.info("Avvio Conformance Checking (DECLARE)...")
        
        try:
            log = self._prepare_event_log(df)
            
            # Se non fornito, scopri il modello DECLARE
            if declare_model is None:
                declare_model = pm4py.discover_declare(log)
            
            # Esegui conformance checking
            conformance_results = pm4py.conformance_declare_tbr(log, declare_model)
            
            # Calcola metriche
            declare_metrics = self._calculate_declare_metrics(conformance_results)
            
            result = {
                'declare_model': declare_model,
                'conformance_results': conformance_results,
                'declare_metrics': declare_metrics,
                'conformance_timestamp': datetime.now().isoformat()
            }
            
            logger.info("Conformance Checking (DECLARE) completato")
            return result
            
        except Exception as e:
            logger.error(f"Errore in Conformance Checking (DECLARE): {e}")
            raise
    
    def detect_deviation_patterns(self, df: pl.DataFrame, 
                                 conformance_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Rileva pattern di deviazione dall'analisi di conformance.
        
        Args:
            df: DataFrame Polars con event log
            conformance_results: Risultati conformance checking
            
        Returns:
            Dizionario con pattern di deviazione
        """
        logger.info("Rilevamento pattern di deviazione...")
        
        try:
            deviation_patterns = {
                'timestamp': datetime.now().isoformat(),
                'total_cases': len(df['case_id'].unique()) if 'case_id' in df.columns else 0,
                'deviating_cases': [],
                'common_deviation_types': {},
                'deviation_frequency': {},
                'root_cause_analysis': []
            }
            
            # Analizza i risultati di conformance per trovare deviazioni
            if 'aligned_traces' in conformance_results:
                aligned_traces = conformance_results['aligned_traces']
                
                for case_id, alignment in aligned_traces.items():
                    if alignment['cost'] > 0:  # C'è una deviazione
                        deviation_patterns['deviating_cases'].append(case_id)
                        
                        # Analizza il tipo di deviazione
                        deviation_type = self._classify_deviation(alignment)
                        if deviation_type in deviation_patterns['common_deviation_types']:
                            deviation_patterns['common_deviation_types'][deviation_type] += 1
                        else:
                            deviation_patterns['common_deviation_types'][deviation_type] = 1
            
            # Calcola frequenze
            total_deviating = len(deviation_patterns['deviating_cases'])
            for deviation_type, count in deviation_patterns['common_deviation_types'].items():
                deviation_patterns['deviation_frequency'][deviation_type] = {
                    'count': count,
                    'percentage': (count / total_deviating * 100) if total_deviating > 0 else 0
                }
            
            # Analisi cause profonde
            deviation_patterns['root_cause_analysis'] = self._analyze_root_causes(df, deviation_patterns)
            
            logger.info(f"Rilevati {len(deviation_patterns['deviating_cases'])} casi devianti")
            return deviation_patterns
            
        except Exception as e:
            logger.error(f"Errore nel rilevamento pattern di deviazione: {e}")
            raise
    
    def calculate_fitness_precision(self, df: pl.DataFrame, 
                                   model_type: str = 'petri_net') -> Dict[str, float]:
        """
        Calcola fitness e precision del modello.
        
        Args:
            df: DataFrame Polars con event log
            model_type: Tipo di modello ('petri_net', 'dfg', 'declare')
            
        Returns:
            Dizionario con fitness e precision
        """
        logger.info(f"Calcolo fitness e precision ({model_type})...")
        
        try:
            log = self._prepare_event_log(df)
            
            if model_type == 'petri_net':
                # Scopri rete di Petri
                net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(log)
                
                # Calcola fitness
                fitness = pm4py.fitness_alignments(log, net, initial_marking, final_marking)
                
                # Calcola precision
                precision = pm4py.precision_alignments(log, net, initial_marking, final_marking)
                
            elif model_type == 'dfg':
                # Scopri DFG
                dfg, start_activities, end_activities = pm4py.discover_dfg(log)
                
                # Calcola fitness
                fitness = pm4py.fitness_token_based_replay(log, dfg, start_activities, end_activities)
                
                # Calcola precision (approssimata)
                precision = pm4py.precision_token_based_replay(log, dfg, start_activities, end_activities)
                
            elif model_type == 'declare':
                # Scopri modello DECLARE
                declare_model = pm4py.discover_declare(log)
                
                # Calcola fitness
                fitness = pm4py.fitness_declare_tbr(log, declare_model)
                
                # Calcola precision
                precision = pm4py.precision_declare_tbr(log, declare_model)
                
            else:
                raise ValueError(f"Tipo di modello non supportato: {model_type}")
            
            metrics = {
                'fitness': fitness,
                'precision': precision,
                'harmonic_mean': 2 * (fitness * precision) / (fitness + precision) if (fitness + precision) > 0 else 0,
                'model_type': model_type,
                'calculation_timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Fitness: {fitness:.3f}, Precision: {precision:.3f}")
            return metrics
            
        except Exception as e:
            logger.error(f"Errore nel calcolo fitness/precision: {e}")
            raise
    
    def _prepare_event_log(self, df: pl.DataFrame) -> pd.DataFrame:
        """Converte DataFrame Polars in Pandas formattato per PM4Py."""
        pdf = df.to_pandas()
        formatted_log = pm4py.format_dataframe(
            pdf,
            case_id='case_id',
            activity_key='activity',
            timestamp_key='timestamp'
        )
        return formatted_log
    
    def _calculate_conformance_metrics(self, aligned_traces: Dict) -> Dict[str, Any]:
        """Calcola metriche di conformità."""
        total_cases = len(aligned_traces)
        conforming_cases = sum(1 for alignment in aligned_traces.values() if alignment['cost'] == 0)
        deviating_cases = total_cases - conforming_cases
        
        if total_cases > 0:
            conformance_ratio = conforming_cases / total_cases
            deviation_ratio = deviating_cases / total_cases
        else:
            conformance_ratio = 0
            deviation_ratio = 0
        
        avg_alignment_cost = sum(alignment['cost'] for alignment in aligned_traces.values()) / total_cases if total_cases > 0 else 0
        
        return {
            'total_cases': total_cases,
            'conforming_cases': conforming_cases,
            'deviating_cases': deviating_cases,
            'conformance_ratio': conformance_ratio,
            'deviation_ratio': deviation_ratio,
            'average_alignment_cost': avg_alignment_cost,
            'max_alignment_cost': max((alignment['cost'] for alignment in aligned_traces.values()), default=0)
        }
    
    def _calculate_declare_metrics(self, conformance_results: Dict) -> Dict[str, Any]:
        """Calcola metriche per conformance DECLARE."""
        total_constraints = len(conformance_results) if isinstance(conformance_results, dict) else 0
        satisfied_constraints = sum(1 for result in conformance_results.values() if result['satisfied']) if isinstance(conformance_results, dict) else 0
        
        return {
            'total_constraints': total_constraints,
            'satisfied_constraints': satisfied_constraints,
            'violated_constraints': total_constraints - satisfied_constraints,
            'constraint_satisfaction_ratio': satisfied_constraints / total_constraints if total_constraints > 0 else 0
        }
    
    def _classify_deviation(self, alignment: Dict) -> str:
        """Classifica il tipo di deviazione."""
        if alignment['cost'] == 0:
            return 'no_deviation'
        
        # Analizza il percorso di allineamento per determinare il tipo di deviazione
        model_moves = sum(1 for move in alignment['alignment'] if move[0] != '>>')
        log_moves = sum(1 for move in alignment['alignment'] if move[1] != '>>')
        
        if model_moves > log_moves:
            return 'model_deviation'  # Modello troppo restrittivo
        elif log_moves > model_moves:
            return 'log_deviation'    # Log non conforme al modello
        else:
            return 'mixed_deviation'  # Deviazione mista
    
    def _analyze_root_causes(self, df: pl.DataFrame, deviation_patterns: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analizza le cause profonde delle deviazioni."""
        root_causes = []
        
        # Analisi per risorsa
        if 'resource' in df.columns:
            resource_deviations = df.filter(pl.col('case_id').is_in(deviation_patterns['deviating_cases'])) \
                                   .groupby('resource').count()
            
            for resource, count in resource_deviations.to_pandas().itertuples(index=False):
                root_causes.append({
                    'type': 'resource',
                    'value': resource,
                    'deviation_count': count,
                    'percentage': (count / len(deviation_patterns['deviating_cases'])) * 100 if deviation_patterns['deviating_cases'] else 0
                })
        
        # Analisi per attività
        if 'activity' in df.columns:
            activity_deviations = df.filter(pl.col('case_id').is_in(deviation_patterns['deviating_cases'])) \
                                   .groupby('activity').count()
            
            for activity, count in activity_deviations.to_pandas().itertuples(index=False):
                root_causes.append({
                    'type': 'activity',
                    'value': activity,
                    'deviation_count': count,
                    'percentage': (count / len(deviation_patterns['deviating_cases'])) * 100 if deviation_patterns['deviating_cases'] else 0
                })
        
        return root_causes

# Creazione istanza globale
conformance_service = ConformanceService()