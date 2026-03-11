import pm4py
import polars as pl
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple, Union
from pathlib import Path
from datetime import datetime
from app.core.logger import get_logger
from app.core.config import settings

logger = get_logger()

class DiscoveryService:
    """Servizio per il Process Discovery con PM4Py."""
    
    def __init__(self):
        self.output_dir = settings.data_dir / "processed"
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def discover_dfg(self, df: pl.DataFrame, 
                    output_image_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Scopre il Directly-Follows Graph (DFG) dal DataFrame.
        
        Args:
            df: DataFrame Polars con event log
            output_image_path: Path per salvare l'immagine (opzionale)
            
        Returns:
            Dizionario con DFG e statistiche
        """
        logger.info("Avvio Process Discovery (DFG)...")
        
        try:
            # Converte Polars a Pandas e formatta per PM4Py
            log = self._prepare_event_log(df)
            
            # Calcola DFG
            dfg, start_activities, end_activities = pm4py.discover_dfg(log)
            
            # Salva immagine se richiesto
            if output_image_path is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_image_path = str(self.output_dir / f"process_map_{timestamp}.png")
            
            pm4py.save_vis_dfg(dfg, start_activities, end_activities, output_image_path)
            logger.info(f"Mappa del processo salvata in: {output_image_path}")
            
            # Calcola statistiche
            stats = self._calculate_dfg_statistics(dfg, start_activities, end_activities)
            
            result = {
                'dfg': dfg,
                'start_activities': start_activities,
                'end_activities': end_activities,
                'image_path': output_image_path,
                'statistics': stats,
                'discovery_timestamp': datetime.now().isoformat()
            }
            
            logger.info("Process Discovery (DFG) completato con successo")
            return result
            
        except Exception as e:
            logger.error(f"Errore nel Process Discovery: {e}")
            raise
    
    def discover_alpha_miner(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Scopre il modello di processo con Alpha Miner.
        
        Args:
            df: DataFrame Polars con event log
            
        Returns:
            Dizionario con modello e statistiche
        """
        logger.info("Avvio Alpha Miner...")
        
        try:
            log = self._prepare_event_log(df)
            
            # Calcola modello con Alpha Miner
            net, initial_marking, final_marking = pm4py.discover_petri_net_alpha(log)
            
            # Calcola statistiche
            stats = self._calculate_petri_net_statistics(net, initial_marking, final_marking)
            
            result = {
                'petri_net': net,
                'initial_marking': initial_marking,
                'final_marking': final_marking,
                'statistics': stats,
                'discovery_timestamp': datetime.now().isoformat()
            }
            
            logger.info("Alpha Miner completato con successo")
            return result
            
        except Exception as e:
            logger.error(f"Errore in Alpha Miner: {e}")
            raise
    
    def discover_heuristic_miner(self, df: pl.DataFrame, 
                               dependency_threshold: float = 0.5) -> Dict[str, Any]:
        """
        Scopre il modello di processo con Heuristic Miner.
        
        Args:
            df: DataFrame Polars con event log
            dependency_threshold: Soglia di dipendenza per Heuristic Miner
            
        Returns:
            Dizionario con modello e statistiche
        """
        logger.info(f"Avvio Heuristic Miner (threshold: {dependency_threshold})...")
        
        try:
            log = self._prepare_event_log(df)
            
            # Calcola modello con Heuristic Miner
            net, initial_marking, final_marking = pm4py.discover_petri_net_heuristics(
                log, dependency_threshold=dependency_threshold
            )
            
            # Calcola statistiche
            stats = self._calculate_petri_net_statistics(net, initial_marking, final_marking)
            
            result = {
                'petri_net': net,
                'initial_marking': initial_marking,
                'final_marking': final_marking,
                'dependency_threshold': dependency_threshold,
                'statistics': stats,
                'discovery_timestamp': datetime.now().isoformat()
            }
            
            logger.info("Heuristic Miner completato con successo")
            return result
            
        except Exception as e:
            logger.error(f"Errore in Heuristic Miner: {e}")
            raise
    
    def discover_inductive_miner(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Scopre il modello di processo con Inductive Miner.
        
        Args:
            df: DataFrame Polars con event log
            
        Returns:
            Dizionario con modello e statistiche
        """
        logger.info("Avvio Inductive Miner...")
        
        try:
            log = self._prepare_event_log(df)
            
            # Calcola modello con Inductive Miner
            tree = pm4py.discover_process_tree_inductive(log)
            
            # Converte albero in rete di Petri
            net, initial_marking, final_marking = pm4py.convert_to_petri_net(tree)
            
            # Calcola statistiche
            stats = self._calculate_petri_net_statistics(net, initial_marking, final_marking)
            
            result = {
                'process_tree': tree,
                'petri_net': net,
                'initial_marking': initial_marking,
                'final_marking': final_marking,
                'statistics': stats,
                'discovery_timestamp': datetime.now().isoformat()
            }
            
            logger.info("Inductive Miner completato con successo")
            return result
            
        except Exception as e:
            logger.error(f"Errore in Inductive Miner: {e}")
            raise
    
    def discover_variants(self, df: pl.DataFrame, 
                         min_frequency_threshold: float = 0.05) -> Dict[str, Any]:
        """
        Scopre le varianti del processo.
        
        Args:
            df: DataFrame Polars con event log
            min_frequency_threshold: Soglia minima di frequenza per includere varianti
            
        Returns:
            Dizionario con varianti e statistiche
        """
        logger.info(f"Avvio scoperta varianti (threshold: {min_frequency_threshold})...")
        
        try:
            log = self._prepare_event_log(df)
            
            # Calcola varianti
            variants = pm4py.get_variants(log)
            
            # Filtra varianti per frequenza
            total_cases = len(log['case:concept:name'].unique())
            filtered_variants = {}
            
            for variant, count in variants.items():
                frequency = count / total_cases
                if frequency >= min_frequency_threshold:
                    filtered_variants[variant] = count
            
            # Ordina varianti per frequenza
            sorted_variants = sorted(filtered_variants.items(), key=lambda x: x[1], reverse=True)
            
            # Calcola statistiche
            stats = {
                'total_variants': len(variants),
                'filtered_variants': len(filtered_variants),
                'covered_cases': sum(count for _, count in filtered_variants.items()),
                'coverage_percentage': (sum(count for _, count in filtered_variants.items()) / total_cases) * 100,
                'top_variant': sorted_variants[0] if sorted_variants else None
            }
            
            result = {
                'variants': dict(sorted_variants),
                'min_frequency_threshold': min_frequency_threshold,
                'statistics': stats,
                'discovery_timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Scoperta varianti completata: {len(filtered_variants)} varianti trovate")
            return result
            
        except Exception as e:
            logger.error(f"Errore nella scoperta varianti: {e}")
            raise
    
    def discover_performance_dfg(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Scopre il DFG con informazioni di performance.
        
        Args:
            df: DataFrame Polars con event log
            
        Returns:
            Dizionario con DFG performance e statistiche
        """
        logger.info("Avvio Performance DFG...")
        
        try:
            log = self._prepare_event_log(df)
            
            # Calcola DFG con performance
            dfg, start_activities, end_activities = pm4py.discover_performance_dfg(log)
            
            # Calcola statistiche
            stats = self._calculate_performance_statistics(dfg)
            
            result = {
                'performance_dfg': dfg,
                'start_activities': start_activities,
                'end_activities': end_activities,
                'statistics': stats,
                'discovery_timestamp': datetime.now().isoformat()
            }
            
            logger.info("Performance DFG completato con successo")
            return result
            
        except Exception as e:
            logger.error(f"Errore in Performance DFG: {e}")
            raise
    
    def _prepare_event_log(self, df: pl.DataFrame) -> pd.DataFrame:
        """Converte DataFrame Polars in Pandas formattato per PM4Py."""
        # Converte Polars in Pandas
        pdf = df.to_pandas()
        
        # Formattiamo indicando a PM4Py le 3 colonne obbligatorie
        formatted_log = pm4py.format_dataframe(
            pdf,
            case_id='case_id',
            activity_key='activity',
            timestamp_key='timestamp'
        )
        return formatted_log
    
    def _calculate_dfg_statistics(self, dfg: Dict, start_activities: Dict, end_activities: Dict) -> Dict[str, Any]:
        """Calcola statistiche per il DFG."""
        return {
            'total_edges': len(dfg),
            'unique_activities': len(set(list(dfg.keys()) + [edge[1] for edge in dfg.keys()])),
            'start_activities_count': len(start_activities),
            'end_activities_count': len(end_activities),
            'most_frequent_transition': max(dfg.items(), key=lambda x: x[1]) if dfg else None
        }
    
    def _calculate_petri_net_statistics(self, net, initial_marking, final_marking) -> Dict[str, Any]:
        """Calcola statistiche per la rete di Petri."""
        return {
            'places_count': len(net.places),
            'transitions_count': len(net.transitions),
            'arcs_count': len(net.arcs),
            'initial_marking_places': len(initial_marking),
            'final_marking_places': len(final_marking)
        }
    
    def _calculate_performance_statistics(self, performance_dfg: Dict) -> Dict[str, Any]:
        """Calcola statistiche per il DFG performance."""
        if not performance_dfg:
            return {}
        
        avg_durations = [info['average'] for info in performance_dfg.values()]
        min_durations = [info['minimum'] for info in performance_dfg.values()]
        max_durations = [info['maximum'] for info in performance_dfg.values()]
        
        return {
            'total_transitions': len(performance_dfg),
            'avg_duration_mean': sum(avg_durations) / len(avg_durations) if avg_durations else 0,
            'avg_duration_min': min(avg_durations) if avg_durations else 0,
            'avg_duration_max': max(avg_durations) if avg_durations else 0,
            'fastest_transition': min(performance_dfg.items(), key=lambda x: x[1]['average']) if avg_durations else None,
            'slowest_transition': max(performance_dfg.items(), key=lambda x: x[1]['average']) if avg_durations else None
        }

# Creazione istanza globale
discovery_service = DiscoveryService()