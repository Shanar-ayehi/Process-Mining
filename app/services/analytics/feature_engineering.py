"""
Servizio Feature Engineering per Process Mining.

Questo modulo implementa l'estrazione automatica di features
dagli event log per modelli predittivi.
"""

import polars as pl
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from collections import Counter

from app.core.logger import get_logger
from app.core.config import settings

logger = get_logger()


class FeatureEngineeringService:
    """Servizio per l'estrazione automatica di features dagli event log."""
    
    def __init__(self):
        """Inizializza il servizio di feature engineering."""
        self.feature_cache = {}
        logger.info("FeatureEngineeringService inizializzato")
    
    def extract_basic_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Estrae features base dall'event log.
        
        Args:
            df: DataFrame Polars con event log
            
        Returns:
            Dizionario con features estratte
        """
        logger.info("Estrazione features base dall'event log")
        
        try:
            features = {}
            
            if df.is_empty():
                logger.warning("DataFrame vuoto, nessuna feature estratta")
                return features
            
            # Features temporali
            features.update(self._extract_temporal_features(df))
            
            # Features di frequenza
            features.update(self._extract_frequency_features(df))
            
            # Features di sequenza
            features.update(self._extract_sequence_features(df))
            
            # Features di performance
            features.update(self._extract_performance_features(df))
            
            logger.info(f"Estratte {len(features)} features base")
            return features
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione features base: {e}")
            raise
    
    def extract_advanced_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Estrae features avanzate dall'event log.
        
        Args:
            df: DataFrame Polars con event log
            
        Returns:
            Dizionario con features avanzate
        """
        logger.info("Estrazione features avanzate dall'event log")
        
        try:
            advanced_features = {}
            
            if df.is_empty():
                logger.warning("DataFrame vuoto, nessuna feature avanzata estratta")
                return advanced_features
            
            # Features di varianti
            advanced_features['process_variants'] = self._extract_variant_features(df)
            
            # Features di social network
            advanced_features['social_network'] = self._extract_social_network_features(df)
            
            # Features di performance avanzate
            advanced_features['advanced_performance'] = self._extract_advanced_performance_features(df)
            
            # Features di pattern
            advanced_features['patterns'] = self._extract_pattern_features(df)
            
            logger.info(f"Estratte {len(advanced_features)} categorie di features avanzate")
            return advanced_features
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione features avanzate: {e}")
            raise
    
    def _extract_temporal_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Estrae features temporali."""
        features = {}
        
        if 'timestamp' not in df.columns:
            return features
        
        # Durata totale del caso
        case_durations = df.groupby('case_id').agg([
            (pl.col('timestamp').max() - pl.col('timestamp').min()).alias('duration')
        ])
        
        durations_seconds = case_durations['duration'].dt.total_seconds().drop_nulls().to_list()
        
        if durations_seconds:
            features['avg_case_duration_seconds'] = sum(durations_seconds) / len(durations_seconds)
            features['min_case_duration_seconds'] = min(durations_seconds)
            features['max_case_duration_seconds'] = max(durations_seconds)
            features['median_case_duration_seconds'] = sorted(durations_seconds)[len(durations_seconds) // 2]
        
        # Tempo tra attività consecutive
        df_sorted = df.sort(['case_id', 'timestamp'])
        df_with_lag = df_sorted.with_columns([
            pl.col('timestamp').shift(1).over('case_id').alias('prev_timestamp')
        ])
        
        df_with_gap = df_with_lag.with_columns([
            (pl.col('timestamp') - pl.col('prev_timestamp')).dt.total_seconds().alias('time_gap')
        ])
        
        time_gaps = df_with_gap.filter(pl.col('time_gap').is_not_null())['time_gap'].to_list()
        
        if time_gaps:
            features['avg_time_between_activities'] = sum(time_gaps) / len(time_gaps)
            features['min_time_between_activities'] = min(time_gaps)
            features['max_time_between_activities'] = max(time_gaps)
        
        return features
    
    def _extract_frequency_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Estrae features di frequenza."""
        features = {}
        
        if 'activity' not in df.columns:
            return features
        
        # Frequenza attività
        activity_counts = df.groupby('activity').count().sort('count', descending=True)
        
        features['total_activities'] = len(df)
        features['unique_activities'] = len(activity_counts)
        
        # Top 5 attività più frequenti
        top_activities = activity_counts.head(5).to_dicts()
        features['top_activities'] = [a['activity'] for a in top_activities]
        features['top_activities_frequency'] = [a['count'] for a in top_activities]
        
        # Distribuzione attività
        total = len(df)
        features['activity_distribution'] = {
            row['activity']: row['count'] / total 
            for row in activity_counts.to_dicts()
        }
        
        return features
    
    def _extract_sequence_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Estrae features di sequenza."""
        features = {}
        
        if 'case_id' not in df.columns or 'activity' not in df.columns:
            return features
        
        # Sequenze per caso
        case_sequences = df.sort(['case_id', 'timestamp']).groupby('case_id').agg([
            pl.col('activity').list().alias('sequence')
        ])
        
        sequences = case_sequences['sequence'].to_list()
        
        # Lunghezza media sequenza
        seq_lengths = [len(seq) for seq in sequences]
        if seq_lengths:
            features['avg_sequence_length'] = sum(seq_lengths) / len(seq_lengths)
            features['min_sequence_length'] = min(seq_lengths)
            features['max_sequence_length'] = max(seq_lengths)
        
        # Transizioni comuni
        transitions = []
        for seq in sequences:
            for i in range(len(seq) - 1):
                transitions.append((seq[i], seq[i + 1]))
        
        if transitions:
            transition_counts = Counter(transitions)
            features['most_common_transitions'] = [
                {'from': t[0], 'to': t[1], 'count': c}
                for t, c in transition_counts.most_common(5)
            ]
        
        return features
    
    def _extract_performance_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Estrae features di performance."""
        features = {}
        
        if 'timestamp' not in df.columns or 'case_id' not in df.columns:
            return features
        
        # Tempo per attività
        activity_durations = []
        for activity in df['activity'].unique().to_list():
            activity_df = df.filter(pl.col('activity') == activity)
            
            # Calcola durata media per questa attività
            case_times = activity_df.groupby('case_id').agg([
                pl.col('timestamp').min().alias('start'),
                pl.col('timestamp').max().alias('end')
            ])
            
            durations = (case_times['end'] - case_times['start']).dt.total_seconds().drop_nulls().to_list()
            
            if durations:
                activity_durations.append({
                    'activity': activity,
                    'avg_duration': sum(durations) / len(durations),
                    'occurrences': len(durations)
                })
        
        features['activity_durations'] = sorted(
            activity_durations,
            key=lambda x: x['avg_duration'],
            reverse=True
        )[:10]  # Top 10 attività più lente
        
        return features
    
    def _extract_variant_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Estrae features sulle varianti del processo."""
        if 'case_id' not in df.columns or 'activity' not in df.columns:
            return {}
        
        # Crea sequenze per caso
        case_sequences = df.sort(['case_id', 'timestamp']).groupby('case_id').agg([
            pl.col('activity').list().alias('sequence')
        ])
        
        # Converti sequenze in tuple per hash
        sequences_as_tuples = [tuple(seq) for seq in case_sequences['sequence'].to_list()]
        
        # Conta varianti
        variant_counts = Counter(sequences_as_tuples)
        
        return {
            'total_variants': len(variant_counts),
            'variant_distribution': {
                str(variant): count
                for variant, count in variant_counts.most_common(10)
            },
            'most_common_variant': str(variant_counts.most_common(1)[0][0]) if variant_counts else None
        }
    
    def _extract_social_network_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Estrae features di social network analysis."""
        if 'resource' not in df.columns or 'activity' not in df.columns:
            return {}
        
        # Matrice handover of work
        resource_activities = df.groupby('resource').agg([
            pl.col('activity').unique().list().alias('activities')
        ])
        
        resources = resource_activities['resource'].to_list()
        
        # Calcola similarità tra risorse
        resource_similarity = {}
        for i, r1 in enumerate(resources):
            for r2 in resources[i+1:]:
                acts1 = set(resource_activities.filter(pl.col('resource') == r1)['activities'][0])
                acts2 = set(resource_activities.filter(pl.col('resource') == r2)['activities'][0])
                
                if acts1 and acts2:
                    similarity = len(acts1 & acts2) / len(acts1 | acts2)
                    resource_similarity[f"{r1}-{r2}"] = similarity
        
        return {
            'resource_count': len(resources),
            'avg_resource_similarity': sum(resource_similarity.values()) / len(resource_similarity) if resource_similarity else 0,
            'most_connected_resources': sorted(
                resource_similarity.items(),
                key=lambda x: x[1],
                reverse=True
            )[:5]
        }
    
    def _extract_advanced_performance_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Estrae features di performance avanzate."""
        if 'timestamp' not in df.columns:
            return {}
        
        # Calcola tempi di ciclo
        cycle_times = []
        for case_id in df['case_id'].unique().to_list():
            case_df = df.filter(pl.col('case_id') == case_id).sort('timestamp')
            timestamps = case_df['timestamp'].to_list()
            
            if len(timestamps) >= 2:
                cycle_time = (timestamps[-1] - timestamps[0]).total_seconds()
                cycle_times.append(cycle_time)
        
        if not cycle_times:
            return {}
        
        # Calcola percentili
        sorted_times = sorted(cycle_times)
        
        return {
            'cycle_time_p25': sorted_times[int(len(sorted_times) * 0.25)],
            'cycle_time_p50': sorted_times[int(len(sorted_times) * 0.50)],
            'cycle_time_p75': sorted_times[int(len(sorted_times) * 0.75)],
            'cycle_time_p90': sorted_times[int(len(sorted_times) * 0.90)],
            'cycle_time_std': np.std(cycle_times) if len(cycle_times) > 1 else 0
        }
    
    def _extract_pattern_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Estrae features di pattern."""
        if 'activity' not in df.columns or 'case_id' not in df.columns:
            return {}
        
        # Cerca pattern ripetitivi
        case_sequences = df.sort(['case_id', 'timestamp']).groupby('case_id').agg([
            pl.col('activity').list().alias('sequence')
        ])
        
        # Analizza pattern di rework (attività ripetute)
        rework_cases = 0
        for seq in case_sequences['sequence'].to_list():
            if len(seq) != len(set(seq)):
                rework_cases += 1
        
        total_cases = len(case_sequences)
        
        return {
            'rework_rate': rework_cases / total_cases if total_cases > 0 else 0,
            'rework_cases': rework_cases,
            'total_cases_analyzed': total_cases
        }


# Istanza globale
feature_engineering_service = FeatureEngineeringService()