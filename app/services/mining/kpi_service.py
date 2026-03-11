import pm4py
import polars as pl
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime, timedelta
from app.core.logger import get_logger
from app.core.config import settings

logger = get_logger()

class KPIsService:
    """Servizio per il calcolo e analisi KPI di processo."""
    
    def __init__(self):
        self.default_variant_threshold = settings.mining_default_variant_threshold
    
    def calculate_process_kpis(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Calcola KPI principali del processo.
        
        Args:
            df: DataFrame Polars con event log
            
        Returns:
            Dizionario con KPI calcolati
        """
        logger.info("Calcolo KPI principali del processo...")
        
        try:
            log = self._prepare_event_log(df)
            
            # Calcola KPI base
            basic_kpis = self._calculate_basic_kpis(df)
            
            # Calcola KPI di performance
            performance_kpis = self._calculate_performance_kpis(df)
            
            # Calcola KPI di qualità
            quality_kpis = self._calculate_quality_kpis(df)
            
            # Calcola KPI di efficienza
            efficiency_kpis = self._calculate_efficiency_kpis(df)
            
            # Calcola KPI di variabilità
            variability_kpis = self._calculate_variability_kpis(df)
            
            all_kpis = {
                'timestamp': datetime.now().isoformat(),
                'environment': settings.environment,
                'basic_kpis': basic_kpis,
                'performance_kpis': performance_kpis,
                'quality_kpis': quality_kpis,
                'efficiency_kpis': efficiency_kpis,
                'variability_kpis': variability_kpis,
                'overall_score': self._calculate_overall_score(basic_kpis, performance_kpis, quality_kpis, efficiency_kpis)
            }
            
            logger.info("Calcolo KPI completato")
            return all_kpis
            
        except Exception as e:
            logger.error(f"Errore nel calcolo KPI: {e}")
            raise
    
    def calculate_case_kpis(self, df: pl.DataFrame, case_id: str) -> Dict[str, Any]:
        """
        Calcola KPI specifici per un singolo caso.
        
        Args:
            df: DataFrame Polars con event log
            case_id: ID del caso da analizzare
            
        Returns:
            Dizionario con KPI del caso
        """
        logger.info(f"Calcolo KPI per caso: {case_id}")
        
        try:
            case_df = df.filter(pl.col('case_id') == case_id)
            
            if case_df.is_empty():
                logger.warning(f"Caso {case_id} non trovato")
                return {}
            
            # Calcola KPI specifici per il caso
            case_kpis = {
                'case_id': case_id,
                'timestamp': datetime.now().isoformat(),
                'total_activities': len(case_df),
                'start_time': case_df['timestamp'].min(),
                'end_time': case_df['timestamp'].max(),
                'lead_time': self._calculate_case_lead_time(case_df),
                'activities': case_df['activity'].to_list(),
                'resources': case_df['resource'].unique().to_list(),
                'deviations': self._detect_case_deviations(case_df)
            }
            
            logger.info(f"KPI caso {case_id} calcolati")
            return case_kpis
            
        except Exception as e:
            logger.error(f"Errore nel calcolo KPI caso {case_id}: {e}")
            raise
    
    def calculate_resource_kpis(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Calcola KPI per risorsa/utente.
        
        Args:
            df: DataFrame Polars con event log
            
        Returns:
            Dizionario con KPI per risorsa
        """
        logger.info("Calcolo KPI per risorsa...")
        
        try:
            if 'resource' not in df.columns:
                logger.warning("Colonna 'resource' non presente, impossibile calcolare KPI per risorsa")
                return {}
            
            resource_kpis = {}
            
            # Raggruppa per risorsa
            for resource in df['resource'].unique().to_list():
                if not resource or resource == 'Unknown':
                    continue
                
                resource_df = df.filter(pl.col('resource') == resource)
                
                resource_stats = {
                    'total_cases': len(resource_df['case_id'].unique()),
                    'total_activities': len(resource_df),
                    'avg_activities_per_case': len(resource_df) / len(resource_df['case_id'].unique()) if len(resource_df['case_id'].unique()) > 0 else 0,
                    'cases': resource_df['case_id'].unique().to_list(),
                    'activities': resource_df['activity'].unique().to_list(),
                    'performance_metrics': self._calculate_resource_performance(resource_df)
                }
                
                resource_kpis[resource] = resource_stats
            
            logger.info(f"KPI per {len(resource_kpis)} risorse calcolati")
            return resource_kpis
            
        except Exception as e:
            logger.error(f"Errore nel calcolo KPI per risorsa: {e}")
            raise
    
    def calculate_activity_kpis(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Calcola KPI per attività.
        
        Args:
            df: DataFrame Polars con event log
            
        Returns:
            Dizionario con KPI per attività
        """
        logger.info("Calcolo KPI per attività...")
        
        try:
            if 'activity' not in df.columns:
                logger.warning("Colonna 'activity' non presente, impossibile calcolare KPI per attività")
                return {}
            
            activity_kpis = {}
            
            # Raggruppa per attività
            for activity in df['activity'].unique().to_list():
                activity_df = df.filter(pl.col('activity') == activity)
                
                activity_stats = {
                    'total_occurrences': len(activity_df),
                    'unique_cases': len(activity_df['case_id'].unique()),
                    'frequency': len(activity_df) / len(df) if len(df) > 0 else 0,
                    'avg_duration': self._calculate_activity_duration(activity_df),
                    'resources': activity_df['resource'].unique().to_list(),
                    'performance_metrics': self._calculate_activity_performance(activity_df)
                }
                
                activity_kpis[activity] = activity_stats
            
            logger.info(f"KPI per {len(activity_kpis)} attività calcolati")
            return activity_kpis
            
        except Exception as e:
            logger.error(f"Errore nel calcolo KPI per attività: {e}")
            raise
    
    def calculate_trend_kpis(self, df: pl.DataFrame, time_window: str = '1d') -> Dict[str, Any]:
        """
        Calcola KPI di trend temporale.
        
        Args:
            df: DataFrame Polars con event log
            time_window: Finestra temporale per l'analisi ('1d', '1w', '1m')
            
        Returns:
            Dizionario con KPI di trend
        """
        logger.info(f"Calcolo KPI di trend ({time_window})...")
        
        try:
            if 'timestamp' not in df.columns:
                logger.warning("Colonna 'timestamp' non presente, impossibile calcolare KPI di trend")
                return {}
            
            # Converte timestamp in datetime se necessario
            df_with_time = df.clone()
            if not pl.datatypes.is_temporal(df_with_time['timestamp'].dtype):
                df_with_time = df_with_time.with_columns([
                    pl.col('timestamp').str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S.%fZ")
                    .fill_null(pl.col('timestamp').str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ"))
                ])
            
            # Raggruppa per finestra temporale
            df_grouped = df_with_time.groupby_dynamic(
                'timestamp',
                every=time_window,
                period=time_window
            ).agg([
                pl.col('case_id').n_unique().alias('cases_count'),
                pl.col('activity').count().alias('activities_count'),
                pl.col('resource').n_unique().alias('resources_count')
            ])
            
            trend_kpis = {
                'time_window': time_window,
                'data_points': len(df_grouped),
                'cases_trend': df_grouped.select(['timestamp', 'cases_count']).to_dicts(),
                'activities_trend': df_grouped.select(['timestamp', 'activities_count']).to_dicts(),
                'resources_trend': df_grouped.select(['timestamp', 'resources_count']).to_dicts(),
                'summary': {
                    'avg_cases_per_period': df_grouped['cases_count'].mean(),
                    'avg_activities_per_period': df_grouped['activities_count'].mean(),
                    'avg_resources_per_period': df_grouped['resources_count'].mean()
                }
            }
            
            logger.info(f"KPI di trend calcolati per {len(df_grouped)} periodi")
            return trend_kpis
            
        except Exception as e:
            logger.error(f"Errore nel calcolo KPI di trend: {e}")
            raise
    
    def _calculate_basic_kpis(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Calcola KPI base del processo."""
        return {
            'total_cases': len(df['case_id'].unique()) if 'case_id' in df.columns else 0,
            'total_activities': len(df),
            'unique_activities': len(df['activity'].unique()) if 'activity' in df.columns else 0,
            'avg_activities_per_case': len(df) / len(df['case_id'].unique()) if len(df['case_id'].unique()) > 0 else 0,
            'cases_with_timestamps': len(df.filter(pl.col('timestamp').is_not_null())) if 'timestamp' in df.columns else 0
        }
    
    def _calculate_performance_kpis(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Calcola KPI di performance."""
        if 'timestamp' not in df.columns:
            return {}
        
        # Calcola tempi di lead per caso
        case_times = df.groupby('case_id').agg([
            pl.col('timestamp').min().alias('start_time'),
            pl.col('timestamp').max().alias('end_time')
        ])
        
        case_times = case_times.with_columns([
            (pl.col('end_time') - pl.col('start_time')).dt.total_seconds().alias('lead_time_seconds')
        ])
        
        lead_times = case_times['lead_time_seconds'].drop_nulls().to_list()
        
        return {
            'avg_lead_time_seconds': sum(lead_times) / len(lead_times) if lead_times else 0,
            'min_lead_time_seconds': min(lead_times) if lead_times else 0,
            'max_lead_time_seconds': max(lead_times) if lead_times else 0,
            'median_lead_time_seconds': sorted(lead_times)[len(lead_times)//2] if lead_times else 0,
            'cases_completed': len(lead_times)
        }
    
    def _calculate_quality_kpis(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Calcola KPI di qualità."""
        quality_issues = 0
        total_records = len(df)
        
        # Controlla timestamp mancanti
        if 'timestamp' in df.columns:
            missing_timestamps = df.filter(pl.col('timestamp').is_null()).height()
            quality_issues += missing_timestamps
        
        # Controlla case_id mancanti
        if 'case_id' in df.columns:
            missing_case_ids = df.filter(pl.col('case_id').is_null()).height()
            quality_issues += missing_case_ids
        
        # Controlla activity mancanti
        if 'activity' in df.columns:
            missing_activities = df.filter(pl.col('activity').is_null()).height()
            quality_issues += missing_activities
        
        return {
            'total_records': total_records,
            'quality_issues': quality_issues,
            'quality_score': 1.0 - (quality_issues / total_records) if total_records > 0 else 0,
            'missing_timestamps': missing_timestamps if 'timestamp' in df.columns else 0,
            'missing_case_ids': missing_case_ids if 'case_id' in df.columns else 0,
            'missing_activities': missing_activities if 'activity' in df.columns else 0
        }
    
    def _calculate_efficiency_kpis(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Calcola KPI di efficienza."""
        if 'case_id' not in df.columns or 'activity' not in df.columns:
            return {}
        
        # Calcola numero medio di attività per caso
        case_activity_counts = df.groupby('case_id').agg([
            pl.col('activity').count().alias('activity_count')
        ])
        
        activity_counts = case_activity_counts['activity_count'].to_list()
        
        return {
            'avg_activities_per_case': sum(activity_counts) / len(activity_counts) if activity_counts else 0,
            'min_activities_per_case': min(activity_counts) if activity_counts else 0,
            'max_activities_per_case': max(activity_counts) if activity_counts else 0,
            'cases_with_rework': self._detect_rework_cases(df),
            'rework_ratio': self._calculate_rework_ratio(df)
        }
    
    def _calculate_variability_kpis(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Calcola KPI di variabilità."""
        if 'case_id' not in df.columns or 'activity' not in df.columns:
            return {}
        
        # Calcola varianti del processo
        variants = df.groupby('case_id').agg([
            pl.col('activity').list().alias('variant')
        ])
        
        unique_variants = variants['variant'].unique().to_list()
        
        return {
            'total_variants': len(unique_variants),
            'most_common_variant': max(unique_variants, key=lambda x: variants.filter(pl.col('variant') == x).height()) if unique_variants else None,
            'variant_diversity': len(unique_variants) / len(df['case_id'].unique()) if len(df['case_id'].unique()) > 0 else 0,
            'cases_per_variant': [variants.filter(pl.col('variant') == v).height() for v in unique_variants]
        }
    
    def _calculate_overall_score(self, basic_kpis: Dict, performance_kpis: Dict, 
                               quality_kpis: Dict, efficiency_kpis: Dict) -> float:
        """Calcola punteggio complessivo KPI."""
        # Pesi per i diversi tipi di KPI
        weights = {
            'quality': 0.3,
            'performance': 0.3,
            'efficiency': 0.2,
            'basic': 0.1,
            'variability': 0.1
        }
        
        # Calcola punteggi parziali
        quality_score = quality_kpis.get('quality_score', 0)
        performance_score = min(1.0, (performance_kpis.get('avg_lead_time_seconds', 0) / 3600) / 24)  # Normalizza su 24 ore
        efficiency_score = 1.0 - efficiency_kpis.get('rework_ratio', 0)
        basic_score = min(1.0, basic_kpis.get('avg_activities_per_case', 0) / 10)  # Normalizza su 10 attività
        
        # Calcola punteggio complessivo
        overall_score = (
            weights['quality'] * quality_score +
            weights['performance'] * (1 - performance_score) +  # Inverso: meno tempo = meglio
            weights['efficiency'] * efficiency_score +
            weights['basic'] * basic_score +
            weights['variability'] * 0.5  # Valore fisso per variabilità
        )
        
        return round(overall_score, 2)
    
    def _calculate_case_lead_time(self, case_df: pl.DataFrame) -> Optional[float]:
        """Calcola il lead time per un caso."""
        if 'timestamp' not in case_df.columns:
            return None
        
        start_time = case_df['timestamp'].min()
        end_time = case_df['timestamp'].max()
        
        if start_time and end_time:
            return (end_time - start_time).total_seconds()
        return None
    
    def _detect_case_deviations(self, case_df: pl.DataFrame) -> List[str]:
        """Rileva deviazioni in un caso specifico."""
        deviations = []
        
        # Controlla timestamp non crescenti
        timestamps = case_df['timestamp'].sort().to_list()
        for i in range(1, len(timestamps)):
            if timestamps[i] < timestamps[i-1]:
                deviations.append(f"Timestamp non crescente: {timestamps[i-1]} -> {timestamps[i]}")
        
        return deviations
    
    def _calculate_resource_performance(self, resource_df: pl.DataFrame) -> Dict[str, Any]:
        """Calcola metriche di performance per risorsa."""
        return {
            'avg_activities_per_case': len(resource_df) / len(resource_df['case_id'].unique()) if len(resource_df['case_id'].unique()) > 0 else 0,
            'total_cases': len(resource_df['case_id'].unique()),
            'activity_distribution': resource_df.groupby('activity').count().to_dicts()
        }
    
    def _calculate_activity_duration(self, activity_df: pl.DataFrame) -> Optional[float]:
        """Calcola la durata media di un'attività."""
        if 'timestamp' not in activity_df.columns:
            return None
        
        # Calcola durata tra occorrenze consecutive
        durations = []
        for case_id in activity_df['case_id'].unique():
            case_activities = activity_df.filter(pl.col('case_id') == case_id).sort('timestamp')
            timestamps = case_activities['timestamp'].to_list()
            
            for i in range(1, len(timestamps)):
                duration = (timestamps[i] - timestamps[i-1]).total_seconds()
                durations.append(duration)
        
        return sum(durations) / len(durations) if durations else None
    
    def _calculate_activity_performance(self, activity_df: pl.DataFrame) -> Dict[str, Any]:
        """Calcola metriche di performance per attività."""
        return {
            'total_occurrences': len(activity_df),
            'unique_resources': len(activity_df['resource'].unique()),
            'avg_duration': self._calculate_activity_duration(activity_df)
        }
    
    def _detect_rework_cases(self, df: pl.DataFrame) -> int:
        """Rileva casi con attività di rework."""
        rework_cases = 0
        
        for case_id in df['case_id'].unique():
            case_activities = df.filter(pl.col('case_id') == case_id)['activity'].to_list()
            
            # Controlla se ci sono attività ripetute
            if len(case_activities) != len(set(case_activities)):
                rework_cases += 1
        
        return rework_cases
    
    def _calculate_rework_ratio(self, df: pl.DataFrame) -> float:
        """Calcola il ratio di rework."""
        total_cases = len(df['case_id'].unique())
        rework_cases = self._detect_rework_cases(df)
        
        return rework_cases / total_cases if total_cases > 0 else 0
    
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

# Creazione istanza globale
kpi_service = KPIsService()