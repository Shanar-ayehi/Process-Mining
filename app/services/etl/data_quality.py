from typing import List, Dict, Any, Optional, Union
import polars as pl
from pandera import DataFrameSchema, Column, Check
from pandera.typing import Series
from datetime import datetime
from app.core.logger import get_logger
from app.core.config import settings

logger = get_logger()

class EventLogSchema:
    """Schema di validazione per event log secondo standard Process Mining."""
    
    schema = DataFrameSchema({
        "case_id": Column(str, nullable=False, checks=[
            Check(lambda x: x.str.len() > 0, element_wise=False)
        ]),
        "activity": Column(str, nullable=False, checks=[
            Check(lambda x: x.str.len() > 0, element_wise=False)
        ]),
        "timestamp": Column(pl.Datetime, nullable=False),
        "resource": Column(str, nullable=True, checks=[
            Check(lambda x: x.str.len() > 0, element_wise=False, ignore_na=True)
        ]),
        "deal_name": Column(str, nullable=True),
        "amount": Column(str, nullable=True),
        "pipeline": Column(str, nullable=True),
        "stage_id": Column(str, nullable=True),
        "transformed_at": Column(str, nullable=True),
        "environment": Column(str, nullable=True)
    }, strict=False)

class DataQualityService:
    """Servizio per la validazione e controllo qualità dati."""
    
    def __init__(self):
        self.staged_dir = settings.staged_data_dir
        self.logs_dir = settings.logs_dir
    
    def validate_event_log_schema(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Valida lo schema di un event log.
        
        Args:
            df: DataFrame da validare
            
        Returns:
            Report di validazione
        """
        logger.info("Validazione schema event log")
        
        validation_report = {
            'timestamp': datetime.now().isoformat(),
            'total_rows': len(df),
            'validation_passed': False,
            'errors': [],
            'warnings': [],
            'statistics': {}
        }
        
        try:
            # Converte Polars a Pandas per Pandera
            pandas_df = df.to_pandas()
            
            # Validazione schema
            validated_df = EventLogSchema.schema.validate(pandas_df)
            
            validation_report['validation_passed'] = True
            logger.info("Validazione schema event log superata")
            
            # Calcola statistiche
            validation_report['statistics'] = self._calculate_log_statistics(df)
            
        except Exception as e:
            validation_report['errors'].append(str(e))
            logger.error(f"Errore validazione schema: {e}")
        
        # Controlli aggiuntivi
        additional_checks = self._run_additional_checks(df)
        validation_report['errors'].extend(additional_checks['errors'])
        validation_report['warnings'].extend(additional_checks['warnings'])
        
        # Salva report
        self._save_validation_report(validation_report, "event_log_validation")
        
        return validation_report
    
    def validate_data_completeness(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Controlla la completezza dei dati.
        
        Args:
            df: DataFrame da analizzare
            
        Returns:
            Report completezza
        """
        logger.info("Controllo completezza dati")
        
        completeness_report = {
            'timestamp': datetime.now().isoformat(),
            'total_rows': len(df),
            'completeness_score': 0.0,
            'missing_values': {},
            'critical_missing': [],
            'warnings': []
        }
        
        # Colonne critiche che non devono avere valori mancanti
        critical_columns = ['case_id', 'activity', 'timestamp']
        
        for column in df.columns:
            if column in df.columns:
                missing_count = df[column].null_count()
                total_count = len(df)
                missing_percentage = (missing_count / total_count) * 100 if total_count > 0 else 0
                
                completeness_report['missing_values'][column] = {
                    'count': missing_count,
                    'percentage': round(missing_percentage, 2)
                }
                
                # Controlla colonne critiche
                if column in critical_columns and missing_count > 0:
                    completeness_report['critical_missing'].append(column)
                    completeness_report['warnings'].append(
                        f"Colonna critica {column} ha {missing_count} valori mancanti"
                    )
        
        # Calcola punteggio completezza
        total_missing = sum(info['count'] for info in completeness_report['missing_values'].values())
        if len(df) > 0:
            completeness_report['completeness_score'] = 1.0 - (total_missing / (len(df) * len(df.columns)))
        
        logger.info(f"Controllo completezza completato - punteggio: {completeness_report['completeness_score']:.2f}")
        return completeness_report
    
    def validate_data_consistency(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Controlla la consistenza dei dati.
        
        Args:
            df: DataFrame da analizzare
            
        Returns:
            Report consistenza
        """
        logger.info("Controllo consistenza dati")
        
        consistency_report = {
            'timestamp': datetime.now().isoformat(),
            'total_rows': len(df),
            'consistency_score': 1.0,
            'inconsistencies': [],
            'duplicates_found': 0,
            'timestamp_issues': []
        }
        
        # Controllo duplicati
        initial_count = len(df)
        df_unique = df.unique()
        duplicates = initial_count - len(df_unique)
        
        if duplicates > 0:
            consistency_report['duplicates_found'] = duplicates
            consistency_report['inconsistencies'].append(f"Trovari {duplicates} record duplicati")
            consistency_report['consistency_score'] -= 0.1
        
        # Controllo timestamp
        if 'timestamp' in df.columns:
            timestamp_issues = self._check_timestamp_consistency(df)
            consistency_report['timestamp_issues'] = timestamp_issues
            
            if timestamp_issues:
                consistency_report['consistency_score'] -= 0.2
        
        # Controllo sequenze temporali per ogni caso
        if 'case_id' in df.columns and 'timestamp' in df.columns:
            sequence_issues = self._check_temporal_sequences(df)
            if sequence_issues:
                consistency_report['inconsistencies'].extend(sequence_issues)
                consistency_report['consistency_score'] -= 0.3
        
        # Normalizza punteggio tra 0 e 1
        consistency_report['consistency_score'] = max(0.0, min(1.0, consistency_report['consistency_score']))
        
        logger.info(f"Controllo consistenza completato - punteggio: {consistency_report['consistency_score']:.2f}")
        return consistency_report
    
    def generate_data_quality_report(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Genera un report completo sulla qualità dati.
        
        Args:
            df: DataFrame da analizzare
            
        Returns:
            Report qualità completo
        """
        logger.info("Generazione report qualità dati completo")
        
        quality_report = {
            'timestamp': datetime.now().isoformat(),
            'environment': settings.environment,
            'dataset_info': {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'columns': df.columns
            },
            'schema_validation': self.validate_event_log_schema(df),
            'completeness_validation': self.validate_data_completeness(df),
            'consistency_validation': self.validate_data_consistency(df),
            'overall_score': 0.0,
            'recommendations': []
        }
        
        # Calcola punteggio complessivo
        schema_score = 1.0 if quality_report['schema_validation']['validation_passed'] else 0.0
        completeness_score = quality_report['completeness_validation']['completeness_score']
        consistency_score = quality_report['consistency_validation']['consistency_score']
        
        quality_report['overall_score'] = (schema_score + completeness_score + consistency_score) / 3
        
        # Genera raccomandazioni
        quality_report['recommendations'] = self._generate_recommendations(quality_report)
        
        # Salva report completo
        self._save_validation_report(quality_report, "data_quality_complete")
        
        logger.info(f"Report qualità dati completato - punteggio: {quality_report['overall_score']:.2f}")
        return quality_report
    
    def _calculate_log_statistics(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Calcola statistiche base dell'event log."""
        stats = {
            'unique_cases': len(df['case_id'].unique()) if 'case_id' in df.columns else 0,
            'unique_activities': len(df['activity'].unique()) if 'activity' in df.columns else 0,
            'time_range': None,
            'avg_events_per_case': 0.0
        }
        
        if 'case_id' in df.columns and 'timestamp' in df.columns:
            # Range temporale
            min_time = df['timestamp'].min()
            max_time = df['timestamp'].max()
            if min_time and max_time:
                stats['time_range'] = {
                    'start': min_time.isoformat(),
                    'end': max_time.isoformat()
                }
            
            # Eventi medi per caso
            case_counts = df.groupby('case_id').count()
            stats['avg_events_per_case'] = case_counts['count'].mean()
        
        return stats
    
    def _run_additional_checks(self, df: pl.DataFrame) -> Dict[str, List[str]]:
        """Esegue controlli aggiuntivi sulla qualità dati."""
        checks = {'errors': [], 'warnings': []}
        
        # Controllo valori vuoti in colonne critiche
        critical_columns = ['case_id', 'activity', 'timestamp']
        for column in critical_columns:
            if column in df.columns:
                empty_count = df.filter(pl.col(column) == '').height()
                if empty_count > 0:
                    checks['errors'].append(f"Colonna {column} ha {empty_count} valori vuoti")
        
        # Controllo tipi dati
        if 'timestamp' in df.columns:
            if not pl.datatypes.is_temporal(df['timestamp'].dtype):
                checks['warnings'].append("Colonna timestamp non è di tipo temporale")
        
        return checks
    
    def _check_timestamp_consistency(self, df: pl.DataFrame) -> List[str]:
        """Controlla la consistenza dei timestamp."""
        issues = []
        
        if 'timestamp' not in df.columns:
            return issues
        
        # Controlla timestamp futuri
        future_timestamps = df.filter(pl.col('timestamp') > datetime.now())
        if len(future_timestamps) > 0:
            issues.append(f"Trovari {len(future_timestamps)} timestamp futuri")
        
        # Controlla timestamp nulli
        null_timestamps = df.filter(pl.col('timestamp').is_null())
        if len(null_timestamps) > 0:
            issues.append(f"Trovari {len(null_timestamps)} timestamp nulli")
        
        return issues
    
    def _check_temporal_sequences(self, df: pl.DataFrame) -> List[str]:
        """Controlla la correttezza delle sequenze temporali."""
        issues = []
        
        try:
            # Ordina per case_id e timestamp
            df_sorted = df.sort(['case_id', 'timestamp'])
            
            # Controlla per ogni caso se ci sono timestamp non crescenti
            for case_id in df_sorted['case_id'].unique():
                case_events = df_sorted.filter(pl.col('case_id') == case_id)
                timestamps = case_events['timestamp'].to_list()
                
                for i in range(1, len(timestamps)):
                    if timestamps[i] < timestamps[i-1]:
                        issues.append(f"Sequenza temporale non valida per caso {case_id}")
                        break
        
        except Exception as e:
            issues.append(f"Errore nel controllo sequenze temporali: {e}")
        
        return issues
    
    def _generate_recommendations(self, quality_report: Dict[str, Any]) -> List[str]:
        """Genera raccomandazioni basate sui risultati della validazione."""
        recommendations = []
        
        # Raccomandazioni basate sul punteggio complessivo
        overall_score = quality_report['overall_score']
        if overall_score < 0.5:
            recommendations.append("Qualità dati molto bassa: revisione completa necessaria")
        elif overall_score < 0.7:
            recommendations.append("Qualità dati accettabile ma migliorabile")
        elif overall_score < 0.9:
            recommendations.append("Buona qualità dati, piccoli miglioramenti possibili")
        else:
            recommendations.append("Eccellente qualità dati")
        
        # Raccomandazioni specifiche
        if not quality_report['schema_validation']['validation_passed']:
            recommendations.append("Correggere gli errori di schema prima dell'analisi")
        
        if quality_report['completeness_validation']['completeness_score'] < 0.8:
            recommendations.append("Migliorare la completezza dei dati, soprattutto per colonne critiche")
        
        if quality_report['consistency_validation']['consistency_score'] < 0.8:
            recommendations.append("Risolvere problemi di consistenza (duplicati, timestamp)")
        
        return recommendations
    
    def _save_validation_report(self, report: Dict[str, Any], report_type: str) -> None:
        """Salva il report di validazione."""
        import json
        from pathlib import Path
        
        # Crea directory se non esiste
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{report_type}_{timestamp}.json"
        filepath = self.logs_dir / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            logger.info(f"Report validazione salvato in: {filepath}")
        except Exception as e:
            logger.error(f"Errore nel salvataggio report validazione: {e}")

# Creazione istanza globale
data_quality_service = DataQualityService()