"""
Data Service Layer - Astrazione per l'accesso ai dati.

Questo modulo fornisce un'interfaccia pulita per l'accesso ai dati,
nascondendo i dettagli implementativi del database e fornendo operazioni
standardizzate per tutte le API.
"""

import json
from typing import Dict, List, Optional, Any
from datetime import datetime
import polars as pl

from app.core.logger import get_logger
from app.core.database import save_event_log, load_event_log
from app.core.config import settings

logger = get_logger()


class DataRepository:
    """Repository per l'accesso ai dati con astrazione del database."""
    
    def __init__(self):
        self.db_path = settings.processed_data_dir / "etl_results"
        self.db_path.mkdir(parents=True, exist_ok=True)
    
    async def get_latest_event_log(self, limit: Optional[int] = None) -> pl.DataFrame:
        """
        Recupera l'event log più recente dal database.
        
        Args:
            limit: Limite opzionale sul numero di righe
            
        Returns:
            Polars DataFrame con l'event log
        """
        try:
            logger.info("Recupero event log più recente dal database")
            df = load_event_log("event_log")
            
            if df.is_empty():
                logger.warning("Nessun event log trovato nel database")
                return pl.DataFrame()
            
            if limit:
                df = df.head(limit)
            
            logger.info(f"Event log recuperato: {len(df)} righe")
            return df
            
        except Exception as e:
            logger.error(f"Errore nel recupero event log: {e}")
            return pl.DataFrame()
    
    async def save_event_log(self, df: pl.DataFrame, table_name: str = "event_log") -> bool:
        """
        Salva un event log nel database.
        
        Args:
            df: DataFrame da salvare
            table_name: Nome della tabella
            
        Returns:
            bool: True se salvataggio riuscito
        """
        try:
            if df.is_empty():
                logger.warning("Tentativo di salvare event log vuoto")
                return False
            
            logger.info(f"Salvataggio event log ({len(df)} righe) nel database")
            save_event_log(df, table_name)
            logger.info("Event log salvato con successo")
            return True
            
        except Exception as e:
            logger.error(f"Errore nel salvataggio event log: {e}")
            return False
    
    async def get_data_quality_report(self, table_name: str = "event_log") -> Dict[str, Any]:
        """
        Recupera l'ultimo report di qualità dati.
        
        Returns:
            Dict con il report di qualità dati
        """
        try:
            reports_dir = self.db_path / "quality_reports"
            if not reports_dir.exists():
                return {"error": "Nessun report di qualità dati disponibile"}
            
            # Trova il report più recente
            report_files = list(reports_dir.glob("*.json"))
            if not report_files:
                return {"error": "Nessun report di qualità dati disponibile"}
            
            latest_report = max(report_files, key=lambda f: f.stat().st_mtime)
            
            with open(latest_report, 'r', encoding='utf-8') as f:
                report = json.load(f)
            
            logger.info(f"Report qualità dati recuperato: {latest_report.name}")
            return report
            
        except Exception as e:
            logger.error(f"Errore nel recupero report qualità dati: {e}")
            return {"error": str(e)}
    
    async def save_data_quality_report(self, report: Dict[str, Any]) -> bool:
        """
        Salva un report di qualità dati.
        
        Args:
            report: Report da salvare
            
        Returns:
            bool: True se salvataggio riuscito
        """
        try:
            reports_dir = self.db_path / "quality_reports"
            reports_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"quality_report_{timestamp}.json"
            filepath = reports_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Report qualità dati salvato: {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Errore nel salvataggio report qualità dati: {e}")
            return False
    
    async def get_process_discovery_results(self, process_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Recupera i risultati dell'analisi di discovery.
        
        Args:
            process_id: ID opzionale del processo
            
        Returns:
            Dict con i risultati del discovery
        """
        try:
            discovery_dir = self.db_path / "discovery_results"
            if not discovery_dir.exists():
                return {"error": "Nessun risultato discovery disponibile"}
            
            if process_id:
                # Cerca risultati specifici per processo
                result_files = list(discovery_dir.glob(f"*{process_id}*.json"))
            else:
                # Cerca tutti i risultati
                result_files = list(discovery_dir.glob("*.json"))
            
            if not result_files:
                return {"error": "Nessun risultato discovery disponibile"}
            
            # Leggi tutti i file trovati
            results = []
            for file_path in result_files:
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        result_data = json.load(f)
                        results.append(result_data)
                except Exception as e:
                    logger.warning(f"Errore nella lettura file discovery {file_path}: {e}")
                    continue
            
            if not results:
                return {"error": "Nessun risultato discovery valido disponibile"}
            
            # Se c'è un solo risultato, restituiscilo direttamente
            if len(results) == 1:
                return results[0]
            
            # Altrimenti restituisci tutti i risultati
            return {
                "multiple_results": True,
                "results": results,
                "count": len(results)
            }
            
        except Exception as e:
            logger.error(f"Errore nel recupero risultati discovery: {e}")
            return {"error": str(e)}
    
    async def save_process_discovery_results(self, results: Dict[str, Any], process_id: Optional[str] = None) -> bool:
        """
        Salva i risultati dell'analisi di discovery.
        
        Args:
            results: Risultati da salvare
            process_id: ID opzionale del processo
            
        Returns:
            bool: True se salvataggio riuscito
        """
        try:
            discovery_dir = self.db_path / "discovery_results"
            discovery_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if process_id:
                filename = f"discovery_{process_id}_{timestamp}.json"
            else:
                filename = f"discovery_{timestamp}.json"
            
            filepath = discovery_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Risultati discovery salvati: {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Errore nel salvataggio risultati discovery: {e}")
            return False
    
    async def get_kpi_metrics(self, time_range: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Recupera le metriche KPI calcolate.
        
        Args:
            time_range: Filtro temporale opzionale
            
        Returns:
            Dict con le metriche KPI
        """
        try:
            kpi_dir = self.db_path / "kpi_metrics"
            if not kpi_dir.exists():
                return {"error": "Nessuna metrica KPI disponibile"}
            
            # Trova l'ultimo file KPI
            kpi_files = list(kpi_dir.glob("*.json"))
            if not kpi_files:
                return {"error": "Nessuna metrica KPI disponibile"}
            
            latest_kpi = max(kpi_files, key=lambda f: f.stat().st_mtime)
            
            with open(latest_kpi, 'r', encoding='utf-8') as f:
                kpi_data = json.load(f)
            
            # Applica filtro temporale se specificato
            if time_range and 'start_date' in time_range and 'end_date' in time_range:
                start_date = datetime.fromisoformat(time_range['start_date'])
                end_date = datetime.fromisoformat(time_range['end_date'])
                
                # Filtra le metriche per data (se presenti)
                if 'metrics' in kpi_data and isinstance(kpi_data['metrics'], list):
                    filtered_metrics = []
                    for metric in kpi_data['metrics']:
                        if 'timestamp' in metric:
                            metric_date = datetime.fromisoformat(metric['timestamp'])
                            if start_date <= metric_date <= end_date:
                                filtered_metrics.append(metric)
                    kpi_data['metrics'] = filtered_metrics
            
            logger.info(f"Metriche KPI recuperate: {latest_kpi.name}")
            return kpi_data
            
        except Exception as e:
            logger.error(f"Errore nel recupero metriche KPI: {e}")
            return {"error": str(e)}
    
    async def save_kpi_metrics(self, metrics: Dict[str, Any]) -> bool:
        """
        Salva le metriche KPI calcolate.
        
        Args:
            metrics: Metriche da salvare
            
        Returns:
            bool: True se salvataggio riuscito
        """
        try:
            kpi_dir = self.db_path / "kpi_metrics"
            kpi_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"kpi_metrics_{timestamp}.json"
            filepath = kpi_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(metrics, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Metriche KPI salvate: {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Errore nel salvataggio metriche KPI: {e}")
            return False
    
    async def get_analytics_models(self, model_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Recupera i modelli di analytics salvati.
        
        Args:
            model_type: Tipo di modello opzionale
            
        Returns:
            Dict con i modelli analytics
        """
        try:
            models_dir = self.db_path / "analytics_models"
            if not models_dir.exists():
                return {"error": "Nessun modello analytics disponibile"}
            
            if model_type:
                # Cerca modelli specifici per tipo
                model_files = list(models_dir.glob(f"*{model_type}*.json"))
            else:
                # Cerca tutti i modelli
                model_files = list(models_dir.glob("*.json"))
            
            if not model_files:
                return {"error": "Nessun modello analytics disponibile"}
            
            # Leggi tutti i file trovati
            models = []
            for file_path in model_files:
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        model_data = json.load(f)
                        models.append(model_data)
                except Exception as e:
                    logger.warning(f"Errore nella lettura file modello {file_path}: {e}")
                    continue
            
            if not models:
                return {"error": "Nessun modello analytics valido disponibile"}
            
            # Se c'è un solo modello, restituiscilo direttamente
            if len(models) == 1:
                return models[0]
            
            # Altrimenti restituisci tutti i modelli
            return {
                "multiple_models": True,
                "models": models,
                "count": len(models)
            }
            
        except Exception as e:
            logger.error(f"Errore nel recupero modelli analytics: {e}")
            return {"error": str(e)}
    
    async def save_analytics_model(self, model: Dict[str, Any], model_type: str, model_id: Optional[str] = None) -> bool:
        """
        Salva un modello di analytics.
        
        Args:
            model: Modello da salvare
            model_type: Tipo di modello
            model_id: ID opzionale del modello
            
        Returns:
            bool: True se salvataggio riuscito
        """
        try:
            models_dir = self.db_path / "analytics_models"
            models_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if model_id:
                filename = f"model_{model_type}_{model_id}_{timestamp}.json"
            else:
                filename = f"model_{model_type}_{timestamp}.json"
            
            filepath = models_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(model, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Modello analytics salvato: {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Errore nel salvataggio modello analytics: {e}")
            return False
    
    async def get_system_status(self) -> Dict[str, Any]:
        """
        Recupera lo stato del sistema e dei dati disponibili.
        
        Returns:
            Dict con lo stato del sistema
        """
        try:
            status = {
                "timestamp": datetime.now().isoformat(),
                "data_availability": {},
                "last_updates": {},
                "file_counts": {}
            }
            
            # Controlla disponibilità dati
            data_dirs = {
                "event_log": self.db_path,
                "quality_reports": self.db_path / "quality_reports",
                "discovery_results": self.db_path / "discovery_results",
                "kpi_metrics": self.db_path / "kpi_metrics",
                "analytics_models": self.db_path / "analytics_models",
                "external_cards": self.db_path / "external_cards",
                "cache": self.db_path / "cache"
            }
            
            for data_type, dir_path in data_dirs.items():
                if dir_path.exists():
                    files = list(dir_path.glob("*.json"))
                    if files:
                        latest_file = max(files, key=lambda f: f.stat().st_mtime)
                        status["data_availability"][data_type] = True
                        status["last_updates"][data_type] = datetime.fromtimestamp(
                            latest_file.stat().st_mtime
                        ).isoformat()
                        status["file_counts"][data_type] = len(files)
                    else:
                        status["data_availability"][data_type] = False
                        status["last_updates"][data_type] = None
                        status["file_counts"][data_type] = 0
                else:
                    status["data_availability"][data_type] = False
                    status["last_updates"][data_type] = None
                    status["file_counts"][data_type] = 0
            
            # Controlla event log nel database
            try:
                df = load_event_log("event_log")
                status["data_availability"]["database_event_log"] = not df.is_empty()
                status["record_counts"] = {"event_log": len(df) if not df.is_empty() else 0}
            except Exception:
                status["data_availability"]["database_event_log"] = False
                status["record_counts"] = {"event_log": 0}
            
            logger.info("Stato sistema recuperato")
            return status
            
        except Exception as e:
            logger.error(f"Errore nel recupero stato sistema: {e}")
            return {"error": str(e), "timestamp": datetime.now().isoformat()}
    
    async def save_external_card_config(self, card_id: str, config: Dict[str, Any]) -> bool:
        """
        Salva configurazione card esterna.
        
        Args:
            card_id: ID della card
            config: Configurazione da salvare
            
        Returns:
            bool: True se salvataggio riuscito
        """
        try:
            cards_dir = self.db_path / "external_cards"
            cards_dir.mkdir(exist_ok=True)
            
            filename = f"card_{card_id}.json"
            filepath = cards_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Configurazione card salvata: {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Errore nel salvataggio configurazione card: {e}")
            return False
    
    async def get_external_card_config(self, card_id: str) -> Optional[Dict[str, Any]]:
        """
        Recupera configurazione card esterna.
        
        Args:
            card_id: ID della card
            
        Returns:
            Dict con configurazione o None se non trovata
        """
        try:
            cards_dir = self.db_path / "external_cards"
            if not cards_dir.exists():
                return None
            
            filename = f"card_{card_id}.json"
            filepath = cards_dir / filename
            
            if not filepath.exists():
                return None
            
            with open(filepath, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            logger.info(f"Configurazione card recuperata: {filepath}")
            return config
            
        except Exception as e:
            logger.error(f"Errore nel recupero configurazione card: {e}")
            return None
    
    async def list_external_card_configs(self) -> List[Dict[str, Any]]:
        """
        Lista tutte le configurazioni card esterne.
        
        Returns:
            Lista di configurazioni card
        """
        try:
            cards_dir = self.db_path / "external_cards"
            if not cards_dir.exists():
                return []
            
            card_files = list(cards_dir.glob("card_*.json"))
            if not card_files:
                return []
            
            configs = []
            for file_path in card_files:
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        config = json.load(f)
                        configs.append(config)
                except Exception as e:
                    logger.warning(f"Errore nella lettura file card {file_path}: {e}")
                    continue
            
            logger.info(f"Recuperate {len(configs)} configurazioni card")
            return configs
            
        except Exception as e:
            logger.error(f"Errore nel recupero lista configurazioni card: {e}")
            return []
    
    async def delete_external_card_config(self, card_id: str) -> bool:
        """
        Elimina configurazione card esterna.
        
        Args:
            card_id: ID della card
            
        Returns:
            bool: True se eliminazione riuscita
        """
        try:
            cards_dir = self.db_path / "external_cards"
            if not cards_dir.exists():
                return False
            
            filename = f"card_{card_id}.json"
            filepath = cards_dir / filename
            
            if not filepath.exists():
                return False
            
            filepath.unlink()
            logger.info(f"Configurazione card eliminata: {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Errore nell'eliminazione configurazione card: {e}")
            return False
    
    async def get_cached_data(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """
        Recupera dati dalla cache.
        
        Args:
            cache_key: Chiave cache
            
        Returns:
            Dict con dati cached o None se non trovato/scaduto
        """
        try:
            cache_dir = self.db_path / "cache"
            if not cache_dir.exists():
                return None
            
            # Crea nome file sicuro dalla chiave
            safe_key = cache_key.replace(":", "_").replace("/", "_")
            filename = f"{safe_key}.json"
            filepath = cache_dir / filename
            
            if not filepath.exists():
                return None
            
            with open(filepath, 'r', encoding='utf-8') as f:
                cached = json.load(f)
            
            logger.debug(f"Dati cache recuperati: {cache_key}")
            return cached
            
        except Exception as e:
            logger.error(f"Errore nel recupero cache: {e}")
            return None
    
    async def save_cached_data(self, cache_key: str, data: Dict[str, Any]) -> bool:
        """
        Salva dati in cache.
        
        Args:
            cache_key: Chiave cache
            data: Dati da salvare
            
        Returns:
            bool: True se salvataggio riuscito
        """
        try:
            cache_dir = self.db_path / "cache"
            cache_dir.mkdir(exist_ok=True)
            
            # Crea nome file sicuro dalla chiave
            safe_key = cache_key.replace(":", "_").replace("/", "_")
            filename = f"{safe_key}.json"
            filepath = cache_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"Dati cache salvati: {cache_key}")
            return True
            
        except Exception as e:
            logger.error(f"Errore nel salvataggio cache: {e}")
            return False
    
    async def delete_cached_data(self, cache_key: str) -> bool:
        """
        Elimina dati dalla cache.
        
        Args:
            cache_key: Chiave cache
            
        Returns:
            bool: True se eliminazione riuscita
        """
        try:
            cache_dir = self.db_path / "cache"
            if not cache_dir.exists():
                return False
            
            # Crea nome file sicuro dalla chiave
            safe_key = cache_key.replace(":", "_").replace("/", "_")
            filename = f"{safe_key}.json"
            filepath = cache_dir / filename
            
            if not filepath.exists():
                return False
            
            filepath.unlink()
            logger.debug(f"Dati cache eliminati: {cache_key}")
            return True
            
        except Exception as e:
            logger.error(f"Errore nell'eliminazione cache: {e}")
            return False
    
    async def invalidate_cache_pattern(self, pattern: str) -> bool:
        """
        Invalida tutti i dati cache che corrispondono a un pattern.
        
        Args:
            pattern: Pattern da cercare (es. "card:123:*")
            
        Returns:
            bool: True se operazione riuscita
        """
        try:
            cache_dir = self.db_path / "cache"
            if not cache_dir.exists():
                return True
            
            # Converti pattern in glob pattern
            glob_pattern = pattern.replace("*", "*.json").replace(":", "_").replace("/", "_")
            cache_files = list(cache_dir.glob(glob_pattern))
            
            deleted_count = 0
            for file_path in cache_files:
                try:
                    file_path.unlink()
                    deleted_count += 1
                except Exception as e:
                    logger.warning(f"Errore nell'eliminazione file cache {file_path}: {e}")
                    continue
            
            logger.info(f"Invalidati {deleted_count} file cache per pattern: {pattern}")
            return True
            
        except Exception as e:
            logger.error(f"Errore nell'invalidazione cache: {e}")
            return False


# Creazione istanza globale
data_repository = DataRepository()


# Funzioni helper per l'uso sincrono
def get_data_repository() -> DataRepository:
    """Restituisce l'istanza del data repository."""
    return data_repository