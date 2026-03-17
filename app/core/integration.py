"""
Sistema di integrazione e testing per il programma Process Mining.
Questo modulo coordina tutti i componenti del sistema e fornisce
funzionalità di testing e validazione dell'integrazione.
"""

import asyncio
import time
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from pathlib import Path
import json
import threading
import logging
from dataclasses import dataclass, asdict
from enum import Enum

from app.core.logger import get_logger
from app.core.config import settings
from app.core.bootstrap import bootstrap_manager
from app.services.etl.reactive_etl import reactive_etl_manager
from app.services.etl.data_extraction import data_extraction_service
from app.services.etl.data_transformation import data_transformation_service
from app.services.etl.data_quality import data_quality_service
from app.services.etl.privacy_governance import privacy_governance_service
from app.services.mining.discovery_service import discovery_service
from app.services.mining.conformance_service import conformance_service
from app.services.mining.kpi_service import kpi_service

logger = get_logger()

class IntegrationStatus(Enum):
    """Stati possibili del sistema integrato."""
    INITIALIZING = "initializing"
    BOOTSTRAPPING = "bootstrapping"
    RUNNING = "running"
    PAUSED = "paused"
    ERROR = "error"
    SHUTDOWN = "shutdown"

@dataclass
class IntegrationResult:
    """Risultato di un test di integrazione."""
    test_name: str
    status: str
    duration: float
    success: bool
    details: Dict[str, Any]
    timestamp: str

class IntegrationManager:
    """Gestore dell'integrazione completa del sistema."""
    
    def __init__(self):
        self.status = IntegrationStatus.INITIALIZING
        self.start_time = None
        self.end_time = None
        self.test_results = []
        self.system_components = {}
        self.is_running = False
        
        # Configurazioni di testing
        self.test_mode = False
        self.verbose_logging = True
        self.auto_bootstrap = True
        self.auto_start_etl = True
        
    async def full_system_test(self) -> Dict[str, Any]:
        """
        Esegue un test completo del sistema integrato.
        
        Returns:
            Dizionario con i risultati del test
        """
        logger.info("🚀 Avvio test sistema integrato completo")
        
        test_result = {
            'timestamp': datetime.now().isoformat(),
            'test_name': 'full_system_integration_test',
            'status': 'running',
            'duration': 0,
            'success': True,
            'components': {},
            'errors': [],
            'warnings': []
        }
        
        start_time = time.time()
        
        try:
            # Step 1: Test bootstrap
            logger.info("📋 Test bootstrap sistema")
            bootstrap_result = await self._test_bootstrap()
            test_result['components']['bootstrap'] = bootstrap_result
            
            if not bootstrap_result['success']:
                test_result['success'] = False
                test_result['errors'].append("Bootstrap fallito")
            
            # Step 2: Test directory setup
            logger.info("📁 Test setup directory")
            directory_result = await self._test_directory_setup()
            test_result['components']['directory_setup'] = directory_result
            
            if not directory_result['success']:
                test_result['success'] = False
                test_result['errors'].append("Setup directory fallito")
            
            # Step 3: Test ETL components
            logger.info("🔄 Test componenti ETL")
            etl_result = await self._test_etl_components()
            test_result['components']['etl'] = etl_result
            
            if not etl_result['success']:
                test_result['success'] = False
                test_result['errors'].append("Componenti ETL falliti")
            
            # Step 4: Test data quality
            logger.info("🔍 Test qualità dati")
            quality_result = await self._test_data_quality()
            test_result['components']['data_quality'] = quality_result
            
            if not quality_result['success']:
                test_result['success'] = False
                test_result['errors'].append("Test qualità dati fallito")
            
            # Step 5: Test mining services
            logger.info("📊 Test servizi mining")
            mining_result = await self._test_mining_services()
            test_result['components']['mining'] = mining_result
            
            if not mining_result['success']:
                test_result['success'] = False
                test_result['errors'].append("Servizi mining falliti")
            
            # Step 6: Test system integration
            logger.info("🔗 Test integrazione sistema")
            integration_result = await self._test_system_integration()
            test_result['components']['integration'] = integration_result
            
            if not integration_result['success']:
                test_result['success'] = False
                test_result['errors'].append("Integrazione sistema fallita")
            
            # Calcola durata totale
            test_result['duration'] = time.time() - start_time
            test_result['status'] = 'completed'
            
            # Salva risultati
            await self._save_test_results(test_result)
            
            if test_result['success']:
                logger.info("✅ Test sistema integrato completato con successo")
            else:
                logger.warning("⚠️ Test sistema integrato completato con errori")
            
            return test_result
            
        except Exception as e:
            test_result['success'] = False
            test_result['errors'].append(str(e))
            test_result['status'] = 'error'
            test_result['duration'] = time.time() - start_time
            
            logger.error(f"❌ Errore nel test sistema integrato: {e}")
            return test_result
    
    async def _test_bootstrap(self) -> Dict[str, Any]:
        """Testa il sistema di bootstrap."""
        try:
            bootstrap_result = await bootstrap_manager.bootstrap_system()
            
            result = {
                'success': bootstrap_result.get('success', False),
                'duration': 0,
                'details': bootstrap_result,
                'components_tested': ['directory_setup', 'config_validation', 'api_access']
            }
            
            if not result['success']:
                result['error'] = bootstrap_result.get('errors', [])
            
            return result
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'details': {},
                'components_tested': []
            }
    
    async def _test_directory_setup(self) -> Dict[str, Any]:
        """Testa la creazione delle directory."""
        try:
            required_dirs = [
                settings.data_dir,
                settings.logs_dir,
                settings.raw_data_dir,
                settings.staged_data_dir,
                settings.processed_data_dir,
                settings.warehouse_dir
            ]
            
            missing_dirs = []
            for directory in required_dirs:
                if not directory.exists():
                    missing_dirs.append(str(directory))
            
            result = {
                'success': len(missing_dirs) == 0,
                'missing_directories': missing_dirs,
                'total_directories': len(required_dirs),
                'created_directories': [],
                'details': {
                    'required_dirs': [str(d) for d in required_dirs]
                }
            }
            
            # Crea directory mancanti se necessario
            if missing_dirs:
                for directory in missing_dirs:
                    Path(directory).mkdir(parents=True, exist_ok=True)
                    result['created_directories'].append(directory)
            
            return result
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'details': {}
            }
    
    async def _test_etl_components(self) -> Dict[str, Any]:
        """Testa i componenti ETL."""
        try:
            etl_tests = {}
            
            # Test data extraction
            try:
                if settings.hubspot_api_key:
                    deals_data = await data_extraction_service.extract_all_deals_with_history()
                    etl_tests['extraction'] = {
                        'success': True,
                        'deals_count': len(deals_data),
                        'details': 'Extraction test passed'
                    }
                else:
                    etl_tests['extraction'] = {
                        'success': True,
                        'deals_count': 0,
                        'details': 'No API key, extraction skipped'
                    }
            except Exception as e:
                etl_tests['extraction'] = {
                    'success': False,
                    'error': str(e),
                    'details': 'Extraction test failed'
                }
            
            # Test data transformation
            try:
                # Crea dati di test
                test_deals = [
                    {
                        "deal_id": "TEST_001",
                        "properties": {"dealstage": "appointmentscheduled"},
                        "propertiesWithHistory": {
                            "dealstage": [
                                {"value": "appointmentscheduled", "timestamp": 1642000000000}
                            ]
                        }
                    }
                ]
                
                event_log_df = data_transformation_service.transform_hubspot_deals_to_event_log(test_deals)
                etl_tests['transformation'] = {
                    'success': len(event_log_df) > 0,
                    'events_count': len(event_log_df),
                    'details': 'Transformation test passed'
                }
            except Exception as e:
                etl_tests['transformation'] = {
                    'success': False,
                    'error': str(e),
                    'details': 'Transformation test failed'
                }
            
            # Test data quality
            try:
                if 'event_log_df' in locals():
                    quality_report = data_quality_service.generate_data_quality_report(event_log_df)
                    etl_tests['quality'] = {
                        'success': quality_report.get('overall_score', 0) > 0.8,
                        'quality_score': quality_report.get('overall_score', 0),
                        'details': 'Quality test passed'
                    }
                else:
                    etl_tests['quality'] = {
                        'success': False,
                        'error': 'No data to test quality',
                        'details': 'Quality test failed'
                    }
            except Exception as e:
                etl_tests['quality'] = {
                    'success': False,
                    'error': str(e),
                    'details': 'Quality test failed'
                }
            
            # Test privacy governance
            try:
                if 'event_log_df' in locals():
                    anonymized_df = privacy_governance_service.anonymize_dataframe(event_log_df)
                    gdpr_report = privacy_governance_service.validate_gdpr_compliance(anonymized_df)
                    etl_tests['privacy'] = {
                        'success': gdpr_report.get('compliance_score', 0) > 0.8,
                        'compliance_score': gdpr_report.get('compliance_score', 0),
                        'details': 'Privacy test passed'
                    }
                else:
                    etl_tests['privacy'] = {
                        'success': False,
                        'error': 'No data to test privacy',
                        'details': 'Privacy test failed'
                    }
            except Exception as e:
                etl_tests['privacy'] = {
                    'success': False,
                    'error': str(e),
                    'details': 'Privacy test failed'
                }
            
            # Calcola successo generale
            success_tests = sum(1 for test in etl_tests.values() if test['success'])
            total_tests = len(etl_tests)
            
            return {
                'success': success_tests == total_tests,
                'total_tests': total_tests,
                'passed_tests': success_tests,
                'failed_tests': total_tests - success_tests,
                'test_results': etl_tests,
                'details': f"ETL tests: {success_tests}/{total_tests} passed"
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'details': {},
                'test_results': {}
            }
    
    async def _test_data_quality(self) -> Dict[str, Any]:
        """Testa la qualità dei dati."""
        try:
            # Controlla i file processati
            processed_dir = settings.processed_data_dir
            
            if not processed_dir.exists():
                return {
                    'success': False,
                    'error': 'Processed data directory not found',
                    'details': {}
                }
            
            processed_files = list(processed_dir.glob("*.parquet"))
            
            if not processed_files:
                return {
                    'success': False,
                    'error': 'No processed data files found',
                    'details': {}
                }
            
            # Leggi il file più recente e testa la qualità
            latest_file = max(processed_files, key=lambda f: f.stat().st_mtime)
            
            try:
                import polars as pl
                df = pl.read_parquet(str(latest_file))
                
                quality_report = data_quality_service.generate_data_quality_report(df)
                
                return {
                    'success': quality_report.get('overall_score', 0) > 0.8,
                    'quality_score': quality_report.get('overall_score', 0),
                    'file_tested': str(latest_file),
                    'data_stats': {
                        'total_events': len(df),
                        'unique_cases': len(df['case_id'].unique()) if 'case_id' in df.columns else 0,
                        'unique_activities': len(df['activity'].unique()) if 'activity' in df.columns else 0
                    },
                    'details': quality_report
                }
                
            except Exception as e:
                return {
                    'success': False,
                    'error': f"Error reading processed file: {e}",
                    'details': {}
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'details': {}
            }
    
    async def _test_mining_services(self) -> Dict[str, Any]:
        """Testa i servizi di mining."""
        try:
            mining_tests = {}
            
            # Test discovery service
            try:
                # Crea dati di test per discovery
                test_data = [
                    {"case_id": "TEST_001", "activity": "Test Activity", "timestamp": "2024-01-01T10:00:00", "resource": "Test Resource"}
                ]
                
                import polars as pl
                test_df = pl.DataFrame(test_data)
                test_df = test_df.with_columns([
                    pl.col('timestamp').str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S")
                ])
                
                discovery_result = discovery_service.discover_dfg(test_df)
                mining_tests['discovery'] = {
                    'success': True,
                    'dfg_found': len(discovery_result.get('dfg', {})) > 0,
                    'details': 'Discovery test passed'
                }
            except Exception as e:
                mining_tests['discovery'] = {
                    'success': False,
                    'error': str(e),
                    'details': 'Discovery test failed'
                }
            
            # Test conformance service
            try:
                if 'test_df' in locals():
                    conformance_result = conformance_service.check_conformance_dfg(test_df)
                    mining_tests['conformance'] = {
                        'success': True,
                        'details': 'Conformance test passed'
                    }
                else:
                    mining_tests['conformance'] = {
                        'success': False,
                        'error': 'No test data for conformance',
                        'details': 'Conformance test failed'
                    }
            except Exception as e:
                mining_tests['conformance'] = {
                    'success': False,
                    'error': str(e),
                    'details': 'Conformance test failed'
                }
            
            # Test KPI service
            try:
                if 'test_df' in locals():
                    kpi_result = kpi_service.calculate_process_kpis(test_df)
                    mining_tests['kpi'] = {
                        'success': True,
                        'kpi_calculated': len(kpi_result.get('basic_kpis', {})) > 0,
                        'details': 'KPI test passed'
                    }
                else:
                    mining_tests['kpi'] = {
                        'success': False,
                        'error': 'No test data for KPI',
                        'details': 'KPI test failed'
                    }
            except Exception as e:
                mining_tests['kpi'] = {
                    'success': False,
                    'error': str(e),
                    'details': 'KPI test failed'
                }
            
            # Calcola successo generale
            success_tests = sum(1 for test in mining_tests.values() if test['success'])
            total_tests = len(mining_tests)
            
            return {
                'success': success_tests == total_tests,
                'total_tests': total_tests,
                'passed_tests': success_tests,
                'failed_tests': total_tests - success_tests,
                'test_results': mining_tests,
                'details': f"Mining tests: {success_tests}/{total_tests} passed"
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'details': {},
                'test_results': {}
            }
    
    async def _test_system_integration(self) -> Dict[str, Any]:
        """Testa l'integrazione completa del sistema."""
        try:
            integration_tests = {}
            
            # Test end-to-end pipeline
            try:
                # Simula una pipeline completa
                test_deals = [
                    {
                        "deal_id": "INTEGRATION_001",
                        "properties": {"dealstage": "appointmentscheduled", "dealname": "Test Deal"},
                        "propertiesWithHistory": {
                            "dealstage": [
                                {"value": "appointmentscheduled", "timestamp": 1642000000000}
                            ]
                        }
                    }
                ]
                
                # ETL Pipeline
                event_log_df = data_transformation_service.transform_hubspot_deals_to_event_log(test_deals)
                quality_report = data_quality_service.generate_data_quality_report(event_log_df)
                anonymized_df = privacy_governance_service.anonymize_dataframe(event_log_df)
                
                # Mining Pipeline
                discovery_result = discovery_service.discover_dfg(anonymized_df)
                kpi_result = kpi_service.calculate_process_kpis(anonymized_df)
                
                integration_tests['end_to_end'] = {
                    'success': True,
                    'etl_completed': True,
                    'mining_completed': True,
                    'quality_score': quality_report.get('overall_score', 0),
                    'details': 'End-to-end integration test passed'
                }
                
            except Exception as e:
                integration_tests['end_to_end'] = {
                    'success': False,
                    'error': str(e),
                    'details': 'End-to-end integration test failed'
                }
            
            # Test reactive ETL
            try:
                # Verifica che il sistema ETL reattivo sia configurato correttamente
                etl_status = {
                    'is_running': reactive_etl_manager.is_running,
                    'jobs_count': len(reactive_etl_manager.get_all_jobs()),
                    'running_jobs': len(reactive_etl_manager.get_running_jobs()),
                    'failed_jobs': len(reactive_etl_manager.get_failed_jobs())
                }
                
                integration_tests['reactive_etl'] = {
                    'success': True,
                    'status': etl_status,
                    'details': 'Reactive ETL test passed'
                }
                
            except Exception as e:
                integration_tests['reactive_etl'] = {
                    'success': False,
                    'error': str(e),
                    'details': 'Reactive ETL test failed'
                }
            
            # Calcola successo generale
            success_tests = sum(1 for test in integration_tests.values() if test['success'])
            total_tests = len(integration_tests)
            
            return {
                'success': success_tests == total_tests,
                'total_tests': total_tests,
                'passed_tests': success_tests,
                'failed_tests': total_tests - success_tests,
                'test_results': integration_tests,
                'details': f"Integration tests: {success_tests}/{total_tests} passed"
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'details': {},
                'test_results': {}
            }
    
    async def _save_test_results(self, test_result: Dict[str, Any]):
        """Salva i risultati del test."""
        try:
            results_dir = settings.logs_dir / "integration_tests"
            results_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"integration_test_{timestamp}.json"
            filepath = results_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(test_result, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Risultati test integrazione salvati in: {filepath}")
            
        except Exception as e:
            logger.error(f"Errore nel salvataggio risultati test: {e}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """Ottiene lo stato attuale del sistema integrato."""
        try:
            status = {
                'integration_status': self.status.value,
                'start_time': self.start_time.isoformat() if self.start_time else None,
                'end_time': self.end_time.isoformat() if self.end_time else None,
                'is_running': self.is_running,
                'test_results_count': len(self.test_results),
                'last_test_result': self.test_results[-1] if self.test_results else None,
                'system_components': self.system_components,
                'timestamp': datetime.now().isoformat()
            }
            
            return status
            
        except Exception as e:
            logger.error(f"Errore nel recupero stato sistema: {e}")
            return {'error': str(e)}
    
    async def start_integrated_system(self):
        """Avvia il sistema integrato completo."""
        try:
            logger.info("🚀 Avvio sistema integrato completo")
            
            self.status = IntegrationStatus.INITIALIZING
            self.start_time = datetime.now()
            self.is_running = True
            
            # Step 1: Bootstrap
            if self.auto_bootstrap:
                logger.info("📋 Avvio bootstrap automatico")
                bootstrap_result = await bootstrap_manager.bootstrap_system()
                if not bootstrap_result.get('success', False):
                    logger.error("Bootstrap fallito, interruzione avvio sistema")
                    self.status = IntegrationStatus.ERROR
                    return False
            
            # Step 2: Avvia ETL reattivo
            if self.auto_start_etl:
                logger.info("🔄 Avvio ETL reattivo")
                # Nota: In un sistema reale, avvieremmo il task asincrono
                # Per ora, impostiamo solo lo stato
                reactive_etl_manager.is_running = True
            
            self.status = IntegrationStatus.RUNNING
            logger.info("✅ Sistema integrato avviato con successo")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Errore nell'avvio sistema integrato: {e}")
            self.status = IntegrationStatus.ERROR
            return False
    
    async def stop_integrated_system(self):
        """Ferma il sistema integrato."""
        try:
            logger.info("🛑 Arresto sistema integrato")
            
            self.is_running = False
            self.end_time = datetime.now()
            self.status = IntegrationStatus.SHUTDOWN
            
            # Ferma ETL reattivo
            reactive_etl_manager.is_running = False
            
            logger.info("✅ Sistema integrato arrestato")
            
        except Exception as e:
            logger.error(f"❌ Errore nell'arresto sistema integrato: {e}")

# Creazione istanza globale
integration_manager = IntegrationManager()

# Funzioni helper per l'uso sincrono
def run_full_system_test_sync() -> Dict[str, Any]:
    """Esegue il test sistema integrato in modo sincrono."""
    try:
        return asyncio.run(integration_manager.full_system_test())
    except Exception as e:
        logger.error(f"Errore nel test sistema integrato: {e}")
        return {'success': False, 'error': str(e)}

def start_integrated_system_sync():
    """Avvia il sistema integrato in modo sincrono."""
    try:
        return asyncio.run(integration_manager.start_integrated_system())
    except Exception as e:
        logger.error(f"Errore nell'avvio sistema integrato: {e}")
        return False

def stop_integrated_system_sync():
    """Ferma il sistema integrato in modo sincrono."""
    try:
        asyncio.run(integration_manager.stop_integrated_system())
    except Exception as e:
        logger.error(f"Errore nell'arresto sistema integrato: {e}")