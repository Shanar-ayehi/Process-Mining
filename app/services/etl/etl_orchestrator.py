"""
Orchestratore ETL per Process Mining.

Questo modulo unifica tutti i servizi ETL in un'unica interfaccia coordinata,
implementando l'architettura plastica e l'auto-adattività.
"""

from typing import Dict, List, Any, Optional, Union
import polars as pl
from datetime import datetime
from pathlib import Path
from app.core.logger import get_logger
from app.core.config import settings
from app.core.container import get_container
from app.services.etl.data_extraction import DataExtractionService
from app.services.etl.data_transformation import DataTransformationService
from app.services.etl.data_quality import DataQualityService
from app.services.etl.privacy_governance import PrivacyGovernanceService
from app.connectors.hubspot_client import HubSpotClient

logger = get_logger()

class ETLOrchestrator:
    """Orchestratore centrale per tutti i servizi ETL."""
    
    def __init__(self, db_session=None):
        """
        Inizializza l'orchestratore ETL.
        
        Args:
            db_session: Sessione database per OAuth (opzionale)
        """
        self.db_session = db_session
        self.container = get_container()
        
        # Inizializza servizi
        self._init_services()
        
        # Stato orchestratore
        self.etl_state = {
            'last_extraction': None,
            'last_transformation': None,
            'last_validation': None,
            'last_privacy_check': None,
            'total_records_processed': 0,
            'errors': []
        }
        
        logger.info("ETL Orchestrator inizializzato")
    
    def _init_services(self):
        """Inizializza tutti i servizi ETL."""
        try:
            # Servizi core
            self.data_extraction = self.container.get('data_extraction') if self.container.has('data_extraction') else None
            self.data_transformation = self.container.get('data_transformation') if self.container.has('data_transformation') else None
            self.data_quality = self.container.get('data_quality') if self.container.has('data_quality') else None
            self.privacy_governance = self.container.get('privacy_governance') if self.container.has('privacy_governance') else None
            
            logger.info("Servizi ETL inizializzati")
            
        except Exception as e:
            logger.error(f"Errore nell'inizializzazione servizi ETL: {e}")
            self.etl_state['errors'].append(str(e))
    
    def run_full_etl_pipeline(self, 
                                   portal_id: str,
                                   source_type: str = 'hubspot',
                                   properties_with_history: Optional[List[str]] = None,
                                   include_contacts: bool = False,
                                   include_companies: bool = False,
                                   save_intermediate: bool = True) -> Dict[str, Any]:
        """
        Esegue l'intera pipeline ETL.
        
        Args:
            portal_id: ID del portale HubSpot (obbligatorio per multi-tenancy)
            source_type: Tipo di sorgente ('hubspot', 'file', 'api')
            properties_with_history: Proprietà da estrarre con cronologia
            include_contacts: Se includere l'estrazione contatti
            include_companies: Se includere l'estrazione aziende
            save_intermediate: Se salvare dati intermedi
            
        Returns:
            Dizionario con risultati pipeline
        """
        logger.info(f"Inizio pipeline ETL completa - portal_id: {portal_id}, sorgente: {source_type}")
        
        pipeline_results = {
            'pipeline_id': self._generate_pipeline_id(),
            'portal_id': portal_id,
            'source_type': source_type,
            'start_time': datetime.now().isoformat(),
            'steps': {},
            'success': False,
            'final_dataframe': None,
            'quality_report': None,
            'errors': []
        }
        
        try:
            # Step 1: Estrazione
            extraction_result = self._run_extraction_step(
                portal_id, source_type, properties_with_history, include_contacts, include_companies, save_intermediate
            )
            pipeline_results['steps']['extraction'] = extraction_result
            
            if not extraction_result['success']:
                raise Exception(f"Estrazione fallita: {extraction_result.get('error', 'Errore sconosciuto')}")
            
            # Step 2: Trasformazione
            transformation_result = self._run_transformation_step(
                portal_id, extraction_result['data'], save_intermediate
            )
            pipeline_results['steps']['transformation'] = transformation_result
            
            if not transformation_result['success']:
                raise Exception(f"Trasformazione fallita: {transformation_result.get('error', 'Errore sconosciuto')}")
            
            # Step 3: Validazione qualità
            quality_result = self._run_quality_validation_step(
                transformation_result['dataframe']
            )
            pipeline_results['steps']['quality_validation'] = quality_result
            
            # Step 4: Governance privacy
            privacy_result = self._run_privacy_governance_step(
                transformation_result['dataframe']
            )
            pipeline_results['steps']['privacy_governance'] = privacy_result
            
            # Step 5: Fusione sorgenti multiple (se necessario)
            if include_contacts or include_companies:
                merge_result = self._run_merge_step(
                    transformation_result['dataframe'],
                    extraction_result['data'].get('contacts'),
                    extraction_result['data'].get('companies')
                )
                pipeline_results['steps']['merge'] = merge_result
                
                if merge_result['success']:
                    final_df = merge_result['dataframe']
                else:
                    final_df = transformation_result['dataframe']
            else:
                final_df = transformation_result['dataframe']
            
            # Aggiorna stato
            self.etl_state['last_extraction'] = extraction_result['timestamp']
            self.etl_state['last_transformation'] = transformation_result['timestamp']
            self.etl_state['last_validation'] = quality_result['timestamp']
            self.etl_state['last_privacy_check'] = privacy_result['timestamp']
            self.etl_state['total_records_processed'] += len(final_df)
            
            # Risultati finali
            pipeline_results['success'] = True
            pipeline_results['end_time'] = datetime.now().isoformat()
            pipeline_results['final_dataframe'] = final_df
            pipeline_results['quality_report'] = quality_result.get('report')
            pipeline_results['total_records'] = len(final_df)
            
            logger.info(f"Pipeline ETL completata con successo: {len(final_df)} record")
            
        except Exception as e:
            logger.error(f"Errore nella pipeline ETL: {e}")
            pipeline_results['success'] = False
            pipeline_results['error'] = str(e)
            pipeline_results['end_time'] = datetime.now().isoformat()
            self.etl_state['errors'].append(str(e))
        
        return pipeline_results
    
    def _run_extraction_step(self, 
                                  portal_id: str,
                                  source_type: str,
                                  properties_with_history: Optional[List[str]],
                                  include_contacts: bool,
                                  include_companies: bool,
                                  save_intermediate: bool) -> Dict[str, Any]:
        """Esegue il step di estrazione."""
        logger.info(f"Step estrazione: {source_type} per portal_id: {portal_id}")
        
        try:
            if source_type == 'hubspot':
                if not self.data_extraction:
                    raise Exception("Servizio estrazione non disponibile")
                
                # Estrai deal con cronologia
                deals_data = self.data_extraction.extract_deals_with_history(
                    portal_id=portal_id,
                    properties_with_history=properties_with_history,
                    save_to_file=save_intermediate
                )
                
                result = {
                    'success': True,
                    'data': {'deals': deals_data},
                    'timestamp': datetime.now().isoformat(),
                    'records_extracted': len(deals_data)
                }
                
                # Estrai contatti se richiesto
                if include_contacts:
                    contacts_data = self.data_extraction.extract_contacts(
                        portal_id=portal_id,
                        save_to_file=save_intermediate
                    )
                    result['data']['contacts'] = contacts_data
                    result['records_extracted'] += len(contacts_data)
                
                # Estrai aziende se richiesto
                if include_companies:
                    companies_data = self.data_extraction.extract_companies(
                        portal_id=portal_id,
                        save_to_file=save_intermediate
                    )
                    result['data']['companies'] = companies_data
                    result['records_extracted'] += len(companies_data)
                
            elif source_type == 'file':
                # Placeholder per estrazione da file
                result = {
                    'success': True,
                    'data': {'deals': []},
                    'timestamp': datetime.now().isoformat(),
                    'records_extracted': 0
                }
            else:
                raise Exception(f"Tipo sorgente non supportato: {source_type}")
            
            logger.info(f"Estrazione completata: {result['records_extracted']} record")
            return result
            
        except Exception as e:
            logger.error(f"Errore nell'estrazione: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _run_transformation_step(self, 
                                      portal_id: str,
                                      extraction_data: Dict[str, Any],
                                      save_intermediate: bool) -> Dict[str, Any]:
        """Esegue il step di trasformazione."""
        logger.info(f"Step trasformazione per portal_id: {portal_id}")
        
        try:
            if not self.data_transformation:
                raise Exception("Servizio trasformazione non disponibile")
            
            # Trasforma deal in event log
            deals_data = extraction_data.get('deals', [])
            if not deals_data:
                raise Exception("Nessun dato deal da trasformare")
            
            event_log_df = self.data_transformation.transform_hubspot_deals_to_event_log(deals_data)
            
            if event_log_df.is_empty():
                raise Exception("Nessun evento generato dalla trasformazione")
            
            # Salva dati processati se richiesto
            if save_intermediate:
                self.data_transformation._save_processed_data(event_log_df, portal_id)
            
            result = {
                'success': True,
                'dataframe': event_log_df,
                'timestamp': datetime.now().isoformat(),
                'records_transformed': len(event_log_df)
            }
            
            logger.info(f"Trasformazione completata: {len(event_log_df)} eventi")
            return result
            
        except Exception as e:
            logger.error(f"Errore nella trasformazione: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _run_quality_validation_step(self, dataframe: pl.DataFrame) -> Dict[str, Any]:
        """Esegue il step di validazione qualità."""
        logger.info("Step validazione qualità")
        
        try:
            if not self.data_quality:
                raise Exception("Servizio qualità non disponibile")
            
            # Genera report qualità completo
            quality_report = self.data_quality.generate_data_quality_report(dataframe)
            
            result = {
                'success': True,
                'report': quality_report,
                'timestamp': datetime.now().isoformat(),
                'overall_score': quality_report.get('overall_score', 0)
            }
            
            logger.info(f"Validazione completata - punteggio: {quality_report.get('overall_score', 0):.2f}")
            return result
            
        except Exception as e:
            logger.error(f"Errore nella validazione: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _run_privacy_governance_step(self, dataframe: pl.DataFrame) -> Dict[str, Any]:
        """Esegue il step di governance privacy."""
        logger.info("Step governance privacy")
        
        try:
            if not self.privacy_governance:
                raise Exception("Servizio governance privacy non disponibile")
            
            # Valida compliance GDPR
            gdpr_report = self.privacy_governance.validate_gdpr_compliance(dataframe)
            
            # Applica pseudonimizzazione se necessario
            anonymized_df = self.privacy_governance.anonymize_dataframe(dataframe)
            
            result = {
                'success': True,
                'gdpr_report': gdpr_report,
                'anonymized_dataframe': anonymized_df,
                'timestamp': datetime.now().isoformat(),
                'compliance_score': gdpr_report.get('compliance_score', 0)
            }
            
            logger.info(f"Governance privacy completata - compliance: {gdpr_report.get('compliance_score', 0):.2f}")
            return result
            
        except Exception as e:
            logger.error(f"Errore nella governance privacy: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _run_merge_step(self, 
                             event_log_df: pl.DataFrame,
                             contacts_data: Optional[List[Dict[str, Any]]],
                             companies_data: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Esegue il step di fusione sorgenti multiple."""
        logger.info("Step fusione sorgenti multiple")
        
        try:
            if not self.data_transformation:
                raise Exception("Servizio trasformazione non disponibile")
            
            # Trasforma entità
            contacts_entities = None
            companies_entities = None
            
            if contacts_data:
                contacts_entities = self.data_transformation.transform_contacts_to_entities(contacts_data)
            
            if companies_data:
                companies_entities = self.data_transformation.transform_companies_to_entities(companies_data)
            
            # Fonde i dati
            merged_df = self.data_transformation.merge_multiple_sources(
                event_log_df, contacts_entities, companies_entities
            )
            
            result = {
                'success': True,
                'dataframe': merged_df,
                'timestamp': datetime.now().isoformat(),
                'records_merged': len(merged_df)
            }
            
            logger.info(f"Fusione completata: {len(merged_df)} record")
            return result
            
        except Exception as e:
            logger.error(f"Errore nella fusione: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _generate_pipeline_id(self) -> str:
        """Genera un ID univoco per la pipeline."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"etl_pipeline_{timestamp}"
    
    def get_etl_status(self) -> Dict[str, Any]:
        """Restituisce lo stato corrente dell'ETL."""
        return {
            'orchestrator_status': 'active',
            'etl_state': self.etl_state,
            'services_available': {
                'data_extraction': self.data_extraction is not None,
                'data_transformation': self.data_transformation is not None,
                'data_quality': self.data_quality is not None,
                'privacy_governance': self.privacy_governance is not None
            },
            'timestamp': datetime.now().isoformat()
        }
    
    def cleanup_old_data(self, retention_days: int = 30) -> Dict[str, Any]:
        """
        Pulisce i dati vecchi secondo le policy di retention.
        
        Args:
            retention_days: Giorni di retention per i dati
            
        Returns:
            Dizionario con risultati pulizia
        """
        logger.info(f"Pulizia dati con retention {retention_days} giorni")
        
        try:
            if self.privacy_governance:
                stats = self.privacy_governance.apply_data_retention_policy()
                
                return {
                    'success': True,
                    'stats': stats,
                    'timestamp': datetime.now().isoformat()
                }
            else:
                return {
                    'success': False,
                    'error': 'Servizio governance privacy non disponibile',
                    'timestamp': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Errore nella pulizia dati: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

# Factory per la creazione dell'orchestratore
class ETLOrchestratorFactory:
    """Factory per la creazione dell'orchestratore ETL."""
    
    @staticmethod
    def create(db_session=None) -> ETLOrchestrator:
        """Crea un'istanza dell'orchestratore ETL."""
        return ETLOrchestrator(db_session)

# Istanza globale (singleton)
_etl_orchestrator_instance = None

def get_etl_orchestrator(db_session=None) -> ETLOrchestrator:
    """
    Ottiene l'istanza singleton dell'orchestratore ETL.
    
    Args:
        db_session: Sessione database per OAuth (opzionale)
        
    Returns:
        Istanza dell'orchestratore ETL
    """
    global _etl_orchestrator_instance
    
    if _etl_orchestrator_instance is None:
        _etl_orchestrator_instance = ETLOrchestratorFactory.create(db_session)
    
    return _etl_orchestrator_instance

# Creazione istanza globale
etl_orchestrator = get_etl_orchestrator()