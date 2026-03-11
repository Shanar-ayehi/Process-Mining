from typing import Dict, List, Any, Optional, Union
from celery import chain, group
from app.tasks.base_task import (
    celery_app, integration_task, create_task_metadata, create_task_result, 
    validate_task_input, handle_task_error
)
from app.services.integration.hubspot_sync import hubspot_sync_service
from app.services.integration.journey_bridge import journey_bridge_service
from app.core.logger import get_logger

logger = get_logger()

@integration_task()
def sync_kpis_to_hubspot_task(self, kpis_data: Dict[str, Any], 
                            deal_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Task per la sincronizzazione KPI verso HubSpot.
    
    Args:
        kpis_data: Dati KPI da sincronizzare
        deal_ids: ID deal specifici (opzionale, altrimenti tutti)
        
    Returns:
        Dizionario con risultati sincronizzazione
    """
    try:
        logger.info("Inizio task sincronizzazione KPI verso HubSpot")
        
        sync_result = hubspot_sync_service.sync_kpis_to_hubspot(
            kpis_data, deal_ids
        )
        
        result = create_task_result(
            success=True,
            data={
                'synced_deals': sync_result['synced_count'],
                'failed_deals': sync_result['failed_count'],
                'sync_result': sync_result,
                'metadata': create_task_metadata('sync_kpis_to_hubspot', 
                                               synced_count=sync_result['synced_count'])
            }
        )
        
        logger.info(f"Task sincronizzazione KPI completato: {sync_result['synced_count']} deal sincronizzati")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task sincronizzazione KPI: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@integration_task()
def sync_process_scores_task(self, process_scores: Dict[str, float],
                           deal_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Task per la sincronizzazione punteggi processo verso HubSpot.
    
    Args:
        process_scores: Punteggi processo per deal
        deal_ids: ID deal specifici (opzionale)
        
    Returns:
        Dizionario con risultati sincronizzazione
    """
    try:
        logger.info("Inizio task sincronizzazione punteggi processo")
        
        sync_result = hubspot_sync_service.sync_process_scores_to_hubspot(
            process_scores, deal_ids
        )
        
        result = create_task_result(
            success=True,
            data={
                'synced_scores': sync_result['synced_count'],
                'failed_scores': sync_result['failed_count'],
                'sync_result': sync_result,
                'metadata': create_task_metadata('sync_process_scores', 
                                               synced_count=sync_result['synced_count'])
            }
        )
        
        logger.info(f"Task sincronizzazione punteggi completato: {sync_result['synced_count']} punteggi sincronizzati")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task sincronizzazione punteggi: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@integration_task()
def sync_deviation_alerts_task(self, deviation_data: Dict[str, Any],
                             deal_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Task per la sincronizzazione alert deviazione verso HubSpot.
    
    Args:
        deviation_data: Dati deviazione per deal
        deal_ids: ID deal specifici (opzionale)
        
    Returns:
        Dizionario con risultati sincronizzazione
    """
    try:
        logger.info("Inizio task sincronizzazione alert deviazione")
        
        sync_result = hubspot_sync_service.sync_deviation_alerts_to_hubspot(
            deviation_data, deal_ids
        )
        
        result = create_task_result(
            success=True,
            data={
                'synced_alerts': sync_result['synced_count'],
                'failed_alerts': sync_result['failed_count'],
                'sync_result': sync_result,
                'metadata': create_task_metadata('sync_deviation_alerts', 
                                               synced_count=sync_result['synced_count'])
            }
        )
        
        logger.info(f"Task sincronizzazione alert completato: {sync_result['synced_count']} alert sincronizzati")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task sincronizzazione alert: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@integration_task()
def create_hubspot_workflows_task(self, workflow_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Task per la creazione workflow HubSpot basati su analisi mining.
    
    Args:
        workflow_config: Configurazione workflow
        
    Returns:
        Dizionario con risultati creazione workflow
    """
    try:
        logger.info("Inizio task creazione workflow HubSpot")
        
        workflow_result = hubspot_sync_service.create_mining_workflows(
            workflow_config
        )
        
        result = create_task_result(
            success=True,
            data={
                'created_workflows': workflow_result['created_count'],
                'failed_workflows': workflow_result['failed_count'],
                'workflow_result': workflow_result,
                'metadata': create_task_metadata('create_hubspot_workflows', 
                                               created_count=workflow_result['created_count'])
            }
        )
        
        logger.info(f"Task creazione workflow completato: {workflow_result['created_count']} workflow creati")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task creazione workflow: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@integration_task()
def sync_journey_report_task(self, journey_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Task per la sincronizzazione report journey verso HubSpot.
    
    Args:
        journey_config: Configurazione report journey
        
    Returns:
        Dizionario con risultati sincronizzazione
    """
    try:
        logger.info("Inizio task sincronizzazione report journey")
        
        journey_result = journey_bridge_service.sync_journey_report_to_hubspot(
            journey_config
        )
        
        result = create_task_result(
            success=True,
            data={
                'journey_synced': journey_result['synced'],
                'journey_id': journey_result['journey_id'],
                'journey_result': journey_result,
                'metadata': create_task_metadata('sync_journey_report', 
                                               synced=journey_result['synced'])
            }
        )
        
        logger.info(f"Task sincronizzazione journey completato: {journey_result['journey_id']}")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task sincronizzazione journey: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@integration_task()
def bridge_mining_to_hubspot_task(self, mining_results: Dict[str, Any],
                                sync_options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Task orchestratore per il bridge mining verso HubSpot.
    
    Args:
        mining_results: Risultati analisi mining
        sync_options: Opzioni di sincronizzazione (opzionale)
        
    Returns:
        Dizionario con risultati bridge
    """
    try:
        logger.info("Inizio task bridge mining verso HubSpot")
        
        if sync_options is None:
            sync_options = {
                'sync_kpis': True,
                'sync_scores': True,
                'sync_alerts': True,
                'create_workflows': True,
                'sync_journey': True
            }
        
        bridge_results = {}
        
        # Sincronizza KPI
        if sync_options.get('sync_kpis', False):
            try:
                kpis_data = mining_results.get('kpis_results', {})
                kpis_result = hubspot_sync_service.sync_kpis_to_hubspot(kpis_data)
                bridge_results['kpis'] = kpis_result
            except Exception as e:
                logger.warning(f"Errore sincronizzazione KPI: {e}")
                bridge_results['kpis'] = {'error': str(e)}
        
        # Sincronizza punteggi
        if sync_options.get('sync_scores', False):
            try:
                process_scores = mining_results.get('process_scores', {})
                scores_result = hubspot_sync_service.sync_process_scores_to_hubspot(process_scores)
                bridge_results['scores'] = scores_result
            except Exception as e:
                logger.warning(f"Errore sincronizzazione punteggi: {e}")
                bridge_results['scores'] = {'error': str(e)}
        
        # Sincronizza alert
        if sync_options.get('sync_alerts', False):
            try:
                deviation_data = mining_results.get('deviation_data', {})
                alerts_result = hubspot_sync_service.sync_deviation_alerts_to_hubspot(deviation_data)
                bridge_results['alerts'] = alerts_result
            except Exception as e:
                logger.warning(f"Errore sincronizzazione alert: {e}")
                bridge_results['alerts'] = {'error': str(e)}
        
        # Crea workflow
        if sync_options.get('create_workflows', False):
            try:
                workflow_config = mining_results.get('workflow_config', {})
                workflows_result = hubspot_sync_service.create_mining_workflows(workflow_config)
                bridge_results['workflows'] = workflows_result
            except Exception as e:
                logger.warning(f"Errore creazione workflow: {e}")
                bridge_results['workflows'] = {'error': str(e)}
        
        # Sincronizza journey
        if sync_options.get('sync_journey', False):
            try:
                journey_config = mining_results.get('journey_config', {})
                journey_result = journey_bridge_service.sync_journey_report_to_hubspot(journey_config)
                bridge_results['journey'] = journey_result
            except Exception as e:
                logger.warning(f"Errore sincronizzazione journey: {e}")
                bridge_results['journey'] = {'error': str(e)}
        
        result = create_task_result(
            success=True,
            data={
                'bridge_results': bridge_results,
                'sync_options': sync_options,
                'metadata': create_task_metadata('bridge_mining_to_hubspot', **sync_options)
            }
        )
        
        logger.info("Task bridge mining verso HubSpot completato")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task bridge mining: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@integration_task()
def reverse_etl_task(self, data_to_sync: Dict[str, Any],
                   target_system: str = 'hubspot') -> Dict[str, Any]:
    """
    Task per il reverse ETL verso sistemi esterni.
    
    Args:
        data_to_sync: Dati da sincronizzare
        target_system: Sistema target ('hubspot', 'warehouse', etc.)
        
    Returns:
        Dizionario con risultati reverse ETL
    """
    try:
        logger.info(f"Inizio task reverse ETL verso {target_system}")
        
        if target_system == 'hubspot':
            sync_result = hubspot_sync_service.sync_reverse_etl_to_hubspot(data_to_sync)
        else:
            # Placeholder per altri sistemi
            sync_result = {
                'synced_count': 0,
                'failed_count': 0,
                'error': f"Sistema {target_system} non implementato"
            }
        
        result = create_task_result(
            success=True,
            data={
                'target_system': target_system,
                'sync_result': sync_result,
                'metadata': create_task_metadata('reverse_etl', target_system=target_system)
            }
        )
        
        logger.info(f"Task reverse ETL completato verso {target_system}")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task reverse ETL verso {target_system}: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@integration_task()
def export_to_warehouse_task(self, data_to_export: Dict[str, Any],
                           warehouse_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Task per l'esportazione dati verso data warehouse.
    
    Args:
        data_to_export: Dati da esportare
        warehouse_config: Configurazione warehouse (opzionale)
        
    Returns:
        Dizionario con risultati esportazione
    """
    try:
        logger.info("Inizio task esportazione verso data warehouse")
        
        # Placeholder - in produzione si implementerebbe
        # la connessione a Snowflake, BigQuery, etc.
        
        export_result = {
            'exported_tables': [],
            'exported_rows': 0,
            'export_status': 'completed',
            'warehouse_config': warehouse_config or {}
        }
        
        result = create_task_result(
            success=True,
            data={
                'export_result': export_result,
                'metadata': create_task_metadata('export_to_warehouse')
            }
        )
        
        logger.info("Task esportazione warehouse completato")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task esportazione warehouse: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

@integration_task()
def create_integration_report_task(self, integration_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Task per la creazione report integrazione.
    
    Args:
        integration_results: Risultati integrazione
        
    Returns:
        Dizionario con risultati report
    """
    try:
        logger.info("Inizio task creazione report integrazione")
        
        # Analizza risultati integrazione
        integration_summary = {
            'systems_integrated': list(integration_results.keys()),
            'total_syncs': sum(result.get('synced_count', 0) for result in integration_results.values()),
            'total_failures': sum(result.get('failed_count', 0) for result in integration_results.values()),
            'integration_timestamp': create_task_metadata('integration_report')['timestamp']
        }
        
        report = {
            'integration_summary': integration_summary,
            'detailed_results': integration_results,
            'recommendations': self._generate_integration_recommendations(integration_results)
        }
        
        result = create_task_result(
            success=True,
            data={
                'integration_report': report,
                'summary': integration_summary,
                'metadata': create_task_metadata('create_integration_report')
            }
        )
        
        logger.info("Task creazione report integrazione completato")
        return result
        
    except Exception as e:
        logger.error(f"Errore nel task creazione report integrazione: {e}")
        handle_task_error(self.request.id, e)
        return create_task_result(success=False, error=str(e))

def _generate_integration_recommendations(self, integration_results: Dict[str, Any]) -> List[str]:
    """Genera raccomandazioni basate sui risultati integrazione."""
    recommendations = []
    
    total_syncs = sum(result.get('synced_count', 0) for result in integration_results.values())
    total_failures = sum(result.get('failed_count', 0) for result in integration_results.values())
    
    if total_failures > 0:
        failure_rate = total_failures / (total_syncs + total_failures)
        if failure_rate > 0.1:
            recommendations.append("Alto tasso di fallimento integrazione: revisione connettori necessaria")
    
    if 'kpis' in integration_results and integration_results['kpis'].get('synced_count', 0) == 0:
        recommendations.append("Nessun KPI sincronizzato: verificare mappatura proprietà HubSpot")
    
    if 'alerts' in integration_results and integration_results['alerts'].get('synced_count', 0) > 0:
        recommendations.append("Alert deviazione attivi: monitorare workflow HubSpot")
    
    return recommendations

# Task helper per la gestione delle integrazioni
@integration_task()
def get_integration_status(self, integration_id: str) -> Dict[str, Any]:
    """
    Ottiene lo stato di un'integrazione.
    
    Args:
        integration_id: ID dell'integrazione
        
    Returns:
        Dizionario con stato integrazione
    """
    try:
        from celery.result import AsyncResult
        
        result = AsyncResult(integration_id)
        
        status_data = {
            'integration_id': integration_id,
            'status': result.status,
            'ready': result.ready(),
            'successful': result.successful() if result.ready() else None,
            'result': result.result if result.ready() else None
        }
        
        logger.info(f"Stato integrazione {integration_id}: {status_data}")
        return create_task_result(success=True, data=status_data)
        
    except Exception as e:
        logger.error(f"Errore nel recupero stato integrazione {integration_id}: {e}")
        return create_task_result(success=False, error=str(e))

@integration_task()
def cancel_integration_task(self, integration_id: str) -> Dict[str, Any]:
    """
    Cancella un'integrazione in esecuzione.
    
    Args:
        integration_id: ID dell'integrazione da cancellare
        
    Returns:
        Dizionario con risultati cancellazione
    """
    try:
        from celery.result import AsyncResult
        
        result = AsyncResult(integration_id)
        result.revoke(terminate=True)
        
        logger.info(f"Integrazione {integration_id} cancellata")
        return create_task_result(success=True, data={'integration_id': integration_id, 'cancelled': True})
        
    except Exception as e:
        logger.error(f"Errore nella cancellazione integrazione {integration_id}: {e}")
        return create_task_result(success=False, error=str(e))

# Creazione istanza globale
integration_task_instance = integration_task()