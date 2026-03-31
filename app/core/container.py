"""
Container per Dependency Injection.

Questo modulo fornisce un container per la gestione centralizzata delle dipendenze,
implementando il pattern Singleton e Dependency Injection per tutti i servizi.
"""

from typing import Dict, Any, TypeVar
from functools import lru_cache
from app.core.config import settings
from app.core.logger import get_logger
from app.core.database import get_db

logger = get_logger()

T = TypeVar('T')

class DependencyContainer:
    """Container per la gestione delle dipendenze."""
    
    _instance = None
    _dependencies: Dict[str, Any] = {}
    _initialized = False
    
    def __new__(cls):
        """Implementa pattern Singleton."""
        if cls._instance is None:
            cls._instance = super(DependencyContainer, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Inizializza il container."""
        if not self._initialized:
            self._dependencies = {}
            self._initialized = True
            logger.info("Dependency Container inizializzato")
    
    def register(self, name: str, instance: Any) -> None:
        """
        Registra una dipendenza nel container.
        
        Args:
            name: Nome della dipendenza
            instance: Istanza da registrare
        """
        self._dependencies[name] = instance
        logger.debug(f"Dipendenza registrata: {name}")
    
    def get(self, name: str) -> Any:
        """
        Ottiene una dipendenza dal container.
        
        Args:
            name: Nome della dipendenza
            
        Returns:
            Istanza della dipendenza
            
        Raises:
            KeyError: Se la dipendenza non è registrata
        """
        if name not in self._dependencies:
            raise KeyError(f"Dipendenza '{name}' non registrata")
        return self._dependencies[name]
    
    def has(self, name: str) -> bool:
        """
        Verifica se una dipendenza è registrata.
        
        Args:
            name: Nome della dipendenza
            
        Returns:
            True se la dipendenza è registrata
        """
        return name in self._dependencies
    
    def get_all(self) -> Dict[str, Any]:
        """Restituisce tutte le dipendenze registrate."""
        return self._dependencies.copy()
    
    def clear(self) -> None:
        """Cancella tutte le dipendenze."""
        self._dependencies.clear()
        logger.info("Container dipendenze svuotato")
    
    def get_or_create(self, name: str, factory: callable, *args, **kwargs) -> Any:
        """
        Ottiene una dipendenza o la crea se non esiste.
        
        Args:
            name: Nome della dipendenza
            factory: Funzione factory per creare l'istanza
            args: Argomenti posizionali per la factory
            kwargs: Argomenti keyword per la factory
            
        Returns:
            Istanza della dipendenza
        """
        if not self.has(name):
            instance = factory(*args, **kwargs)
            self.register(name, instance)
        return self.get(name)

# Istanza globale del container
container = DependencyContainer()

def get_container() -> DependencyContainer:
    """Restituisce l'istanza globale del container."""
    return container

def inject(name: str):
    """
    Decoratore per l'iniezione delle dipendenze.
    
    Args:
        name: Nome della dipendenza da iniettare
        
    Returns:
        Decoratore
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            dependency = container.get(name)
            return func(dependency, *args, **kwargs)
        return wrapper
    return decorator

def singleton(cls):
    """
    Decoratore per implementare il pattern Singleton.
    
    Args:
        cls: Classe da rendere singleton
        
    Returns:
        Classe singleton
    """
    instances = {}
    
    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    
    return get_instance

@singleton
class ServiceRegistry:
    """Registro centralizzato per tutti i servizi."""
    
    def __init__(self):
        self._services: Dict[str, Any] = {}
        self._service_factories: Dict[str, callable] = {}
        logger.info("Service Registry inizializzato")
    
    def register_service(self, name: str, factory: callable, singleton: bool = True) -> None:
        """
        Registra un servizio.
        
        Args:
            name: Nome del servizio
            factory: Funzione factory per creare il servizio
            singleton: Se il servizio deve essere singleton
        """
        self._service_factories[name] = {
            'factory': factory,
            'singleton': singleton,
            'instance': None
        }
        logger.debug(f"Servizio registrato: {name} (singleton: {singleton})")
    
    def get_service(self, name: str) -> Any:
        """
        Ottiene un servizio.
        
        Args:
            name: Nome del servizio
            
        Returns:
            Istanza del servizio
            
        Raises:
            KeyError: Se il servizio non è registrato
        """
        if name not in self._service_factories:
            raise KeyError(f"Servizio '{name}' non registrato")
        
        service_info = self._service_factories[name]
        
        if service_info['singleton'] and service_info['instance'] is None:
            service_info['instance'] = service_info['factory']()
        
        return service_info['instance'] if service_info['singleton'] else service_info['factory']()
    
    def get_all_services(self) -> Dict[str, Any]:
        """Restituisce tutti i servizi registrati."""
        return {
            name: self.get_service(name)
            for name in self._service_factories.keys()
        }

# Istanza globale del service registry
service_registry = ServiceRegistry()

def get_service_registry() -> ServiceRegistry:
    """Restituisce l'istanza globale del service registry."""
    return service_registry

# Funzioni di utilità per la registrazione dei servizi
def register_core_services():
    """Registra tutti i servizi core."""
    from app.core.logger import get_logger
    from app.core.security import security_instance
    
    # Registra servizi core
    container.register('settings', settings)
    container.register('logger', get_logger())
    container.register('security', security_instance)
    
    logger.info("Servizi core registrati")

def register_etl_services():
    """Registra tutti i servizi ETL."""
    from app.services.etl.data_extraction import data_extraction_service
    from app.services.etl.data_transformation import data_transformation_service
    from app.services.etl.data_quality import data_quality_service
    from app.services.etl.privacy_governance import privacy_governance_service
    
    # Registra servizi ETL
    container.register('data_extraction', data_extraction_service)
    container.register('data_transformation', data_transformation_service)
    container.register('data_quality', data_quality_service)
    container.register('privacy_governance', privacy_governance_service)
    
    logger.info("Servizi ETL registrati")

def register_mining_services():
    """Registra tutti i servizi Mining."""
    from app.services.mining.discovery_service import discovery_service
    from app.services.mining.conformance_service import conformance_service
    from app.services.mining.kpi_service import kpi_service
    
    # Registra servizi Mining
    container.register('discovery', discovery_service)
    container.register('conformance', conformance_service)
    container.register('kpi', kpi_service)
    
    logger.info("Servizi Mining registrati")

# TODO: Implementare il modulo app.services.integration per il sync bidirezionale
def register_integration_services():
    """Registra tutti i servizi Integration."""
    # from app.services.integration.hubspot_sync import hubspot_sync_service
    # from app.services.integration.journey_bridge import journey_bridge_service
    
    # # Registra servizi Integration
    # container.register('hubspot_sync', hubspot_sync_service)
    # container.register('journey_bridge', journey_bridge_service)
    
    # logger.info("Servizi Integration registrati")
    pass

def register_connectors():
    """Registra tutti i connettori."""
    from app.connectors.hubspot_mapper import hubspot_mapper
    
    # Registra connettori
    container.register('hubspot_mapper', hubspot_mapper)
    
    logger.info("Connettori registrati")

def initialize_container():
    """Inizializza il container con tutti i servizi."""
    try:
        register_core_services()
        register_etl_services()
        register_mining_services()
        # register_integration_services()  # TODO: Implementare il modulo app.services.integration per il sync bidirezionale
        register_connectors()
        
        logger.info("Container inizializzato con tutti i servizi")
        return True
        
    except Exception as e:
        logger.error(f"Errore nell'inizializzazione container: {e}")
        return False

# Funzioni per l'iniezione delle dipendenze
@lru_cache()
def get_settings():
    """Ottiene le impostazioni."""
    return container.get('settings')

@lru_cache()
def get_logger_instance():
    """Ottiene il logger."""
    return container.get('logger')

@lru_cache()
def get_security():
    """Ottiene il modulo sicurezza."""
    return container.get('security')

@lru_cache()
def get_data_extraction():
    """Ottiene il servizio estrazione dati."""
    return container.get('data_extraction')

@lru_cache()
def get_data_transformation():
    """Ottiene il servizio trasformazione dati."""
    return container.get('data_transformation')

@lru_cache()
def get_data_quality():
    """Ottiene il servizio qualità dati."""
    return container.get('data_quality')

@lru_cache()
def get_privacy_governance():
    """Ottiene il servizio governance privacy."""
    return container.get('privacy_governance')

@lru_cache()
def get_discovery():
    """Ottiene il servizio discovery."""
    return container.get('discovery')

@lru_cache()
def get_conformance():
    """Ottiene il servizio conformance."""
    return container.get('conformance')

@lru_cache()
def get_kpi():
    """Ottiene il servizio KPI."""
    return container.get('kpi')

# TODO: Implementare il modulo app.services.integration per il sync bidirezionale
# @lru_cache()
# def get_hubspot_sync():
#     """Ottiene il servizio sincronizzazione HubSpot."""
#     return container.get('hubspot_sync')

# TODO: Implementare il modulo app.services.integration per il sync bidirezionale
# @lru_cache()
# def get_journey_bridge():
#     """Ottiene il servizio bridge journey."""
#     return container.get('journey_bridge')

@lru_cache()
def get_hubspot_mapper():
    """Ottiene il mapper HubSpot."""
    return container.get('hubspot_mapper')

# Factory per la creazione di servizi
class ServiceFactory:
    """Factory per la creazione di servizi."""
    
    @staticmethod
    def create_hubspot_client(db_session=None):
        """Crea un client HubSpot."""
        from app.connectors.hubspot_client import HubSpotClient
        
        if db_session is None:
            db_session = next(get_db())
        
        return HubSpotClient(db_session)
    
    @staticmethod
    def create_etl_orchestrator():
        """Crea l'orchestratore ETL."""
        from app.services.etl.etl_orchestrator import ETLOrchestrator
        return ETLOrchestrator()
    
    @staticmethod
    def create_analytics_engine():
        """Crea il motore analytics."""
        from app.services.analytics.analytics_engine import AnalyticsEngine
        return AnalyticsEngine()

# Istanza globale della factory
service_factory = ServiceFactory()

def get_service_factory() -> ServiceFactory:
    """Restituisce l'istanza globale della factory."""
    return service_factory

# Inizializzazione automatica del container
if not container._initialized:
    initialize_container()