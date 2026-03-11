import requests
import asyncio
import aiohttp
from typing import Dict, List, Optional, AsyncGenerator, Union
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from app.core.logger import get_logger
from app.core.config import settings

logger = get_logger()

class HubSpotAPIError(Exception):
    """Eccezione personalizzata per errori API HubSpot."""
    pass

class RateLimitError(Exception):
    """Eccezione per errori di rate limiting."""
    pass

class HubSpotClient:
    """Client asincrono per l'API HubSpot con gestione avanzata degli errori e rate limiting."""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Inizializza il client HubSpot.
        
        Args:
            api_key: Chiave API HubSpot. Se None, cerca in settings.
        """
        self.api_key = api_key or settings.hubspot_api_key
        self.base_url = settings.hubspot_api_base_url
        self.rate_limit_delay = settings.hubspot_rate_limit_delay
        
        if not self.api_key:
            raise ValueError("HubSpot API key non trovata. Impostare HUBSPOT_API_KEY nelle variabili d'ambiente.")
        
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # Statistiche di utilizzo
        self.request_count = 0
        self.last_request_time = 0
    
    @retry(
        retry=retry_if_exception_type((requests.exceptions.RequestException, RateLimitError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        reraise=True
    )
    def _make_request(self, method: str, endpoint: str, params: Dict = None, 
                     data: Dict = None) -> Dict:
        """
        Esegue una richiesta HTTP all'API HubSpot con retry automatico.
        
        Args:
            method: Metodo HTTP (GET, POST, etc.)
            endpoint: Endpoint API
            params: Parametri query
            data: Dati per richieste POST/PUT
            
        Returns:
            Risposta JSON
        """
        import time
        
        # Rate limiting semplice
        current_time = time.time()
        if current_time - self.last_request_time < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - (current_time - self.last_request_time))
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                params=params,
                json=data,
                timeout=30
            )
            
            self.request_count += 1
            self.last_request_time = time.time()
            
            # Gestione rate limiting
            if response.status_code == 429:
                logger.warning(f"Rate limit raggiunto. Tentativo {self.request_count}")
                raise RateLimitError("HubSpot rate limit exceeded")
            
            # Gestione altri errori
            if response.status_code >= 400:
                error_msg = f"Errore API HubSpot {response.status_code}: {response.text}"
                logger.error(error_msg)
                raise HubSpotAPIError(error_msg)
            
            return response.json()
            
        except requests.exceptions.Timeout:
            logger.error("Timeout nella richiesta a HubSpot")
            raise
        except requests.exceptions.ConnectionError:
            logger.error("Errore di connessione a HubSpot")
            raise
    
    def get_deals(self, limit: int = 100, after: Optional[str] = None,
                 properties: Optional[List[str]] = None,
                 properties_with_history: Optional[List[str]] = None) -> Dict:
        """
        Recupera i deal da HubSpot.
        
        Args:
            limit: Limite di record per richiesta
            after: Token per paginazione
            properties: Proprietà da includere
            properties_with_history: Proprietà di cui includere la cronologia
            
        Returns:
            Dati dei deal
        """
        endpoint = "/crm/v3/objects/deals"
        
        params = {
            "limit": limit,
            "archived": "false"
        }
        
        if after:
            params["after"] = after
        
        if properties:
            params["properties"] = ",".join(properties)
        
        if properties_with_history:
            params["propertiesWithHistory"] = ",".join(properties_with_history)
        
        logger.info(f"Recupero deal da HubSpot (limit={limit})")
        return self._make_request("GET", endpoint, params=params)
    
    def get_deal_history(self, deal_id: str, property_name: str = "dealstage") -> List[Dict]:
        """
        Recupera la cronologia di una proprietà specifica per un deal.
        
        Args:
            deal_id: ID del deal
            property_name: Nome della proprietà (default: dealstage)
            
        Returns:
            Lista di record di cronologia
        """
        endpoint = f"/crm/v3/objects/deals/{deal_id}/associations/properties/{property_name}"
        
        logger.info(f"Recupero cronologia per deal {deal_id}, proprietà {property_name}")
        return self._make_request("GET", endpoint)
    
    def get_all_deals_with_history(self, properties_with_history: Optional[List[str]] = None) -> List[Dict]:
        """
        Recupera tutti i deal con la cronologia delle proprietà specificate.
        
        Args:
            properties_with_history: Proprietà di cui includere la cronologia
            
        Returns:
            Lista di deal con cronologia
        """
        if properties_with_history is None:
            properties_with_history = ["dealstage"]
        
        all_deals = []
        after = None
        
        while True:
            response = self.get_deals(
                limit=100,
                after=after,
                properties_with_history=properties_with_history
            )
            
            deals = response.get("results", [])
            if not deals:
                break
            
            all_deals.extend(deals)
            
            # Paginazione
            paging = response.get("paging")
            if paging and "next" in paging:
                after = paging["next"]["after"]
            else:
                break
        
        logger.info(f"Recuperati {len(all_deals)} deal con cronologia")
        return all_deals
    
    def get_contacts(self, limit: int = 100, after: Optional[str] = None) -> Dict:
        """Recupera i contatti da HubSpot."""
        endpoint = "/crm/v3/objects/contacts"
        
        params = {
            "limit": limit,
            "archived": "false"
        }
        
        if after:
            params["after"] = after
        
        logger.info(f"Recupero contatti da HubSpot (limit={limit})")
        return self._make_request("GET", endpoint, params=params)
    
    def get_companies(self, limit: int = 100, after: Optional[str] = None) -> Dict:
        """Recupera le aziende da HubSpot."""
        endpoint = "/crm/v3/objects/companies"
        
        params = {
            "limit": limit,
            "archived": "false"
        }
        
        if after:
            params["after"] = after
        
        logger.info(f"Recupero aziende da HubSpot (limit={limit})")
        return self._make_request("GET", endpoint, params=params)
    
    def get_pipeline_stages(self) -> List[Dict]:
        """Recupera le fasi delle pipeline da HubSpot."""
        endpoint = "/crm/v3/pipelines/deals"
        
        logger.info("Recupero pipeline stages da HubSpot")
        response = self._make_request("GET", endpoint)
        
        stages = []
        for pipeline in response.get("results", []):
            stages.extend(pipeline.get("stages", []))
        
        return stages
    
    def get_usage_stats(self) -> Dict:
        """Restituisce statistiche sull'uso del client."""
        return {
            "request_count": self.request_count,
            "rate_limit_delay": self.rate_limit_delay,
            "base_url": self.base_url
        }

# Creazione istanza globale (opzionale)
hubspot_client = HubSpotClient()