"""
Client HubSpot asincrono con OAuth 2.0

Client per l'API HubSpot che utilizza OAuth 2.0 per l'autenticazione
e supporta tutte le operazioni necessarie per il Process Mining.
"""

import httpx
import asyncio
from typing import Dict, List, Optional, AsyncGenerator, Union
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import logging

from app.core.database import get_db
from app.core.logger import get_logger
from app.core.config import settings
from app.models.auth import Token

logger = get_logger()


class HubSpotOAuthClient:
    """Client asincrono per l'API HubSpot con OAuth 2.0."""
    
    def __init__(self, db: AsyncSession):
        """
        Inizializza il client HubSpot OAuth.
        
        Args:
            db: Sessione database per gestione token
        """
        self.db = db
        self.base_url = "https://api.hubapi.com"
        self.access_token = None
        self.refresh_token = None
        self.token_expires_at = None
        
        # Rate limiting
        self.rate_limit_delay = 0.1  # 100ms tra le richieste
        self.last_request_time = 0
    
    async def _ensure_valid_token(self) -> bool:
        """
        Assicura che il token sia valido, rinnovandolo se necessario.
        
        Returns:
            bool: True se token valido, False altrimenti
        """
        # Ottieni token dal database
        result = await self.db.execute(
            select(Token).where(Token.is_active == True)
        )
        token_record = result.scalar_one_or_none()
        
        if not token_record:
            logger.error("Nessun token valido trovato nel database")
            return False
        
        # Controlla se token è scaduto
        if token_record.is_expired():
            logger.info("Token scaduto, tentativo di refresh")
            if not await self._refresh_token(token_record):
                return False
        
        # Aggiorna token interno
        self.access_token = token_record.access_token
        self.refresh_token = token_record.refresh_token
        self.token_expires_at = token_record.expires_at
        
        return True
    
    async def _refresh_token(self, token_record: Token) -> bool:
        """
        Rinnova il token di accesso.
        
        Args:
            token_record: Record token dal database
            
        Returns:
            bool: True se refresh riuscito
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    "https://api.hubapi.com/oauth/v1/token",
                    data={
                        "grant_type": "refresh_token",
                        "client_id": settings.hubspot_client_id,
                        "client_secret": settings.hubspot_client_secret,
                        "refresh_token": token_record.refresh_token
                    },
                    headers={"Content-Type": "application/x-www-form-urlencoded"}
                )
                
                if response.status_code == 200:
                    token_data = response.json()
                    
                    # Aggiorna record nel database
                    token_record.access_token = token_data["access_token"]
                    token_record.refresh_token = token_data["refresh_token"]
                    token_record.expires_at = datetime.utcnow() + timedelta(seconds=token_data["expires_in"])
                    token_record.updated_at = datetime.utcnow()
                    
                    await self.db.commit()
                    
                    logger.info("Token refresh riuscito")
                    return True
                else:
                    logger.error(f"Token refresh fallito: {response.status_code} - {response.text}")
                    return False
                    
        except Exception as e:
            logger.error(f"Errore durante refresh token: {str(e)}")
            return False
    
    async def _make_request(self, method: str, endpoint: str, 
                           params: Dict = None, data: Dict = None) -> Dict:
        """
        Esegue una richiesta HTTP all'API HubSpot.
        
        Args:
            method: Metodo HTTP
            endpoint: Endpoint API
            params: Parametri query
            data: Dati per richieste POST/PUT
            
        Returns:
            Risposta JSON
        """
        # Assicura token valido
        if not await self._ensure_valid_token():
            raise Exception("Impossibile ottenere token valido")
        
        # Rate limiting
        import time
        current_time = time.time()
        if current_time - self.last_request_time < self.rate_limit_delay:
            await asyncio.sleep(self.rate_limit_delay - (current_time - self.last_request_time))
        
        url = f"{self.base_url}{endpoint}"
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    json=data,
                    timeout=30
                )
                
                self.last_request_time = time.time()
                
                # Gestione errori
                if response.status_code >= 400:
                    error_msg = f"Errore API HubSpot {response.status_code}: {response.text}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                
                return response.json()
                
        except Exception as e:
            logger.error(f"Errore nella richiesta a HubSpot: {str(e)}")
            raise
    
    async def get_deals(self, limit: int = 100, after: Optional[str] = None,
                       properties: Optional[List[str]] = None) -> List[Dict]:
        """
        Recupera i deal da HubSpot.
        
        Args:
            limit: Limite di record per richiesta
            after: Token per paginazione
            properties: Proprietà da includere
            
        Returns:
            Lista di deal
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
        
        logger.info(f"Recupero {limit} deal da HubSpot")
        response = await self._make_request("GET", endpoint, params=params)
        
        return response.get("results", [])
    
    async def get_all_deals(self, properties: Optional[List[str]] = None) -> List[Dict]:
        """
        Recupera tutti i deal con paginazione automatica.
        
        Args:
            properties: Proprietà da includere
            
        Returns:
            Lista completa di deal
        """
        all_deals = []
        after = None
        
        while True:
            deals = await self.get_deals(limit=100, after=after, properties=properties)
            
            if not deals:
                break
            
            all_deals.extend(deals)
            
            # Controlla se ci sono altri risultati
            if len(deals) < 100:
                break
            
            # Prepara prossima pagina
            after = deals[-1].get("id")
        
        logger.info(f"Recuperati {len(all_deals)} deal totali")
        return all_deals
    
    async def get_deal_history(self, deal_id: str, property_name: str = "dealstage") -> List[Dict]:
        """
        Recupera la cronologia di una proprietà per un deal.
        
        Args:
            deal_id: ID del deal
            property_name: Nome della proprietà
            
        Returns:
            Lista di record di cronologia
        """
        endpoint = f"/crm/v3/objects/deals/{deal_id}/associations/properties/{property_name}"
        
        logger.info(f"Recupero cronologia per deal {deal_id}")
        response = await self._make_request("GET", endpoint)
        
        return response.get("results", [])
    
    async def get_contacts(self, limit: int = 100, after: Optional[str] = None) -> List[Dict]:
        """Recupera i contatti da HubSpot."""
        endpoint = "/crm/v3/objects/contacts"
        
        params = {
            "limit": limit,
            "archived": "false"
        }
        
        if after:
            params["after"] = after
        
        logger.info(f"Recupero {limit} contatti da HubSpot")
        response = await self._make_request("GET", endpoint, params=params)
        
        return response.get("results", [])
    
    async def get_companies(self, limit: int = 100, after: Optional[str] = None) -> List[Dict]:
        """Recupera le aziende da HubSpot."""
        endpoint = "/crm/v3/objects/companies"
        
        params = {
            "limit": limit,
            "archived": "false"
        }
        
        if after:
            params["after"] = after
        
        logger.info(f"Recupero {limit} aziende da HubSpot")
        response = await self._make_request("GET", endpoint, params=params)
        
        return response.get("results", [])
    
    async def get_pipeline_stages(self) -> List[Dict]:
        """Recupera le fasi delle pipeline da HubSpot."""
        endpoint = "/crm/v3/pipelines/deals"
        
        logger.info("Recupero pipeline stages da HubSpot")
        response = await self._make_request("GET", endpoint)
        
        stages = []
        for pipeline in response.get("results", []):
            stages.extend(pipeline.get("stages", []))
        
        return stages
    
    async def get_engagements(self, limit: int = 100, after: Optional[str] = None) -> List[Dict]:
        """Recupera le attività/engagement da HubSpot."""
        endpoint = "/engagements/v1/engagements/paged"
        
        params = {
            "limit": limit
        }
        
        if after:
            params["offset"] = after
        
        logger.info(f"Recupero {limit} engagements da HubSpot")
        response = await self._make_request("GET", endpoint, params=params)
        
        return response.get("results", [])
    
    async def get_custom_objects(self, object_type: str, limit: int = 100) -> List[Dict]:
        """Recupera oggetti custom da HubSpot."""
        endpoint = f"/crm/v3/objects/{object_type}"
        
        params = {
            "limit": limit,
            "archived": "false"
        }
        
        logger.info(f"Recupero {limit} oggetti custom {object_type} da HubSpot")
        response = await self._make_request("GET", endpoint, params=params)
        
        return response.get("results", [])
    
    async def exchange_code_for_token(self, code: str, redirect_uri: Optional[str] = None) -> bool:
        """
        Scambia l'authorization code per i token di accesso.
        
        Args:
            code: Authorization code ricevuto da HubSpot
            redirect_uri: URI di redirect (opzionale, usa settings se non fornito)
            
        Returns:
            bool: True se lo scambio è riuscito
        """
        try:
            # Usa il redirect_uri fornito o quello dalle settings
            final_redirect_uri = redirect_uri or settings.hubspot_redirect_uri
            
            if not final_redirect_uri:
                logger.error("Nessun redirect_uri disponibile per lo scambio token")
                return False
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    "https://api.hubapi.com/oauth/v1/token",
                    data={
                        "grant_type": "authorization_code",
                        "client_id": settings.hubspot_client_id,
                        "client_secret": settings.hubspot_client_secret,
                        "redirect_uri": final_redirect_uri,
                        "code": code
                    },
                    headers={"Content-Type": "application/x-www-form-urlencoded"}
                )
                
                if response.status_code == 200:
                    token_data = response.json()
                    
                    # Ottieni informazioni utente per creare il record
                    user_info = await self._get_user_info(token_data["access_token"])
                    
                    # Crea nuovo record token nel database
                    new_token = Token(
                        hubspot_user_id=user_info["user_id"],
                        hubspot_portal_id=user_info["hub_id"],
                        access_token=token_data["access_token"],
                        refresh_token=token_data["refresh_token"],
                        expires_at=datetime.utcnow() + timedelta(seconds=token_data["expires_in"]),
                        scopes=",".join([
                            "crm.objects.deals.read",
                            "crm.objects.deals.write", 
                            "crm.objects.contacts.read",
                            "crm.objects.contacts.write",
                            "crm.objects.companies.read",
                            "timeline.events.read",
                            "timeline.events.write",
                            "engagements.read",
                            "settings.user.read"
                        ])
                    )
                    
                    self.db.add(new_token)
                    await self.db.commit()
                    
                    logger.info(f"Token exchange riuscito per utente {user_info['user_id']}")
                    return True
                else:
                    logger.error(f"Token exchange fallito: {response.status_code} - {response.text}")
                    return False
                    
        except Exception as e:
            logger.error(f"Errore durante token exchange: {str(e)}")
            return False
    
    async def _get_user_info(self, access_token: str) -> Dict:
        """
        Ottiene le informazioni utente da HubSpot.
        
        Args:
            access_token: Token di accesso valido
            
        Returns:
            dict: Informazioni utente
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    "https://api.hubapi.com/oauth/v1/access-tokens",
                    headers={"Authorization": f"Bearer {access_token}"}
                )
                
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(f"Failed to get user info: {response.status_code} - {response.text}")
                    raise Exception(f"Failed to get user info: {response.text}")
                    
        except Exception as e:
            logger.error(f"Errore durante recupero info utente: {str(e)}")
            raise
    
    async def get_usage_stats(self) -> Dict:
        """Restituisce statistiche sull'uso del client."""
        return {
            "rate_limit_delay": self.rate_limit_delay,
            "base_url": self.base_url,
            "token_expires_at": self.token_expires_at.isoformat() if self.token_expires_at else None
        }


# Factory function per creare client con sessione database
async def create_hubspot_client(db: AsyncSession) -> HubSpotOAuthClient:
    """Crea un client HubSpot OAuth con sessione database."""
    return HubSpotOAuthClient(db)