from typing import List, Dict, Optional, Union, Any
from pathlib import Path
import json
import asyncio
from app.core.logger import get_logger
from app.core.config import settings
from app.connectors.hubspot_client import HubSpotClient, HubSpotAPIError, create_hubspot_client
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.hubspot_config import hubspot_config_manager

logger = get_logger()

class DataExtractionService:
    """Servizio per l'estrazione dati da HubSpot e altre sorgenti."""
    
    def __init__(self, db: AsyncSession):
        """
        Inizializza il servizio di estrazione dati.
        
        Args:
            db: Sessione database per gestione token OAuth
        """
        self.db = db
        self.data_dir = settings.raw_data_dir
    
    async def _get_hubspot_client(self) -> HubSpotClient:
        """Ottiene un client HubSpot OAuth con sessione database."""
        return await create_hubspot_client(self.db)
    
    async def extract_deals_with_history(self, 
                                       properties_with_history: Optional[List[str]] = None,
                                       save_to_file: bool = True) -> List[Dict[str, Any]]:
        """
        Estrae deal con cronologia da HubSpot.
        
        Args:
            properties_with_history: Proprietà di cui estrarre la cronologia
            save_to_file: Se salvare i dati in file JSON
            
        Returns:
            Lista di deal con cronologia
        """
        # Usa la configurazione dinamica se non specificato
        if properties_with_history is None:
            properties_with_history = [hubspot_config_manager.get_data_structure().stage_field]
        
        logger.info(f"Inizio estrazione deal con cronologia: {properties_with_history}")
        
        try:
            # Estrai dati da HubSpot
            # hubspot_client = await self._get_hubspot_client()
            # deals_data = await hubspot_client.get_all_deals_with_history(
            #     properties_with_history=properties_with_history
            # )
            
            # MOCK MODE: Bypass API HubSpot - Lettura da file locale
            logger.warning("⚠️ MOCK MODE ATTIVO: Bypass API HubSpot. Lettura da file locale in corso...")
            mock_file_path = self.data_dir / "mock_deals.json"
            deals_data = await self.load_deals_from_file(mock_file_path)
            
            if save_to_file:
                # Salva i dati grezzi
                timestamp = self._get_timestamp()
                filename = f"hubspot_deals_{timestamp}.json"
                filepath = self.data_dir / filename
                
                await self._save_json_async(deals_data, filepath)
                logger.info(f"Dati deal salvati in: {filepath}")
            
            logger.info(f"Estratti {len(deals_data)} deal con cronologia")
            return deals_data
            
        except HubSpotAPIError as e:
            logger.error(f"Errore durante l'estrazione deal: {e}")
            raise
        except Exception as e:
            logger.error(f"Errore imprevisto durante l'estrazione deal: {e}")
            raise
    
    async def extract_contacts(self, save_to_file: bool = True) -> List[Dict[str, Any]]:
        """Estrae contatti da HubSpot."""
        logger.info("Inizio estrazione contatti da HubSpot")
        
        try:
            hubspot_client = await self._get_hubspot_client()
            all_contacts = []
            after = None
            
            while True:
                contacts = await hubspot_client.get_contacts(limit=100, after=after)
                
                if not contacts:
                    break
                
                all_contacts.extend(contacts)
                
                # Controlla se ci sono altri risultati
                if len(contacts) < 100:
                    break
                
                # Prepara prossima pagina
                after = contacts[-1].get("id")
            
            if save_to_file:
                timestamp = self._get_timestamp()
                filename = f"hubspot_contacts_{timestamp}.json"
                filepath = self.data_dir / filename
                
                await self._save_json_async(all_contacts, filepath)
                logger.info(f"Dati contatti salvati in: {filepath}")
            
            logger.info(f"Estratti {len(all_contacts)} contatti")
            return all_contacts
            
        except HubSpotAPIError as e:
            logger.error(f"Errore durante l'estrazione contatti: {e}")
            raise
    
    async def extract_companies(self, save_to_file: bool = True) -> List[Dict[str, Any]]:
        """Estrae aziende da HubSpot."""
        logger.info("Inizio estrazione aziende da HubSpot")
        
        try:
            hubspot_client = await self._get_hubspot_client()
            all_companies = []
            after = None
            
            while True:
                companies = await hubspot_client.get_companies(limit=100, after=after)
                
                if not companies:
                    break
                
                all_companies.extend(companies)
                
                # Controlla se ci sono altri risultati
                if len(companies) < 100:
                    break
                
                # Prepara prossima pagina
                after = companies[-1].get("id")
            
            if save_to_file:
                timestamp = self._get_timestamp()
                filename = f"hubspot_companies_{timestamp}.json"
                filepath = self.data_dir / filename
                
                await self._save_json_async(all_companies, filepath)
                logger.info(f"Dati aziende salvati in: {filepath}")
            
            logger.info(f"Estratte {len(all_companies)} aziende")
            return all_companies
            
        except HubSpotAPIError as e:
            logger.error(f"Errore durante l'estrazione aziende: {e}")
            raise
    
    async def extract_pipeline_stages(self, save_to_file: bool = True) -> List[Dict[str, Any]]:
        """Estrae le fasi delle pipeline da HubSpot."""
        logger.info("Inizio estrazione pipeline stages da HubSpot")
        
        try:
            hubspot_client = await self._get_hubspot_client()
            stages = await hubspot_client.get_pipeline_stages()
            
            if save_to_file:
                timestamp = self._get_timestamp()
                filename = f"hubspot_pipeline_stages_{timestamp}.json"
                filepath = self.data_dir / filename
                
                await self._save_json_async(stages, filepath)
                logger.info(f"Dati pipeline stages salvati in: {filepath}")
            
            logger.info(f"Estratte {len(stages)} fasi di pipeline")
            return stages
            
        except HubSpotAPIError as e:
            logger.error(f"Errore durante l'estrazione pipeline stages: {e}")
            raise
    
    async def extract_workflows(self, save_to_file: bool = True) -> List[Dict[str, Any]]:
        """Estrae workflow da HubSpot."""
        logger.info("Inizio estrazione workflow da HubSpot")
        
        try:
            hubspot_client = await self._get_hubspot_client()
            workflows = await hubspot_client.get_workflows()
            
            if save_to_file:
                timestamp = self._get_timestamp()
                filename = f"hubspot_workflows_{timestamp}.json"
                filepath = self.data_dir / filename
                
                await self._save_json_async(workflows, filepath)
                logger.info(f"Dati workflow salvati in: {filepath}")
            
            logger.info(f"Estratti {len(workflows)} workflow")
            return workflows
            
        except HubSpotAPIError as e:
            logger.error(f"Errore durante l'estrazione workflow: {e}")
            raise
    
    async def extract_all_data(self, save_to_file: bool = True) -> Dict[str, List[Dict[str, Any]]]:
        """
        Estrae tutti i dati disponibili da HubSpot.
        
        Args:
            save_to_file: Se salvare i dati in file JSON
            
        Returns:
            Dizionario con tutti i dati estratti
        """
        logger.info("Inizio estrazione completa dati HubSpot")
        
        try:
            # Estrai tutti i dati in parallelo
            tasks = [
                self.extract_deals_with_history(save_to_file=save_to_file),
                self.extract_contacts(save_to_file=save_to_file),
                self.extract_companies(save_to_file=save_to_file),
                self.extract_pipeline_stages(save_to_file=save_to_file),
                self.extract_workflows(save_to_file=save_to_file)
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Gestisci eventuali eccezioni
            data = {}
            data_names = ['deals', 'contacts', 'companies', 'pipeline_stages', 'workflows']
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Errore nell'estrazione {data_names[i]}: {result}")
                    data[data_names[i]] = []
                else:
                    data[data_names[i]] = result
            
            logger.info("Estrazione completa dati HubSpot completata")
            return data
            
        except Exception as e:
            logger.error(f"Errore durante l'estrazione completa: {e}")
            raise
    
    async def load_deals_from_file(self, filepath: Union[str, Path]) -> List[Dict[str, Any]]:
        """
        Carica deal da file JSON locale.
        
        Args:
            filepath: Percorso del file JSON
            
        Returns:
            Lista di deal
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"Caricati {len(data)} deal da {filepath}")
            return data
        except FileNotFoundError:
            logger.error(f"File non trovato: {filepath}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Errore nel parsing JSON: {e}")
            raise
    
    async def _save_json_async(self, data: List[Dict[str, Any]], filepath: Path) -> None:
        """Salva dati in formato JSON in modo asincrono."""
        import aiofiles
        
        # Crea la directory se non esiste
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(data, indent=2, ensure_ascii=False))
    
    def _get_timestamp(self) -> str:
        """Genera timestamp per i nomi file."""
        from datetime import datetime
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    
    async def get_extraction_stats(self) -> Dict[str, Union[int, str]]:
        """Restituisce statistiche sull'estrazione."""
        hubspot_client = await self._get_hubspot_client()
        client_stats = await hubspot_client.get_usage_stats()
        config_stats = hubspot_config_manager.validate_config()
        
        return {
            "hubspot_requests": client_stats.get("request_count", 0),
            "rate_limit_delay": client_stats.get("rate_limit_delay", 0),
            "data_dir": str(self.data_dir),
            "config_valid": config_stats.get("valid", False),
            "stage_count": config_stats.get("stage_count", 0),
            "custom_properties_count": config_stats.get("custom_properties_count", 0),
            "timestamp": self._get_timestamp()
        }

# Nota: L'istanza globale non può essere creata qui perché richiede una sessione database
# Le istanze devono essere create nei servizi che hanno accesso alla sessione database

# Placeholder per l'istanza globale (verrà creata nei servizi)
data_extraction_service = None
