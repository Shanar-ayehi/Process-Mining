"""
Test per il servizio External Cards.

Questo modulo contiene test per verificare l'integrazione con HubSpot
e il funzionamento delle card esterne.
"""

import pytest
import asyncio
from datetime import datetime
from typing import Dict, Any
from unittest.mock import AsyncMock, MagicMock, patch

from app.services.external_cards_service import (
    ExternalCardService,
    ExternalCardConfig,
    CardType,
    SyncStatus
)
from app.connectors.hubspot_client import HubSpotClient


class TestExternalCardService:
    """Test per il servizio External Cards."""
    
    @pytest.fixture
    def mock_hubspot_client(self):
        """Crea un mock del client HubSpot."""
        client = AsyncMock(spec=HubSpotClient)
        
        # Mock per get_deal
        client.get_deal.return_value = {
            "id": "123456",
            "properties": {
                "dealname": "Test Deal",
                "amount": "10000",
                "dealstage": "contractsent",
                "pipeline": "default",
                "createdate": "2024-01-01T00:00:00Z",
                "closedate": "2024-02-01T00:00:00Z",
                "hubspot_owner_id": "owner123"
            }
        }
        
        # Mock per get_contact
        client.get_contact.return_value = {
            "id": "789012",
            "properties": {
                "firstname": "Mario",
                "lastname": "Rossi",
                "email": "mario.rossi@example.com",
                "company": "Test Company",
                "createdate": "2024-01-01T00:00:00Z",
                "lastmodifieddate": "2024-01-15T00:00:00Z",
                "lifecyclestage": "lead",
                "hs_lead_status": "new"
            }
        }
        
        # Mock per get_company
        client.get_company.return_value = {
            "id": "345678",
            "properties": {
                "name": "Test Company SRL",
                "domain": "testcompany.com",
                "industry": "Technology",
                "createdate": "2024-01-01T00:00:00Z",
                "hs_lastmodifieddate": "2024-01-15T00:00:00Z",
                "city": "Milano",
                "country": "Italy"
            }
        }
        
        # Mock per get_deal_associations
        client.get_deal_associations.return_value = {
            "contacts": [
                {"id": "789012", "properties": {"firstname": "Mario", "lastname": "Rossi"}}
            ],
            "companies": [
                {"id": "345678", "properties": {"name": "Test Company SRL"}}
            ]
        }
        
        # Mock per get_deal_timeline
        client.get_deal_timeline.return_value = [
            {
                "id": "event1",
                "timestamp": "2024-01-05T10:00:00Z",
                "type": "NOTE",
                "body": "Prima nota sul deal"
            },
            {
                "id": "event2",
                "timestamp": "2024-01-10T14:00:00Z",
                "type": "EMAIL",
                "body": "Email inviata al cliente"
            }
        ]
        
        # Mock per get_associations
        client.get_associations.return_value = [
            {"id": "123456", "type": "deal_to_contact"}
        ]
        
        # Mock per get_timeline_events
        client.get_timeline_events.return_value = [
            {
                "id": "note1",
                "createdAt": "2024-01-05T10:00:00Z",
                "body": "Nota di test"
            }
        ]
        
        return client
    
    @pytest.fixture
    def mock_repository(self):
        """Crea un mock del repository."""
        repository = AsyncMock()
        
        # Mock per save_external_card_config
        repository.save_external_card_config.return_value = True
        
        # Mock per get_external_card_config
        repository.get_external_card_config.return_value = None
        
        # Mock per list_external_card_configs
        repository.list_external_card_configs.return_value = []
        
        # Mock per delete_external_card_config
        repository.delete_external_card_config.return_value = True
        
        # Mock per cache
        repository.get_cached_data.return_value = None
        repository.save_cached_data.return_value = True
        repository.delete_cached_data.return_value = True
        repository.invalidate_cache_pattern.return_value = True
        
        return repository
    
    @pytest.fixture
    def external_card_service(self, mock_hubspot_client, mock_repository):
        """Crea un'istanza del servizio External Cards con mock."""
        service = ExternalCardService(hubspot_client=mock_hubspot_client)
        service.repository = mock_repository
        return service
    
    @pytest.fixture
    def sample_card_config(self):
        """Crea una configurazione card di esempio."""
        return ExternalCardConfig(
            card_id="test_card_001",
            name="Test Deal Card",
            card_type=CardType.DEAL,
            hubspot_object_type="deals",
            properties_to_display=["dealname", "amount", "dealstage"],
            refresh_interval_minutes=60
        )
    
    @pytest.mark.asyncio
    async def test_create_card(self, external_card_service, sample_card_config):
        """Test creazione card."""
        # Esegui
        result = await external_card_service.create_card(sample_card_config)
        
        # Verifica
        assert result is True
        external_card_service.repository.save_external_card_config.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_card_data_deal(self, external_card_service, sample_card_config):
        """Test recupero dati card per deal."""
        # Setup
        external_card_service.repository.get_external_card_config.return_value = {
            "card_id": "test_card_001",
            "name": "Test Deal Card",
            "card_type": "deal",
            "hubspot_object_type": "deals",
            "properties_to_display": ["dealname", "amount", "dealstage"],
            "refresh_interval_minutes": 60,
            "is_active": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        # Esegui
        result = await external_card_service.get_card_data("test_card_001", "123456")
        
        # Verifica
        assert result is not None
        assert result.card_id == "test_card_001"
        assert result.object_id == "123456"
        assert result.sync_status == SyncStatus.SUCCESS
        assert "dealname" in result.data
        assert result.data["dealname"] == "Test Deal"
    
    @pytest.mark.asyncio
    async def test_get_card_data_contact(self, mock_hubspot_client, mock_repository):
        """Test recupero dati card per contatto."""
        # Setup
        service = ExternalCardService(hubspot_client=mock_hubspot_client)
        service.repository = mock_repository
        
        mock_repository.get_external_card_config.return_value = {
            "card_id": "test_card_002",
            "name": "Test Contact Card",
            "card_type": "contact",
            "hubspot_object_type": "contacts",
            "properties_to_display": ["firstname", "lastname", "email"],
            "refresh_interval_minutes": 60,
            "is_active": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        # Esegui
        result = await service.get_card_data("test_card_002", "789012")
        
        # Verifica
        assert result is not None
        assert result.card_id == "test_card_002"
        assert result.object_id == "789012"
        assert result.sync_status == SyncStatus.SUCCESS
        assert "firstname" in result.data
        assert result.data["firstname"] == "Mario"
    
    @pytest.mark.asyncio
    async def test_get_card_dashboard(self, external_card_service):
        """Test recupero dashboard card."""
        # Setup
        external_card_service.repository.get_external_card_config.return_value = {
            "card_id": "test_card_001",
            "name": "Test Deal Card",
            "card_type": "deal",
            "hubspot_object_type": "deals",
            "properties_to_display": ["dealname", "amount", "dealstage"],
            "refresh_interval_minutes": 60,
            "is_active": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        # Esegui
        result = await external_card_service.get_card_dashboard("test_card_001", "123456")
        
        # Verifica
        assert result is not None
        assert result["card_id"] == "test_card_001"
        assert result["object_id"] == "123456"
        assert result["object_type"] == "deals"
        assert "object_data" in result
        assert "associations" in result
        assert "timeline" in result
        assert "metadata" in result
    
    @pytest.mark.asyncio
    async def test_get_card_analytics(self, external_card_service):
        """Test recupero analisi card."""
        # Setup
        external_card_service.repository.get_external_card_config.return_value = {
            "card_id": "test_card_001",
            "name": "Test Deal Card",
            "card_type": "deal",
            "hubspot_object_type": "deals",
            "properties_to_display": ["dealname", "amount", "dealstage"],
            "refresh_interval_minutes": 60,
            "is_active": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        # Mock per _fetch_hubspot_data
        with patch.object(external_card_service, '_fetch_hubspot_data') as mock_fetch:
            # Il service si aspetta formato {"properties": {...}}
            mock_fetch.return_value = {
                "properties": {
                    "dealname": "Test Deal",
                    "amount": "10000",
                    "dealstage": "contractsent"
                }
            }
            
            # Esegui
            result = await external_card_service.get_card_analytics("test_card_001", "123456")
        
        # Verifica
        assert result is not None
        assert result["deal_id"] == "123456"
        assert result["deal_name"] == "Test Deal"
        assert result["amount"] == "10000"
        assert result["stage"] == "contractsent"
        assert "timeline_events_count" in result
        assert "metadata" in result
    
    @pytest.mark.asyncio
    async def test_get_card_associations(self, external_card_service):
        """Test recupero associazioni card."""
        # Setup
        external_card_service.repository.get_external_card_config.return_value = {
            "card_id": "test_card_001",
            "name": "Test Deal Card",
            "card_type": "deal",
            "hubspot_object_type": "deals",
            "properties_to_display": ["dealname", "amount", "dealstage"],
            "refresh_interval_minutes": 60,
            "is_active": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        # Esegui
        result = await external_card_service.get_card_associations("test_card_001", "123456")
        
        # Verifica
        assert result is not None
        assert result["card_id"] == "test_card_001"
        assert result["object_id"] == "123456"
        assert result["object_type"] == "deals"
        assert "associations" in result
    
    @pytest.mark.asyncio
    async def test_get_card_timeline(self, external_card_service):
        """Test recupero timeline card."""
        # Setup
        external_card_service.repository.get_external_card_config.return_value = {
            "card_id": "test_card_001",
            "name": "Test Deal Card",
            "card_type": "deal",
            "hubspot_object_type": "deals",
            "properties_to_display": ["dealname", "amount", "dealstage"],
            "refresh_interval_minutes": 60,
            "is_active": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        # Esegui
        result = await external_card_service.get_card_timeline("test_card_001", "123456")
        
        # Verifica
        assert result is not None
        assert result["card_id"] == "test_card_001"
        assert result["object_id"] == "123456"
        assert result["object_type"] == "deals"
        assert "timeline" in result
        assert len(result["timeline"]) > 0
    
    @pytest.mark.asyncio
    async def test_sync_card(self, external_card_service):
        """Test sincronizzazione card."""
        # Setup
        external_card_service.repository.get_external_card_config.return_value = {
            "card_id": "test_card_001",
            "name": "Test Deal Card",
            "card_type": "deal",
            "hubspot_object_type": "deals",
            "properties_to_display": ["dealname", "amount", "dealstage"],
            "refresh_interval_minutes": 60,
            "is_active": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        # Esegui
        result = await external_card_service.sync_card("test_card_001")
        
        # Verifica
        assert result is True
        external_card_service.repository.invalidate_cache_pattern.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_card(self, external_card_service):
        """Test eliminazione card."""
        # Setup
        external_card_service.repository.get_external_card_config.return_value = {
            "card_id": "test_card_001",
            "name": "Test Deal Card",
            "card_type": "deal",
            "hubspot_object_type": "deals",
            "properties_to_display": ["dealname", "amount", "dealstage"],
            "refresh_interval_minutes": 60,
            "is_active": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        # Esegui
        result = await external_card_service.delete_card("test_card_001")
        
        # Verifica
        assert result is True
        external_card_service.repository.delete_external_card_config.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_cards(self, external_card_service):
        """Test lista card."""
        # Setup
        external_card_service.repository.list_external_card_configs.return_value = [
            {
                "card_id": "test_card_001",
                "name": "Test Deal Card",
                "card_type": "deal",
                "hubspot_object_type": "deals",
                "properties_to_display": ["dealname", "amount"],
                "refresh_interval_minutes": 60,
                "is_active": True,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            },
            {
                "card_id": "test_card_002",
                "name": "Test Contact Card",
                "card_type": "contact",
                "hubspot_object_type": "contacts",
                "properties_to_display": ["firstname", "lastname"],
                "refresh_interval_minutes": 60,
                "is_active": True,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
        ]
        
        # Esegui
        result = await external_card_service.list_cards()
        
        # Verifica
        assert len(result) == 2
        assert result[0].card_id == "test_card_001"
        assert result[1].card_id == "test_card_002"
    
    @pytest.mark.asyncio
    async def test_list_cards_with_filter(self, external_card_service):
        """Test lista card con filtro per tipo."""
        # Setup
        external_card_service.repository.list_external_card_configs.return_value = [
            {
                "card_id": "test_card_001",
                "name": "Test Deal Card",
                "card_type": "deal",
                "hubspot_object_type": "deals",
                "properties_to_display": ["dealname", "amount"],
                "refresh_interval_minutes": 60,
                "is_active": True,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            },
            {
                "card_id": "test_card_002",
                "name": "Test Contact Card",
                "card_type": "contact",
                "hubspot_object_type": "contacts",
                "properties_to_display": ["firstname", "lastname"],
                "refresh_interval_minutes": 60,
                "is_active": True,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
        ]
        
        # Esegui con filtro
        result = await external_card_service.list_cards(card_type=CardType.DEAL)
        
        # Verifica
        assert len(result) == 1
        assert result[0].card_id == "test_card_001"
        assert result[0].card_type == CardType.DEAL
    
    @pytest.mark.asyncio
    async def test_get_card_data_not_found(self, external_card_service):
        """Test recupero dati card non trovata."""
        # Setup
        external_card_service.repository.get_external_card_config.return_value = None
        
        # Esegui
        result = await external_card_service.get_card_data("non_existent_card", "123456")
        
        # Verifica
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_card_data_hubspot_error(self, external_card_service):
        """Test gestione errore HubSpot."""
        # Setup
        external_card_service.repository.get_external_card_config.return_value = {
            "card_id": "test_card_001",
            "name": "Test Deal Card",
            "card_type": "deal",
            "hubspot_object_type": "deals",
            "properties_to_display": ["dealname", "amount", "dealstage"],
            "refresh_interval_minutes": 60,
            "is_active": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        # Simula errore HubSpot
        external_card_service.hubspot.get_deal.side_effect = Exception("API Error")
        
        # Esegui
        result = await external_card_service.get_card_data("test_card_001", "123456")
        
        # Verifica
        assert result is not None
        assert result.sync_status == SyncStatus.ERROR
        assert result.error_message is not None


class TestExternalCardConfig:
    """Test per la configurazione delle card esterne."""
    
    def test_card_config_validation(self):
        """Test validazione configurazione card."""
        # Configurazione valida
        config = ExternalCardConfig(
            card_id="test_card_001",
            name="Test Card",
            card_type=CardType.DEAL,
            hubspot_object_type="deals",
            properties_to_display=["dealname", "amount"]
        )
        
        assert config.card_id == "test_card_001"
        assert config.name == "Test Card"
        assert config.card_type == CardType.DEAL
        assert config.hubspot_object_type == "deals"
        assert len(config.properties_to_display) == 2
    
    def test_card_config_invalid_card_id(self):
        """Test validazione card_id non valido."""
        with pytest.raises(ValueError):
            ExternalCardConfig(
                card_id="ab",  # Troppo corto
                name="Test Card",
                card_type=CardType.DEAL,
                hubspot_object_type="deals",
                properties_to_display=["dealname"]
            )
    
    def test_card_config_invalid_object_type(self):
        """Test validazione tipo oggetto non valido."""
        with pytest.raises(ValueError):
            ExternalCardConfig(
                card_id="test_card_001",
                name="Test Card",
                card_type=CardType.DEAL,
                hubspot_object_type="invalid_type",  # Non valido
                properties_to_display=["dealname"]
            )


# Test di integrazione (richiedono connessione reale a HubSpot)
class TestExternalCardIntegration:
    """Test di integrazione con HubSpot (richiedono token OAuth valido)."""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_real_hubspot_connection(self):
        """Test connessione reale a HubSpot (skippato se non configurato)."""
        pytest.skip("Test di integrazione - richiede configurazione OAuth reale")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_real_deal_retrieval(self):
        """Test recupero deal reale (skippato se non configurato)."""
        pytest.skip("Test di integrazione - richiede configurazione OAuth reale")


if __name__ == "__main__":
    # Esegui test
    pytest.main([__file__, "-v"])