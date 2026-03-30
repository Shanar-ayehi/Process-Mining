"""
API Routes per External Cards.

Questo modulo fornisce endpoint per gestire card esterne in HubSpot,
permettendo di creare, recuperare, sincronizzare ed eliminare card.
"""

from typing import Dict, List, Any, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel, Field
from datetime import datetime

from app.services.external_cards_service import (
    external_card_service,
    ExternalCardConfig,
    ExternalCardData,
    CardType,
    SyncStatus
)
from app.core.logger import get_logger

logger = get_logger()

router = APIRouter(prefix="/external-cards", tags=["External Cards"])


# Pydantic models per richieste/risposte
class CreateCardRequest(BaseModel):
    """Richiesta per creare una nuova card."""
    card_id: str = Field(..., description="ID univoco della card")
    name: str = Field(..., description="Nome visualizzato")
    card_type: CardType = Field(..., description="Tipo di card")
    hubspot_object_type: str = Field(..., description="Tipo oggetto HubSpot")
    properties_to_display: List[str] = Field(..., description="Proprietà da visualizzare")
    refresh_interval_minutes: int = Field(default=60, ge=5, le=1440)
    webhook_url: Optional[str] = Field(default=None)


class CardResponse(BaseModel):
    """Risposta con informazioni card."""
    card_id: str
    name: str
    card_type: str
    hubspot_object_type: str
    properties_to_display: List[str]
    refresh_interval_minutes: int
    is_active: bool
    created_at: str
    updated_at: str


class CardDataResponse(BaseModel):
    """Risposta con dati card."""
    card_id: str
    object_id: str
    data: Dict[str, Any]
    last_sync: str
    sync_status: str
    error_message: Optional[str] = None


class SyncResponse(BaseModel):
    """Risposta sincronizzazione."""
    card_id: str
    success: bool
    message: str
    timestamp: str


# Endpoints

@router.post("/", response_model=CardResponse, status_code=201)
async def create_external_card(request: CreateCardRequest):
    """
    Crea una nuova card esterna.
    
    Args:
        request: Configurazione della card
        
    Returns:
        CardResponse con informazioni card creata
        
    Raises:
        HTTPException: Se errore nella creazione
    """
    try:
        logger.info(f"Richiesta creazione card: {request.card_id}")
        
        # Crea configurazione
        config = ExternalCardConfig(
            card_id=request.card_id,
            name=request.name,
            card_type=request.card_type,
            hubspot_object_type=request.hubspot_object_type,
            properties_to_display=request.properties_to_display,
            refresh_interval_minutes=request.refresh_interval_minutes,
            webhook_url=request.webhook_url
        )
        
        # Crea card
        success = await external_card_service.create_card(config)
        
        if not success:
            raise HTTPException(
                status_code=500,
                detail=f"Errore nella creazione della card {request.card_id}"
            )
        
        # Recupera configurazione creata
        created_config = await external_card_service.get_card_config(request.card_id)
        if not created_config:
            raise HTTPException(
                status_code=500,
                detail="Card creata ma non recuperabile"
            )
        
        return CardResponse(
            card_id=created_config.card_id,
            name=created_config.name,
            card_type=created_config.card_type.value,
            hubspot_object_type=created_config.hubspot_object_type,
            properties_to_display=created_config.properties_to_display,
            refresh_interval_minutes=created_config.refresh_interval_minutes,
            is_active=created_config.is_active,
            created_at=created_config.created_at.isoformat(),
            updated_at=created_config.updated_at.isoformat()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nella creazione card: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{card_id}/data/{object_id}", response_model=CardDataResponse)
async def get_card_data(card_id: str, object_id: str):
    """
    Recupera dati per una card specifica e un oggetto HubSpot.
    
    Args:
        card_id: ID della card
        object_id: ID oggetto HubSpot
        
    Returns:
        CardDataResponse con dati della card
        
    Raises:
        HTTPException: Se card non trovata o errore
    """
    try:
        logger.info(f"Richiesta dati card {card_id} per oggetto {object_id}")
        
        # Recupera dati
        card_data = await external_card_service.get_card_data(card_id, object_id)
        
        if card_data is None:
            raise HTTPException(
                status_code=404,
                detail=f"Card {card_id} non trovata"
            )
        
        return CardDataResponse(
            card_id=card_data.card_id,
            object_id=card_data.object_id,
            data=card_data.data,
            last_sync=card_data.last_sync.isoformat(),
            sync_status=card_data.sync_status.value,
            error_message=card_data.error_message
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nel recupero dati card: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=List[CardResponse])
async def list_external_cards(
    card_type: Optional[CardType] = Query(None, description="Filtro per tipo card")
):
    """
    Lista tutte le card esterne configurate.
    
    Args:
        card_type: Filtro opzionale per tipo card
        
    Returns:
        Lista di CardResponse
        
    Raises:
        HTTPException: Se errore nel recupero
    """
    try:
        logger.info(f"Richiesta lista card (filtro: {card_type})")
        
        # Recupera lista
        configs = await external_card_service.list_cards(card_type)
        
        # Converti in risposta
        response = []
        for config in configs:
            response.append(CardResponse(
                card_id=config.card_id,
                name=config.name,
                card_type=config.card_type.value,
                hubspot_object_type=config.hubspot_object_type,
                properties_to_display=config.properties_to_display,
                refresh_interval_minutes=config.refresh_interval_minutes,
                is_active=config.is_active,
                created_at=config.created_at.isoformat(),
                updated_at=config.updated_at.isoformat()
            ))
        
        return response
        
    except Exception as e:
        logger.error(f"Errore nel recupero lista card: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{card_id}", response_model=CardResponse)
async def get_card_config(card_id: str):
    """
    Recupera configurazione di una card specifica.
    
    Args:
        card_id: ID della card
        
    Returns:
        CardResponse con configurazione card
        
    Raises:
        HTTPException: Se card non trovata
    """
    try:
        logger.info(f"Richiesta configurazione card: {card_id}")
        
        # Recupera configurazione
        config = await external_card_service.get_card_config(card_id)
        
        if config is None:
            raise HTTPException(
                status_code=404,
                detail=f"Card {card_id} non trovata"
            )
        
        return CardResponse(
            card_id=config.card_id,
            name=config.name,
            card_type=config.card_type.value,
            hubspot_object_type=config.hubspot_object_type,
            properties_to_display=config.properties_to_display,
            refresh_interval_minutes=config.refresh_interval_minutes,
            is_active=config.is_active,
            created_at=config.created_at.isoformat(),
            updated_at=config.updated_at.isoformat()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nel recupero configurazione card: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{card_id}/sync", response_model=SyncResponse)
async def sync_card(card_id: str):
    """
    Forza sincronizzazione dati card.
    
    Args:
        card_id: ID della card da sincronizzare
        
    Returns:
        SyncResponse con risultato sincronizzazione
        
    Raises:
        HTTPException: Se card non trovata o errore
    """
    try:
        logger.info(f"Richiesta sincronizzazione card: {card_id}")
        
        # Verifica esistenza card
        config = await external_card_service.get_card_config(card_id)
        if config is None:
            raise HTTPException(
                status_code=404,
                detail=f"Card {card_id} non trovata"
            )
        
        # Esegui sincronizzazione
        success = await external_card_service.sync_card(card_id)
        
        return SyncResponse(
            card_id=card_id,
            success=success,
            message="Sincronizzazione completata" if success else "Sincronizzazione fallita",
            timestamp=datetime.now().isoformat()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nella sincronizzazione card: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{card_id}", status_code=204)
async def delete_card(card_id: str):
    """
    Elimina una card esterna.
    
    Args:
        card_id: ID della card da eliminare
        
    Raises:
        HTTPException: Se card non trovata o errore
    """
    try:
        logger.info(f"Richiesta eliminazione card: {card_id}")
        
        # Verifica esistenza card
        config = await external_card_service.get_card_config(card_id)
        if config is None:
            raise HTTPException(
                status_code=404,
                detail=f"Card {card_id} non trovata"
            )
        
        # Elimina card
        success = await external_card_service.delete_card(card_id)
        
        if not success:
            raise HTTPException(
                status_code=500,
                detail=f"Errore nell'eliminazione della card {card_id}"
            )
        
        # 204 No Content per eliminazione riuscita
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nell'eliminazione card: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{card_id}/status")
async def get_card_status(card_id: str):
    """
    Recupera stato di una card (ultima sync, errori, etc).
    
    Args:
        card_id: ID della card
        
    Returns:
        Dict con informazioni stato
        
    Raises:
        HTTPException: Se card non trovata
    """
    try:
        logger.info(f"Richiesta stato card: {card_id}")
        
        # Recupera configurazione
        config = await external_card_service.get_card_config(card_id)
        if config is None:
            raise HTTPException(
                status_code=404,
                detail=f"Card {card_id} non trovata"
            )
        
        return {
            "card_id": card_id,
            "name": config.name,
            "is_active": config.is_active,
            "card_type": config.card_type.value,
            "hubspot_object_type": config.hubspot_object_type,
            "refresh_interval_minutes": config.refresh_interval_minutes,
            "created_at": config.created_at.isoformat(),
            "updated_at": config.updated_at.isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nel recupero stato card: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{card_id}/dashboard/{object_id}")
async def get_card_dashboard(card_id: str, object_id: str):
    """
    Recupera dashboard completo per una card e oggetto HubSpot.
    
    Args:
        card_id: ID della card
        object_id: ID oggetto HubSpot
        
    Returns:
        Dict con dati dashboard completo
        
    Raises:
        HTTPException: Se card non trovata o errore
    """
    try:
        logger.info(f"Richiesta dashboard card {card_id} per oggetto {object_id}")
        
        # Recupera dashboard
        dashboard = await external_card_service.get_card_dashboard(card_id, object_id)
        
        if dashboard is None:
            raise HTTPException(
                status_code=404,
                detail=f"Card {card_id} non trovata"
            )
        
        return dashboard
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nel recupero dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{card_id}/analytics/{object_id}")
async def get_card_analytics(card_id: str, object_id: str):
    """
    Recupera analisi Process Mining per una card.
    
    Args:
        card_id: ID della card
        object_id: ID oggetto HubSpot
        
    Returns:
        Dict con analisi Process Mining
        
    Raises:
        HTTPException: Se card non trovata o errore
    """
    try:
        logger.info(f"Richiesta analisi card {card_id} per oggetto {object_id}")
        
        # Recupera analisi
        analytics = await external_card_service.get_card_analytics(card_id, object_id)
        
        if analytics is None:
            raise HTTPException(
                status_code=404,
                detail=f"Card {card_id} non trovata"
            )
        
        return analytics
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nel recupero analisi: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{card_id}/associations/{object_id}")
async def get_card_associations(card_id: str, object_id: str):
    """
    Recupera associazioni per una card.
    
    Args:
        card_id: ID della card
        object_id: ID oggetto HubSpot
        
    Returns:
        Dict con associazioni
        
    Raises:
        HTTPException: Se card non trovata o errore
    """
    try:
        logger.info(f"Richiesta associazioni card {card_id} per oggetto {object_id}")
        
        # Recupera associazioni
        associations = await external_card_service.get_card_associations(card_id, object_id)
        
        if associations is None:
            raise HTTPException(
                status_code=404,
                detail=f"Card {card_id} non trovata"
            )
        
        return associations
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nel recupero associazioni: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{card_id}/timeline/{object_id}")
async def get_card_timeline(card_id: str, object_id: str):
    """
    Recupera timeline per una card.
    
    Args:
        card_id: ID della card
        object_id: ID oggetto HubSpot
        
    Returns:
        Dict con timeline eventi
        
    Raises:
        HTTPException: Se card non trovata o errore
    """
    try:
        logger.info(f"Richiesta timeline card {card_id} per oggetto {object_id}")
        
        # Recupera timeline
        timeline = await external_card_service.get_card_timeline(card_id, object_id)
        
        if timeline is None:
            raise HTTPException(
                status_code=404,
                detail=f"Card {card_id} non trovata"
            )
        
        return timeline
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nel recupero timeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health/check")
async def health_check():
    """
    Health check per il servizio External Cards.
    
    Returns:
        Dict con stato servizio
    """
    try:
        # Verifica servizio
        return {
            "status": "healthy",
            "service": "external_cards",
            "timestamp": datetime.now().isoformat(),
            "features": {
                "create_card": "available",
                "get_card_data": "available",
                "get_card_dashboard": "available",
                "get_card_analytics": "available",
                "get_card_associations": "available",
                "get_card_timeline": "available",
                "sync_card": "available",
                "delete_card": "available",
                "list_cards": "available"
            }
        }
        
    except Exception as e:
        logger.error(f"Errore health check: {e}")
        return {
            "status": "unhealthy",
            "service": "external_cards",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }
