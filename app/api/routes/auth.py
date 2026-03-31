"""
OAuth 2.0 Authentication Routes for HubSpot Integration

Questo modulo gestisce l'autenticazione OAuth 2.0 con HubSpot,
compreso il flusso di autorizzazione, gestione token e refresh.
"""

from typing import Optional
from fastapi import APIRouter, Request, HTTPException, Depends
from fastapi.responses import RedirectResponse, JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import datetime, timedelta
import httpx
import os
from urllib.parse import urlencode

from app.core.database import get_db
from app.core.config import settings
from app.core.logger import get_logger
from app.core.security import create_access_token
from app.models.auth import Token
from app.schemas.auth import AuthResponse, TokenRefreshRequest

logger = get_logger()

router = APIRouter(prefix="/auth", tags=["authentication"])

# Configurazione HubSpot OAuth
HUBSPOT_CLIENT_ID = os.getenv("HUBSPOT_CLIENT_ID")
HUBSPOT_CLIENT_SECRET = os.getenv("HUBSPOT_CLIENT_SECRET")
HUBSPOT_REDIRECT_URI = os.getenv("HUBSPOT_REDIRECT_URI", f"{settings.API_BASE_URL}/api/v1/auth/callback")
HUBSPOT_AUTH_URL = "https://app.hubspot.com/oauth/authorize"
HUBSPOT_TOKEN_URL = "https://api.hubapi.com/oauth/v1/token"
HUBSPOT_USER_INFO_URL = "https://api.hubapi.com/oauth/v1/access-tokens"

# Scopes necessari per il Process Mining
HUBSPOT_SCOPES = [
    "crm.objects.deals.read",
    "crm.objects.deals.write", 
    "crm.objects.contacts.read",
    "crm.objects.contacts.write",
    "crm.objects.companies.read",
    "timeline",
    "settings.users.read",
    "automation"
]


@router.get("/hubspot/login")
async def hubspot_login():
    """
    Inizia il flusso OAuth 2.0 con HubSpot.
    
    Returns:
        RedirectResponse: Redirect alla pagina di autorizzazione HubSpot
    """
    if not HUBSPOT_CLIENT_ID:
        raise HTTPException(
            status_code=500,
            detail="HUBSPOT_CLIENT_ID non configurato"
        )
    
    # Parametri per la richiesta OAuth
    params = {
        "client_id": HUBSPOT_CLIENT_ID,
        "redirect_uri": HUBSPOT_REDIRECT_URI,
        "scope": " ".join(HUBSPOT_SCOPES),
        "response_type": "code"
    }
    
    # Costruisci URL di autorizzazione
    auth_url = f"{HUBSPOT_AUTH_URL}?{urlencode(params)}"
    
    logger.info(f"Redirecting to HubSpot OAuth: {auth_url}")
    return RedirectResponse(url=auth_url)


@router.get("/callback")
async def auth_callback(
    request: Request,
    code: Optional[str] = None,
    error: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Gestisce il callback OAuth 2.0 da HubSpot.
    
    Args:
        request: Richiesta FastAPI
        code: Authorization code da HubSpot
        error: Eventuale errore OAuth
        db: Sessione database
    
    Returns:
        JSONResponse: Token di accesso o errore
    """
    if error:
        logger.error(f"OAuth error from HubSpot: {error}")
        return JSONResponse(
            status_code=400,
            content={"error": f"OAuth error: {error}"}
        )
    
    if not code:
        logger.error("No authorization code received")
        return JSONResponse(
            status_code=400,
            content={"error": "No authorization code received"}
        )
    
    try:
        # Scambia il code per i token
        token_data = await exchange_code_for_tokens(code)
        
        # Ottieni informazioni utente
        user_info = await get_hubspot_user_info(token_data["access_token"])
        
        # Salva o aggiorna token nel database
        await save_tokens_to_db(db, user_info, token_data)
        
        # Crea token JWT per il frontend
        jwt_token = create_access_token(
            data={
                "sub": user_info["user_id"],
                "hubspot_user_id": user_info["user_id"],
                "hubspot_portal_id": user_info["hub_id"],
                "scopes": HUBSPOT_SCOPES
            },
            expires_delta=timedelta(hours=24)
        )
        
        logger.info(f"Successfully authenticated user {user_info['user_id']}")
        
        # Redirect al frontend con token
        frontend_url = os.getenv("FRONTEND_URL", "http://localhost:3000")
        redirect_url = f"{frontend_url}/auth/success?token={jwt_token}"
        
        return RedirectResponse(url=redirect_url)
        
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Authentication failed: {str(e)}"}
        )


@router.post("/refresh", response_model=AuthResponse)
async def refresh_token(
    request: TokenRefreshRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Rinnova il token di accesso HubSpot.
    
    Args:
        request: Richiesta con refresh token
        db: Sessione database
    
    Returns:
        AuthResponse: Nuovi token
    """
    try:
        # Ottieni token dal database
        result = await db.execute(
            select(Token).where(Token.refresh_token == request.refresh_token)
        )
        token_record = result.scalar_one_or_none()
        
        if not token_record:
            raise HTTPException(status_code=401, detail="Invalid refresh token")
        
        # Scambia refresh token per nuovi token
        token_data = await refresh_hubspot_token(token_record.refresh_token)
        
        # Aggiorna token nel database
        token_record.access_token = token_data["access_token"]
        token_record.expires_at = datetime.utcnow() + timedelta(seconds=token_data["expires_in"])
        token_record.updated_at = datetime.utcnow()
        
        await db.commit()
        
        return AuthResponse(
            access_token=token_data["access_token"],
            refresh_token=token_data["refresh_token"],
            expires_in=token_data["expires_in"],
            token_type="Bearer"
        )
        
    except Exception as e:
        logger.error(f"Token refresh failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Token refresh failed")


@router.get("/status")
async def auth_status(
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """
    Verifica lo stato dell'autenticazione.
    
    Returns:
        dict: Stato autenticazione e informazioni utente
    """
    try:
        # Controlla se esiste un token valido
        result = await db.execute(
            select(Token).where(Token.expires_at > datetime.utcnow())
        )
        token_record = result.scalar_one_or_none()
        
        if not token_record:
            return {
                "authenticated": False,
                "message": "Nessun token valido trovato"
            }
        
        # Ottieni informazioni utente
        user_info = await get_hubspot_user_info(token_record.access_token)
        
        return {
            "authenticated": True,
            "user": {
                "user_id": user_info["user_id"],
                "email": user_info["user"],
                "hub_id": user_info["hub_id"],
                "scopes": HUBSPOT_SCOPES
            },
            "token_expires_at": token_record.expires_at.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Auth status check failed: {str(e)}")
        return {
            "authenticated": False,
            "error": str(e)
        }


@router.post("/logout")
async def logout(
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """
    Effettua il logout revocando il token.
    
    Returns:
        dict: Messaggio di conferma
    """
    try:
        # Recupera tutti i token dal database
        result = await db.execute(select(Token))
        tokens = result.scalars().all()
        
        # Importa HubSpotClient per la revoca
        from app.connectors.hubspot_client import HubSpotClient
        
        hubspot_client = HubSpotClient(db)
        
        for token in tokens:
            # Prova a revocare il refresh token su HubSpot
            # Se fallisce, procede comunque con l'eliminazione locale
            await hubspot_client.revoke_token(token.refresh_token)
            await db.delete(token)
        
        await db.commit()
        
        return {"message": "Logout effettuato con successo"}
        
    except Exception as e:
        logger.error(f"Logout failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Logout failed")


async def exchange_code_for_tokens(authorization_code: str) -> dict:
    """
    Scambia l'authorization code per i token di accesso.
    
    Args:
        authorization_code: Code ricevuto da HubSpot
    
    Returns:
        dict: Token data
    """
    if not all([HUBSPOT_CLIENT_ID, HUBSPOT_CLIENT_SECRET]):
        raise HTTPException(status_code=500, detail="HubSpot credentials not configured")
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            HUBSPOT_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "client_id": HUBSPOT_CLIENT_ID,
                "client_secret": HUBSPOT_CLIENT_SECRET,
                "redirect_uri": HUBSPOT_REDIRECT_URI,
                "code": authorization_code
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to exchange code for tokens: {response.text}"
            )
        
        return response.json()


async def refresh_hubspot_token(refresh_token: str) -> dict:
    """
    Rinnova il token di accesso usando il refresh token.
    
    Args:
        refresh_token: Refresh token valido
    
    Returns:
        dict: Nuovi token data
    """
    async with httpx.AsyncClient() as client:
        response = await client.post(
            HUBSPOT_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "client_id": HUBSPOT_CLIENT_ID,
                "client_secret": HUBSPOT_CLIENT_SECRET,
                "refresh_token": refresh_token
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to refresh token: {response.text}"
            )
        
        return response.json()


async def get_hubspot_user_info(access_token: str) -> dict:
    """
    Ottiene le informazioni utente da HubSpot.
    
    Args:
        access_token: Token di accesso valido
    
    Returns:
        dict: Informazioni utente
    """
    # Endpoint corretto per info utente HubSpot
    url = f"https://api.hubapi.com/oauth/v1/access-tokens/{access_token}"
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to get user info: {response.text}"
            )
        
        return response.json()


async def save_tokens_to_db(
    db: AsyncSession,
    user_info: dict,
    token_data: dict
):
    """
    Salva i token nel database.
    
    Args:
        db: Sessione database
        user_info: Informazioni utente
        token_data: Dati token
    """
    # Cancella token esistenti per lo stesso utente
    await db.execute(
        select(Token).where(Token.hubspot_user_id == user_info["user_id"])
    )
    
    # Crea nuovo record token
    expires_at = datetime.utcnow() + timedelta(seconds=token_data["expires_in"])
    
    new_token = Token(
        hubspot_user_id=user_info["user_id"],
        hubspot_portal_id=user_info["hub_id"],
        access_token=token_data["access_token"],
        refresh_token=token_data["refresh_token"],
        expires_at=expires_at,
        scopes=",".join(HUBSPOT_SCOPES)
    )
    
    db.add(new_token)
    await db.commit()