"""
Schemi Pydantic per l'autenticazione OAuth 2.0

Questi schemi definiscono la struttura dei dati per le richieste
e risposte relative all'autenticazione HubSpot.
"""

from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class TokenType(str, Enum):
    """Tipi di token supportati."""
    BEARER = "Bearer"
    MAC = "MAC"


class AuthResponse(BaseModel):
    """Risposta standard per le operazioni di autenticazione."""
    access_token: str = Field(..., description="Token di accesso JWT")
    refresh_token: str = Field(..., description="Token di refresh")
    expires_in: int = Field(..., description="Tempo di scadenza in secondi")
    token_type: TokenType = Field(default=TokenType.BEARER, description="Tipo di token")
    scope: Optional[str] = Field(None, description="Scope concessi")


class TokenRefreshRequest(BaseModel):
    """Richiesta di refresh token."""
    refresh_token: str = Field(..., description="Token di refresh da utilizzare")


class TokenInfo(BaseModel):
    """Informazioni su un token."""
    token_type: TokenType
    expires_at: datetime
    scopes: List[str]
    is_active: bool
    created_at: datetime
    updated_at: Optional[datetime] = None


class UserInfo(BaseModel):
    """Informazioni sull'utente autenticato."""
    user_id: str = Field(..., description="ID utente HubSpot")
    email: Optional[EmailStr] = Field(None, description="Email utente")
    name: Optional[str] = Field(None, description="Nome utente")
    hub_id: str = Field(..., description="ID portal HubSpot")
    scopes: List[str] = Field(default=[], description="Scope concessi")
    is_active: bool = Field(default=True, description="Stato attivo utente")


class AuthStatus(BaseModel):
    """Stato dell'autenticazione."""
    authenticated: bool = Field(..., description="Stato autenticazione")
    user: Optional[UserInfo] = Field(None, description="Informazioni utente")
    token_expires_at: Optional[str] = Field(None, description="Data scadenza token")
    message: Optional[str] = Field(None, description="Messaggio informativo")


class AuthError(BaseModel):
    """Errore di autenticazione."""
    error: str = Field(..., description="Codice errore")
    error_description: Optional[str] = Field(None, description="Descrizione errore")
    error_uri: Optional[str] = Field(None, description="URL documentazione errore")


class OAuthConfig(BaseModel):
    """Configurazione OAuth 2.0."""
    client_id: str = Field(..., description="Client ID HubSpot")
    client_secret: str = Field(..., description="Client Secret HubSpot")
    redirect_uri: str = Field(..., description="Redirect URI per OAuth")
    scopes: List[str] = Field(..., description="Scope richiesti")
    auth_url: str = Field(..., description="URL autorizzazione HubSpot")
    token_url: str = Field(..., description="URL token HubSpot")


class SessionInfo(BaseModel):
    """Informazioni sulla sessione di autenticazione."""
    session_token: str = Field(..., description="Token di sessione")
    user_id: str = Field(..., description="ID utente HubSpot")
    hub_id: str = Field(..., description="ID portal HubSpot")
    expires_at: datetime = Field(..., description="Data scadenza sessione")
    last_activity: Optional[datetime] = Field(None, description="Ultima attività")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Metadati sessione")


class AuthInitiateResponse(BaseModel):
    """Risposta per l'inizio flusso OAuth."""
    auth_url: str = Field(..., description="URL di autorizzazione HubSpot")
    message: str = Field(..., description="Messaggio informativo")


class AuthCallbackResponse(BaseModel):
    """Risposta per il callback OAuth."""
    success: bool = Field(..., description="Esito callback")
    user: Optional[UserInfo] = Field(None, description="Informazioni utente")
    redirect_url: Optional[str] = Field(None, description="URL di redirect")
    error: Optional[str] = Field(None, description="Messaggio errore")


class LogoutResponse(BaseModel):
    """Risposta per il logout."""
    message: str = Field(..., description="Messaggio di conferma logout")


class TokenValidationRequest(BaseModel):
    """Richiesta validazione token."""
    token: str = Field(..., description="Token da validare")


class TokenValidationResponse(BaseModel):
    """Risposta validazione token."""
    valid: bool = Field(..., description="Validità token")
    user_id: Optional[str] = Field(None, description="ID utente associato")
    expires_at: Optional[datetime] = Field(None, description="Data scadenza")
    scopes: List[str] = Field(default=[], description="Scope concessi")
    error: Optional[str] = Field(None, description="Messaggio errore")