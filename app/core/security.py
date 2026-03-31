"""
Modulo sicurezza per OAuth 2.0 e gestione token.

Questo modulo fornisce funzionalità per:
- Creazione e verifica token JWT
- Gestione OAuth 2.0 con HubSpot
- Autenticazione e autorizzazione
"""

from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger()

# Configurazione sicurezza
security = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Configurazione JWT
JWT_SECRET_KEY = settings.jwt_secret_key if hasattr(settings, 'jwt_secret_key') else "your-secret-key-here"
JWT_ALGORITHM = "HS256"
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 30
JWT_REFRESH_TOKEN_EXPIRE_DAYS = 7

def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """
    Crea un token JWT di accesso.
    
    Args:
        data: Dati da includere nel token
        expires_delta: Durata personalizzata del token
        
    Returns:
        Token JWT codificato
    """
    try:
        to_encode = data.copy()
        
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=JWT_ACCESS_TOKEN_EXPIRE_MINUTES)
        
        to_encode.update({
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "access_token"
        })
        
        encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
        
        logger.info(f"Token di accesso creato per utente: {data.get('sub', 'unknown')}")
        return encoded_jwt
        
    except Exception as e:
        logger.error(f"Errore nella creazione token di accesso: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Errore nella creazione del token"
        )

def create_refresh_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """
    Crea un token JWT di refresh.
    
    Args:
        data: Dati da includere nel token
        expires_delta: Durata personalizzata del token
        
    Returns:
        Token JWT di refresh codificato
    """
    try:
        to_encode = data.copy()
        
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(days=JWT_REFRESH_TOKEN_EXPIRE_DAYS)
        
        to_encode.update({
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "refresh_token"
        })
        
        encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
        
        logger.info(f"Token di refresh creato per utente: {data.get('sub', 'unknown')}")
        return encoded_jwt
        
    except Exception as e:
        logger.error(f"Errore nella creazione token di refresh: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Errore nella creazione del token di refresh"
        )

def verify_token(token: str, token_type: str = "access_token") -> Dict[str, Any]:
    """
    Verifica e decodifica un token JWT.
    
    Args:
        token: Token JWT da verificare
        token_type: Tipo di token ("access_token" o "refresh_token")
        
    Returns:
        Dati decodificati dal token
        
    Raises:
        HTTPException: Se il token non è valido
    """
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        
        # Verifica tipo di token
        if payload.get("type") != token_type:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Tipo di token non valido: atteso {token_type}"
            )
        
        # Verifica scadenza
        exp = payload.get("exp")
        if exp is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token senza data di scadenza"
            )
        
        if datetime.utcnow() > datetime.fromtimestamp(exp):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token scaduto"
            )
        
        logger.debug(f"Token verificato con successo per utente: {payload.get('sub', 'unknown')}")
        return payload
        
    except JWTError as e:
        logger.error(f"Errore nella verifica token: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token non valido"
        )
    except Exception as e:
        logger.error(f"Errore imprevisto nella verifica token: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Errore nell'autenticazione"
        )

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    """
    Dependency per ottenere l'utente corrente dal token JWT.
    
    Args:
        credentials: Credenziali HTTP Bearer
        
    Returns:
        Dati dell'utente corrente
        
    Raises:
        HTTPException: Se l'utente non è autenticato
    """
    try:
        token = credentials.credentials
        payload = verify_token(token, "access_token")
        
        user_data = {
            "user_id": payload.get("sub"),
            "hubspot_user_id": payload.get("hubspot_user_id"),
            "hubspot_portal_id": payload.get("hubspot_portal_id"),
            "scopes": payload.get("scopes", []),
            "token_type": payload.get("type"),
            "expires_at": payload.get("exp")
        }
        
        logger.debug(f"Utente corrente estratto: {user_data.get('user_id')}")
        return user_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nell'estrazione utente corrente: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Errore nell'autenticazione"
        )

def hash_password(password: str) -> str:
    """
    Hash di una password.
    
    Args:
        password: Password in chiaro
        
    Returns:
        Password hashata
    """
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verifica una password.
    
    Args:
        plain_password: Password in chiaro
        hashed_password: Password hashata
        
    Returns:
        True se la password è corretta
    """
    return pwd_context.verify(plain_password, hashed_password)

def create_hubspot_oauth_url(state: Optional[str] = None) -> str:
    """
    Crea l'URL per l'autenticazione OAuth 2.0 con HubSpot.
    
    Args:
        state: Parametro state per sicurezza CSRF
        
    Returns:
        URL di autorizzazione HubSpot
    """
    try:
        from urllib.parse import urlencode
        
        # Configurazione OAuth
        client_id = settings.hubspot_client_id
        redirect_uri = settings.hubspot_redirect_uri
        
        if not client_id or not redirect_uri:
            logger.error("Configurazione OAuth HubSpot mancante")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Configurazione OAuth non disponibile"
            )
        
        # Scopes necessari
        scopes = [
            "crm.objects.deals.read",
            "crm.objects.deals.write",
            "crm.objects.contacts.read",
            "crm.objects.contacts.write",
            "crm.objects.companies.read",
            "timeline.events.read",
            "timeline.events.write",
            "engagements.read",
            "settings.user.read"
        ]
        
        # Parametri OAuth
        params = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": " ".join(scopes),
            "response_type": "code"
        }
        
        if state:
            params["state"] = state
        
        # Costruisci URL
        auth_url = f"https://app.hubspot.com/oauth/authorize?{urlencode(params)}"
        
        logger.info(f"URL OAuth HubSpot creato: {auth_url[:100]}...")
        return auth_url
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nella creazione URL OAuth: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Errore nella creazione URL di autorizzazione"
        )

async def exchange_hubspot_code_for_tokens(code: str) -> Dict[str, Any]:
    """
    Scambia il codice di autorizzazione per i token HubSpot.
    
    Args:
        code: Codice di autorizzazione da HubSpot
        
    Returns:
        Dati dei token (access_token, refresh_token, expires_in)
    """
    try:
        import httpx
        
        # Configurazione
        client_id = settings.hubspot_client_id
        client_secret = settings.hubspot_client_secret
        redirect_uri = settings.hubspot_redirect_uri
        
        if not all([client_id, client_secret, redirect_uri]):
            logger.error("Configurazione OAuth HubSpot incompleta")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Configurazione OAuth non disponibile"
            )
        
        # Richiesta token
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://api.hubapi.com/oauth/v1/token",
                data={
                    "grant_type": "authorization_code",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "redirect_uri": redirect_uri,
                    "code": code
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
            
            if response.status_code != 200:
                logger.error(f"Errore scambio token HubSpot: {response.status_code} - {response.text}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Errore nello scambio del codice di autorizzazione"
                )
            
            token_data = response.json()
            
            logger.info("Token HubSpot ottenuti con successo")
            return token_data
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nello scambio codice per token: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Errore nell'ottenimento dei token"
        )

async def refresh_hubspot_token(refresh_token: str) -> Dict[str, Any]:
    """
    Rinnova il token di accesso HubSpot.
    
    Args:
        refresh_token: Token di refresh
        
    Returns:
        Nuovi dati dei token
    """
    try:
        import httpx
        
        # Configurazione
        client_id = settings.hubspot_client_id
        client_secret = settings.hubspot_client_secret
        
        if not all([client_id, client_secret]):
            logger.error("Configurazione OAuth HubSpot incompleta")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Configurazione OAuth non disponibile"
            )
        
        # Richiesta refresh token
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://api.hubapi.com/oauth/v1/token",
                data={
                    "grant_type": "refresh_token",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "refresh_token": refresh_token
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
            
            if response.status_code != 200:
                logger.error(f"Errore refresh token HubSpot: {response.status_code} - {response.text}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Errore nel rinnovo del token"
                )
            
            token_data = response.json()
            
            logger.info("Token HubSpot rinnovato con successo")
            return token_data
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nel refresh token: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Errore nel rinnovo del token"
        )

async def get_hubspot_user_info(access_token: str) -> Dict[str, Any]:
    """
    Ottiene le informazioni utente da HubSpot.
    
    Args:
        access_token: Token di accesso
        
    Returns:
        Informazioni utente
    """
    try:
        import httpx
        
        # Richiesta informazioni utente
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "https://api.hubapi.com/oauth/v1/access-tokens",
                headers={"Authorization": f"Bearer {access_token}"}
            )
            
            if response.status_code != 200:
                logger.error(f"Errore ottenimento info utente HubSpot: {response.status_code} - {response.text}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Errore nell'ottenimento delle informazioni utente"
                )
            
            user_info = response.json()
            
            logger.info(f"Informazioni utente HubSpot ottenute: {user_info.get('user', 'unknown')}")
            return user_info
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nell'ottenimento info utente: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Errore nell'ottenimento delle informazioni utente"
        )

# Funzioni di utilità per validazione
def validate_token_scope(required_scope: str, token_scopes: list) -> bool:
    """
    Valida che il token abbia lo scope richiesto.
    
    Args:
        required_scope: Scope richiesto
        token_scopes: Scopes del token
        
    Returns:
        True se lo scope è presente
    """
    return required_scope in token_scopes

def validate_hubspot_permissions(required_permissions: list, user_permissions: list) -> bool:
    """
    Valida che l'utente abbia i permessi richiesti.
    
    Args:
        required_permissions: Permessi richiesti
        user_permissions: Permessi dell'utente
        
    Returns:
        True se tutti i permessi sono presenti
    """
    return all(perm in user_permissions for perm in required_permissions)

# Creazione istanza globale
security_instance = {
    "create_access_token": create_access_token,
    "create_refresh_token": create_refresh_token,
    "verify_token": verify_token,
    "get_current_user": get_current_user,
    "hash_password": hash_password,
    "verify_password": verify_password,
    "create_hubspot_oauth_url": create_hubspot_oauth_url,
    "exchange_hubspot_code_for_tokens": exchange_hubspot_code_for_tokens,
    "refresh_hubspot_token": refresh_hubspot_token,
    "get_hubspot_user_info": get_hubspot_user_info
}