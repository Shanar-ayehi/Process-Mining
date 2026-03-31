"""
Modelli SQLAlchemy per l'autenticazione OAuth 2.0

Questi modelli gestiscono i token di accesso e refresh per l'integrazione HubSpot.
"""

from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, JSON
from sqlalchemy.sql import func
from datetime import datetime
from typing import Dict, Any

from app.core.database import Base


class User(Base):
    """
    Modello per gli utenti autenticati.
    """
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    hubspot_user_id = Column(String(255), unique=True, index=True, nullable=False)
    hubspot_portal_id = Column(String(255), nullable=False)
    email = Column(String(255), nullable=True)
    name = Column(String(255), nullable=True)
    scopes = Column(Text, nullable=True)  # Scopes concessi
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    def __repr__(self):
        return f"<User(id={self.id}, hubspot_user_id={self.hubspot_user_id}, email={self.email})>"


class Token(Base):
    """
    Modello per i token OAuth 2.0.
    """
    __tablename__ = "tokens"
    
    id = Column(Integer, primary_key=True, index=True)
    hubspot_user_id = Column(String(255), index=True, nullable=False)
    hubspot_portal_id = Column(String(255), nullable=False)
    access_token = Column(Text, nullable=False)
    refresh_token = Column(Text, nullable=False)
    token_type = Column(String(50), default="Bearer")
    expires_at = Column(DateTime(timezone=True), nullable=False)
    scopes = Column(Text, nullable=True)  # Scopes concessi
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    def is_expired(self) -> bool:
        """Controlla se il token è scaduto."""
        return datetime.utcnow() >= self.expires_at
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte il token in dizionario."""
        return {
            "id": self.id,
            "hubspot_user_id": self.hubspot_user_id,
            "hubspot_portal_id": self.hubspot_portal_id,
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "token_type": self.token_type,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "scopes": self.scopes.split(",") if self.scopes else [],
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
    
    def __repr__(self):
        return f"<Token(id={self.id}, user_id={self.hubspot_user_id}, expires_at={self.expires_at})>"


class AuthSession(Base):
    """
    Modello per le sessioni di autenticazione.
    """
    __tablename__ = "auth_sessions"
    
    id = Column(Integer, primary_key=True, index=True)
    session_token = Column(String(255), unique=True, index=True, nullable=False)
    hubspot_user_id = Column(String(255), nullable=False)
    hubspot_portal_id = Column(String(255), nullable=False)
    jwt_token = Column(Text, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    session_metadata = Column(JSON, nullable=True)  # Dati aggiuntivi della sessione
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_activity = Column(DateTime(timezone=True), onupdate=func.now())
    
    def is_expired(self) -> bool:
        """Controlla se la sessione è scaduta."""
        return datetime.utcnow() >= self.expires_at
    
    def update_activity(self):
        """Aggiorna l'ultima attività della sessione."""
        self.last_activity = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte la sessione in dizionario."""
        return {
            "id": self.id,
            "session_token": self.session_token,
            "hubspot_user_id": self.hubspot_user_id,
            "hubspot_portal_id": self.hubspot_portal_id,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "metadata": self.session_metadata,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "last_activity": self.last_activity.isoformat() if self.last_activity else None
        }
    
    def __repr__(self):
        return f"<AuthSession(id={self.id}, user_id={self.hubspot_user_id}, expires_at={self.expires_at})>"