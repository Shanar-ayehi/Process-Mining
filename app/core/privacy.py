import hashlib
import pandas as pd
from typing import Dict, List, Optional, Union
from datetime import datetime, timedelta
from app.core.logger import get_logger
from app.core.config import settings

logger = get_logger()

class PrivacyManager:
    """Gestore della privacy e GDPR per l'applicazione."""
    
    def __init__(self):
        self.salt = settings.email_hash_salt
        self.retention_days = settings.data_retention_days
        self.pseudonymization_enabled = settings.pseudonymization_enabled
    
    def hash_email(self, email: str) -> str:
        """
        Anonimizza l'email per conformità GDPR usando un hash MD5 con salt.
        
        Args:
            email: Email da anonimizzare
            
        Returns:
            Stringa hashata o "Unknown" se email è vuota
        """
        if not email or not email.strip():
            return "Unknown"
        
        # Normalizza l'email
        email_normalized = email.strip().lower()
        
        # Crea hash con salt per maggiore sicurezza
        hash_input = f"{email_normalized}{self.salt}"
        hash_value = hashlib.md5(hash_input.encode('utf-8')).hexdigest()
        
        return f"User_{hash_value[:16]}"  # Hash più lungo per unicità
    
    def pseudonymize_dataframe(self, df: pd.DataFrame, 
                             sensitive_columns: List[str] = None) -> pd.DataFrame:
        """
        Pseudonimizza colonne sensibili in un DataFrame.
        
        Args:
            df: DataFrame da pseudonimizzare
            sensitive_columns: Lista di colonne contenenti dati sensibili
            
        Returns:
            DataFrame pseudonimizzato
        """
        if not self.pseudonymization_enabled:
            logger.info("Pseudonimizzazione disabilitata, ritorno DataFrame originale")
            return df
        
        if sensitive_columns is None:
            sensitive_columns = ['user_email', 'email', 'contact_email']
        
        df_copy = df.copy()
        
        for column in sensitive_columns:
            if column in df_copy.columns:
                logger.info(f"Pseudonimizzazione colonna: {column}")
                df_copy[column] = df_copy[column].apply(self.hash_email)
        
        return df_copy
    
    def apply_data_retention(self, df: pd.DataFrame, 
                           date_column: str = 'timestamp') -> pd.DataFrame:
        """
        Applica la policy di retention ai dati.
        
        Args:
            df: DataFrame da filtrare
            date_column: Colonna contenente la data per il filtro
            
        Returns:
            DataFrame filtrato per retention policy
        """
        if date_column not in df.columns:
            logger.warning(f"Colonna {date_column} non trovata, retention non applicata")
            return df
        
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        
        # Converte la colonna in datetime se necessario
        df_filtered = df.copy()
        if not pd.api.types.is_datetime64_any_dtype(df_filtered[date_column]):
            df_filtered[date_column] = pd.to_datetime(df_filtered[date_column])
        
        # Filtra per data
        mask = df_filtered[date_column] >= cutoff_date
        filtered_count = len(df_filtered) - len(df_filtered[mask])
        
        if filtered_count > 0:
            logger.info(f"Applicata retention policy: rimossi {filtered_count} record")
        
        return df_filtered[mask]
    
    def mask_sensitive_values(self, value: str, 
                            preserve_chars: int = 2) -> str:
        """
        Maschera parzialmente un valore sensibile (es. numeri di telefono, ID).
        
        Args:
            value: Valore da mascherare
            preserve_chars: Numero di caratteri da preservare all'inizio e alla fine
            
        Returns:
            Valore mascherato
        """
        if not value or len(value) <= preserve_chars * 2:
            return "*" * len(value) if value else value
        
        # Preserva i primi e ultimi caratteri, maschera il resto
        start = value[:preserve_chars]
        end = value[-preserve_chars:]
        masked = "*" * (len(value) - preserve_chars * 2)
        
        return f"{start}{masked}{end}"
    
    def audit_data_access(self, operation: str, 
                         user_id: Optional[str] = None,
                         data_description: Optional[str] = None) -> None:
        """
        Registra l'accesso ai dati per audit GDPR.
        
        Args:
            operation: Tipo di operazione (read, write, delete, etc.)
            user_id: ID utente che ha effettuato l'operazione
            data_description: Descrizione dei dati coinvolti
        """
        audit_entry = {
            'timestamp': datetime.now().isoformat(),
            'operation': operation,
            'user_id': user_id or 'system',
            'data_description': data_description or 'unknown',
            'environment': settings.environment
        }
        
        logger.info(f"Audit GDPR: {audit_entry}")

# Creazione istanza globale
privacy_manager = PrivacyManager()

# Funzioni helper per compatibilità con codice esistente
def hash_email(email: str) -> str:
    """Funzione helper per compatibilità con codice esistente."""
    return privacy_manager.hash_email(email)

def pseudonymize_dataframe(df: pd.DataFrame, 
                          sensitive_columns: List[str] = None) -> pd.DataFrame:
    """Funzione helper per compatibilità con codice esistente."""
    return privacy_manager.pseudonymize_dataframe(df, sensitive_columns)