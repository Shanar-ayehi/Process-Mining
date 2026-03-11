from typing import List, Dict, Any, Optional, Union
import polars as pl
from datetime import datetime
from app.core.logger import get_logger
from app.core.config import settings
from app.connectors.hubspot_mapper import hubspot_mapper
from app.core.privacy import privacy_manager

logger = get_logger()

class DataTransformationService:
    """Servizio per la trasformazione e normalizzazione dei dati estratti."""
    
    def __init__(self):
        self.processed_dir = settings.processed_data_dir
    
    def transform_hubspot_deals_to_event_log(self, deals_data: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Trasforma i deal HubSpot in un event log standardizzato.
        
        Args:
            deals_data: Dati deal da HubSpot
            
        Returns:
            DataFrame Polars pronto per Process Mining
        """
        logger.info("Inizio trasformazione deal HubSpot in event log")
        
        try:
            # Usa il mapper per la trasformazione
            df = hubspot_mapper.map_multiple_deals_to_dataframe(deals_data)
            
            if df.is_empty():
                logger.warning("Nessun evento trovato nella trasformazione")
                return df
            
            # Applica pseudonimizzazione se abilitata
            if settings.pseudonymization_enabled:
                df = self._apply_pseudonymization(df)
            
            # Aggiungi colonne di audit
            df = self._add_audit_columns(df)
            
            # Salva il DataFrame trasformato
            self._save_processed_data(df, "event_log")
            
            logger.info(f"Trasformazione completata: {len(df)} eventi generati")
            return df
            
        except Exception as e:
            logger.error(f"Errore nella trasformazione deal: {e}")
            raise
    
    def transform_contacts_to_entities(self, contacts_data: List[Dict[str, Any]]) -> pl.DataFrame:
        """Trasforma contatti in entità per l'integrazione."""
        logger.info("Inizio trasformazione contatti in entità")
        
        try:
            entities = []
            for contact in contacts_data:
                mapped_contact = hubspot_mapper.map_contact_to_entity(contact)
                entities.append(mapped_contact)
            
            if not entities:
                logger.warning("Nessun contatto trovato nella trasformazione")
                return pl.DataFrame()
            
            df = pl.DataFrame(entities)
            
            # Applica pseudonimizzazione
            if settings.pseudonymization_enabled:
                df = self._apply_pseudonymization_to_entities(df, ['email'])
            
            # Salva i dati trasformati
            self._save_processed_data(df, "contacts_entities")
            
            logger.info(f"Trasformazione contatti completata: {len(df)} entità generate")
            return df
            
        except Exception as e:
            logger.error(f"Errore nella trasformazione contatti: {e}")
            raise
    
    def transform_companies_to_entities(self, companies_data: List[Dict[str, Any]]) -> pl.DataFrame:
        """Trasforma aziende in entità per l'integrazione."""
        logger.info("Inizio trasformazione aziende in entità")
        
        try:
            entities = []
            for company in companies_data:
                mapped_company = hubspot_mapper.map_company_to_entity(company)
                entities.append(mapped_company)
            
            if not entities:
                logger.warning("Nessuna azienda trovata nella trasformazione")
                return pl.DataFrame()
            
            df = pl.DataFrame(entities)
            
            # Salva i dati trasformati
            self._save_processed_data(df, "companies_entities")
            
            logger.info(f"Trasformazione aziende completata: {len(df)} entità generate")
            return df
            
        except Exception as e:
            logger.error(f"Errore nella trasformazione aziende: {e}")
            raise
    
    def merge_multiple_sources(self, event_log: pl.DataFrame, 
                             contacts_entities: Optional[pl.DataFrame] = None,
                             companies_entities: Optional[pl.DataFrame] = None) -> pl.DataFrame:
        """
        Fonde dati da più sorgenti in un unico dataset arricchito.
        
        Args:
            event_log: Event log principale
            contacts_entities: Entità contatti
            companies_entities: Entità aziende
            
        Returns:
            DataFrame fuso arricchito
        """
        logger.info("Inizio fusione dati da multiple sorgenti")
        
        try:
            merged_df = event_log.clone()
            
            # Aggiungi informazioni sui contatti se disponibili
            if contacts_entities is not None and not contacts_entities.is_empty():
                # Join temporaneo per arricchire l'event log
                # Nota: Questo è un esempio semplificato, in produzione servirebbe
                # una logica di join più sofisticata basata su relazioni reali
                merged_df = merged_df.with_columns([
                    pl.lit("Contact_Info_Available").alias("contact_info")
                ])
            
            # Aggiungi informazioni sulle aziende se disponibili
            if companies_entities is not None and not companies_entities.is_empty():
                merged_df = merged_df.with_columns([
                    pl.lit("Company_Info_Available").alias("company_info")
                ])
            
            # Salva il dataset fuso
            self._save_processed_data(merged_df, "merged_dataset")
            
            logger.info(f"Fusione completata: {len(merged_df)} record arricchiti")
            return merged_df
            
        except Exception as e:
            logger.error(f"Errore nella fusione dati: {e}")
            raise
    
    def normalize_timestamps(self, df: pl.DataFrame, timestamp_column: str = "timestamp") -> pl.DataFrame:
        """Normalizza i timestamp in un DataFrame."""
        logger.info(f"Normalizzazione timestamp nella colonna: {timestamp_column}")
        
        try:
            if timestamp_column not in df.columns:
                logger.warning(f"Colonna {timestamp_column} non trovata, nessuna normalizzazione effettuata")
                return df
            
            # Assicurati che la colonna sia di tipo Datetime
            df_normalized = df.with_columns([
                pl.col(timestamp_column).cast(pl.Datetime).alias(timestamp_column)
            ])
            
            logger.info("Normalizzazione timestamp completata")
            return df_normalized
            
        except Exception as e:
            logger.error(f"Errore nella normalizzazione timestamp: {e}")
            raise
    
    def clean_data_quality_issues(self, df: pl.DataFrame) -> pl.DataFrame:
        """Pulisce problemi comuni di qualità dato."""
        logger.info("Inizio pulizia problemi qualità dato")
        
        try:
            df_cleaned = df.clone()
            
            # Rimuovi duplicati
            initial_count = len(df_cleaned)
            df_cleaned = df_cleaned.unique()
            duplicates_removed = initial_count - len(df_cleaned)
            
            if duplicates_removed > 0:
                logger.info(f"Rimossi {duplicates_removed} record duplicati")
            
            # Rimuovi record con case_id mancante
            df_cleaned = df_cleaned.filter(pl.col("case_id").is_not_null())
            
            # Rimuovi record con timestamp mancante
            if "timestamp" in df_cleaned.columns:
                df_cleaned = df_cleaned.filter(pl.col("timestamp").is_not_null())
            
            # Rimuovi record con activity mancante
            df_cleaned = df_cleaned.filter(pl.col("activity").is_not_null())
            
            logger.info(f"Pulizia qualità dato completata: {len(df_cleaned)} record rimanenti")
            return df_cleaned
            
        except Exception as e:
            logger.error(f"Errore nella pulizia qualità dato: {e}")
            raise
    
    def _apply_pseudonymization(self, df: pl.DataFrame) -> pl.DataFrame:
        """Applica pseudonimizzazione alle colonne sensibili."""
        logger.info("Applicazione pseudonimizzazione ai dati")
        
        sensitive_columns = ['resource', 'email']
        df_pseudonymized = df.clone()
        
        for column in sensitive_columns:
            if column in df_pseudonymized.columns:
                df_pseudonymized = df_pseudonymized.with_columns([
                    pl.col(column).apply(lambda x: privacy_manager.hash_email(str(x))).alias(column)
                ])
        
        return df_pseudonymized
    
    def _apply_pseudonymization_to_entities(self, df: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
        """Applica pseudonimizzazione a colonne specifiche in entità."""
        df_pseudonymized = df.clone()
        
        for column in columns:
            if column in df_pseudonymized.columns:
                df_pseudonymized = df_pseudonymized.with_columns([
                    pl.col(column).apply(lambda x: privacy_manager.hash_email(str(x))).alias(column)
                ])
        
        return df_pseudonymized
    
    def _add_audit_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aggiunge colonne di audit al DataFrame."""
        current_time = datetime.now().isoformat()
        
        return df.with_columns([
            pl.lit(current_time).alias("transformed_at"),
            pl.lit(settings.environment).alias("environment")
        ])
    
    def _save_processed_data(self, df: pl.DataFrame, filename_prefix: str) -> None:
        """Salva i dati processati in formato Parquet."""
        import os
        
        # Crea la directory se non esiste
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.parquet"
        filepath = self.processed_dir / filename
        
        # Salva in formato Parquet
        df.write_parquet(str(filepath))
        logger.info(f"Dati processati salvati in: {filepath}")

# Creazione istanza globale
data_transformation_service = DataTransformationService()