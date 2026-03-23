import duckdb
import polars as pl
from pathlib import Path
from app.core.logger import get_logger

logger = get_logger()

# Definiamo dove verrà salvato il file del database
DB_PATH = "data/process_mining.db"

def get_db_connection():
    """Restituisce una connessione al database locale DuckDB."""
    # Crea la cartella data se non esiste
    Path("data").mkdir(parents=True, exist_ok=True)
    return duckdb.connect(DB_PATH)

def save_event_log(df: pl.DataFrame, portal_id: str, table_name: str = None):
    """
    Salva un Polars DataFrame in una tabella DuckDB con isolamento multi-tenant.
    
    Args:
        df: DataFrame Polars da salvare
        portal_id: ID del portale HubSpot (obbligatorio per multi-tenancy)
        table_name: Nome personalizzato della tabella (opzionale, default: event_log_{portal_id})
    """
    # Genera nome tabella dinamico basato su portal_id se non specificato
    if table_name is None:
        table_name = f"event_log_{portal_id}"
    
    logger.info(f"Salvataggio di {len(df)} righe nella tabella '{table_name}' per portal_id '{portal_id}'...")
    
    conn = get_db_connection()
    try:
        # DuckDB è magico: può leggere la variabile 'df' direttamente da Python!
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
        
        # Se la tabella esiste già, potremmo volerla sovrascrivere o aggiornare. 
        # Per semplicità ora la sovrascriviamo:
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        
        logger.info(f"Salvataggio completato con successo per portal_id '{portal_id}'.")
    except Exception as e:
        logger.error(f"Errore durante il salvataggio nel DB per portal_id '{portal_id}': {e}")
        raise
    finally:
        conn.close()

def load_event_log(portal_id: str, table_name: str = None) -> pl.DataFrame:
    """
    Carica la tabella dal database con isolamento multi-tenant.
    
    Args:
        portal_id: ID del portale HubSpot (obbligatorio per multi-tenancy)
        table_name: Nome personalizzato della tabella (opzionale, default: event_log_{portal_id})
        
    Returns:
        Polars DataFrame con i dati del portale specificato
    """
    # Genera nome tabella dinamico basato su portal_id se non specificato
    if table_name is None:
        table_name = f"event_log_{portal_id}"
    
    conn = get_db_connection()
    try:
        logger.info(f"Lettura della tabella '{table_name}' per portal_id '{portal_id}' dal DB...")
        # Leggiamo con SQL e convertiamo direttamente in Polars (.pl())
        df = conn.sql(f"SELECT * FROM {table_name}").pl()
        logger.info(f"Lettura completata: {len(df)} righe caricate per portal_id '{portal_id}'.")
        return df
    except Exception as e:
        logger.error(f"Errore durante la lettura dal DB per portal_id '{portal_id}' (la tabella esiste?): {e}")
        return pl.DataFrame()
    finally:
        conn.close()

# Import SQLAlchemy per la funzione get_db
try:
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker, declarative_base
    from sqlalchemy import text
    
    # Configurazione SQLAlchemy per database relazionale (se necessario)
    # Per ora usiamo SQLite in memoria per la gestione OAuth
    SQLALCHEMY_DATABASE_URL = "sqlite+aiosqlite:///./data/oauth.db"
    
    # Crea il motore asincrono
    engine = create_async_engine(
        SQLALCHEMY_DATABASE_URL,
        echo=False,  # Imposta a True per vedere le query SQL
        connect_args={"check_same_thread": False}  # Solo per SQLite
    )
    
    # Crea la session factory
    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    
    # Crea la base declarativa per i modelli SQLAlchemy
    Base = declarative_base()
    
    async def get_db():
        """
        Dependency injection per ottenere una sessione database asincrona.
        
        Returns:
            AsyncSession: Sessione database asincrona
        """
        async with async_session() as session:
            try:
                # Testiamo la connessione
                await session.execute(text("SELECT 1"))
                yield session
            except Exception as e:
                logger.error(f"Errore nella sessione database: {e}")
                await session.rollback()
                raise
            finally:
                await session.close()
                
except ImportError:
    logger.warning("SQLAlchemy non disponibile, la funzione get_db non sarà accessibile")
    
    async def get_db():
        """Placeholder se SQLAlchemy non è disponibile."""
        raise ImportError("SQLAlchemy non è installato. Installare con: pip install sqlalchemy aiosqlite")
    
    # Placeholder per Base se SQLAlchemy non è disponibile
    class Base:
        """Placeholder per Base se SQLAlchemy non è disponibile."""
        pass
