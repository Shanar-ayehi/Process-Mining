from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta
from pathlib import Path
import json
import polars as pl
from app.core.logger import get_logger
from app.core.config import settings
from app.core.privacy import privacy_manager

logger = get_logger()

class PrivacyGovernanceService:
    """Servizio per la governance della privacy e GDPR."""
    
    def __init__(self):
        self.logs_dir = settings.logs_dir
        self.retention_days = settings.data_retention_days
        self.pseudonymization_enabled = settings.pseudonymization_enabled
    
    def apply_data_retention_policy(self, data_dir: Optional[Path] = None) -> Dict[str, int]:
        """
        Applica la policy di retention ai dati.
        
        Args:
            data_dir: Directory da cui applicare la retention
            
        Returns:
            Statistiche sull'applicazione della retention
        """
        if data_dir is None:
            data_dir = settings.data_dir
        
        logger.info(f"Applicazione policy retention ({self.retention_days} giorni) su {data_dir}")
        
        stats = {
            "files_processed": 0,
            "files_deleted": 0,
            "total_size_freed": 0
        }
        
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        
        for file_path in data_dir.rglob("*"):
            if file_path.is_file():
                stats["files_processed"] += 1
                
                # Controlla la data di modifica
                file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                
                if file_mtime < cutoff_date:
                    try:
                        file_size = file_path.stat().st_size
                        file_path.unlink()
                        stats["files_deleted"] += 1
                        stats["total_size_freed"] += file_size
                        logger.info(f"Eliminato file vecchio: {file_path}")
                    except Exception as e:
                        logger.error(f"Errore nell'eliminazione file {file_path}: {e}")
        
        logger.info(f"Retention policy completata: {stats['files_deleted']} file eliminati")
        return stats
    
    def audit_data_access(self, operation: str, user_id: Optional[str] = None,
                         data_description: Optional[str] = None,
                         sensitive_data_accessed: bool = False) -> None:
        """
        Registra l'accesso ai dati per audit GDPR.
        
        Args:
            operation: Tipo di operazione
            user_id: ID utente
            data_description: Descrizione dati
            sensitive_data_accessed: Se sono stati accessati dati sensibili
        """
        audit_entry = {
            'timestamp': datetime.now().isoformat(),
            'operation': operation,
            'user_id': user_id or 'system',
            'data_description': data_description or 'unknown',
            'environment': settings.environment,
            'sensitive_data_accessed': sensitive_data_accessed,
            'pseudonymization_applied': self.pseudonymization_enabled
        }
        
        # Salva audit log
        audit_file = self.logs_dir / "privacy_audit.log"
        
        try:
            with open(audit_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(audit_entry, ensure_ascii=False) + '\n')
        except Exception as e:
            logger.error(f"Errore nella scrittura audit log: {e}")
        
        logger.info(f"Audit GDPR registrato: {audit_entry}")
    
    def generate_privacy_report(self) -> Dict[str, Any]:
        """
        Genera un report sulla governance della privacy.
        
        Returns:
            Report privacy
        """
        logger.info("Generazione report privacy")
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'environment': settings.environment,
            'pseudonymization_enabled': self.pseudonymization_enabled,
            'data_retention_days': self.retention_days,
            'directories_monitored': [],
            'files_status': {},
            'recent_audits': []
        }
        
        # Controlla stato directory
        for dir_name in ['raw', 'staged', 'processed', 'warehouse']:
            dir_path = settings.data_dir / dir_name
            if dir_path.exists():
                report['directories_monitored'].append(str(dir_path))
                
                file_count = len(list(dir_path.rglob('*')))
                report['files_status'][dir_name] = {
                    'total_files': file_count,
                    'last_modified': self._get_latest_file_date(dir_path)
                }
        
        # Leggi ultimi audit
        audit_file = self.logs_dir / "privacy_audit.log"
        if audit_file.exists():
            try:
                with open(audit_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    # Prendi ultimi 10 audit
                    recent_audits = [json.loads(line.strip()) for line in lines[-10:]]
                    report['recent_audits'] = recent_audits
            except Exception as e:
                logger.error(f"Errore nella lettura audit log: {e}")
        
        # Salva report
        report_file = self.logs_dir / f"privacy_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            logger.info(f"Report privacy salvato in: {report_file}")
        except Exception as e:
            logger.error(f"Errore nel salvataggio report privacy: {e}")
        
        return report
    
    def anonymize_dataframe(self, df: pl.DataFrame, 
                          sensitive_columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Anonimizza un DataFrame applicando privacy.
        
        Args:
            df: DataFrame da anonimizzare
            sensitive_columns: Colonne sensibili
            
        Returns:
            DataFrame anonimizzato
        """
        if not self.pseudonymization_enabled:
            logger.info("Pseudonimizzazione disabilitata, ritorno DataFrame originale")
            return df
        
        if sensitive_columns is None:
            sensitive_columns = ['resource', 'email', 'user_email']
        
        logger.info(f"Applicazione pseudonimizzazione a colonne: {sensitive_columns}")
        
        df_anonymized = df.clone()
        
        for column in sensitive_columns:
            if column in df_anonymized.columns:
                df_anonymized = df_anonymized.with_columns([
                    pl.col(column).apply(lambda x: privacy_manager.hash_email(str(x))).alias(column)
                ])
        
        # Registra l'operazione di anonimizzazione
        self.audit_data_access(
            operation="anonymization",
            data_description=f"DataFrame with {len(df_anonymized)} rows",
            sensitive_data_accessed=True
        )
        
        return df_anonymized
    
    def validate_gdpr_compliance(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Valida la compliance GDPR di un DataFrame.
        
        Args:
            df: DataFrame da validare
            
        Returns:
            Report di compliance
        """
        logger.info("Validazione compliance GDPR")
        
        compliance_report = {
            'timestamp': datetime.now().isoformat(),
            'total_rows': len(df),
            'sensitive_columns_found': [],
            'pseudonymization_status': {},
            'compliance_score': 0.0,
            'issues': []
        }
        
        # Colonne potenzialmente sensibili
        sensitive_patterns = ['email', 'user', 'contact', 'name', 'phone', 'address']
        
        for column in df.columns:
            column_lower = column.lower()
            
            # Controlla se è una colonna sensibile
            is_sensitive = any(pattern in column_lower for pattern in sensitive_patterns)
            
            if is_sensitive:
                compliance_report['sensitive_columns_found'].append(column)
                
                # Controlla se è già pseudonimizzata
                sample_values = df.select(pl.col(column)).head(10).to_series().to_list()
                is_pseudonymized = all(self._looks_pseudonymized(str(val)) for val in sample_values if val)
                
                compliance_report['pseudonymization_status'][column] = is_pseudonymized
                
                if not is_pseudonymized:
                    compliance_report['issues'].append(f"Colonna {column} non pseudonimizzata")
        
        # Calcola punteggio compliance
        total_sensitive = len(compliance_report['sensitive_columns_found'])
        if total_sensitive > 0:
            pseudonymized_count = sum(compliance_report['pseudonymization_status'].values())
            compliance_report['compliance_score'] = pseudonymized_count / total_sensitive
        
        # Registra validazione
        self.audit_data_access(
            operation="gdpr_validation",
            data_description=f"DataFrame validation - score: {compliance_report['compliance_score']:.2f}",
            sensitive_data_accessed=True
        )
        
        logger.info(f"Validazione GDPR completata - punteggio: {compliance_report['compliance_score']:.2f}")
        return compliance_report
    
    def _get_latest_file_date(self, directory: Path) -> Optional[str]:
        """Ottiene la data dell'ultimo file modificato in una directory."""
        try:
            files = list(directory.rglob('*'))
            if not files:
                return None
            
            latest_file = max(files, key=lambda f: f.stat().st_mtime)
            return datetime.fromtimestamp(latest_file.stat().st_mtime).isoformat()
        except Exception:
            return None
    
    def _looks_pseudonymized(self, value: str) -> bool:
        """Controlla se un valore sembra pseudonimizzato."""
        if not value or len(value) < 10:
            return False
        
        # Controlla pattern tipici di hash
        import re
        hash_patterns = [
            r'^User_[a-f0-9]{8}$',  # Hash MD5 corto
            r'^User_[a-f0-9]{16}$', # Hash MD5 lungo
            r'^[a-f0-9]{32}$',      # Hash puro
            r'^[a-f0-9]{64}$'       # Hash SHA-256
        ]
        
        return any(re.match(pattern, value) for pattern in hash_patterns)

# Creazione istanza globale
privacy_governance_service = PrivacyGovernanceService()