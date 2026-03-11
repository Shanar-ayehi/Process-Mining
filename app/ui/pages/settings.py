import streamlit as st
import json
import os
from typing import Dict, Any, Optional
from pathlib import Path

def create_system_configuration():
    """Crea la sezione configurazione di sistema."""
    st.subheader("⚙️ Configurazione Sistema")
    
    # System settings form
    with st.form("system_settings"):
        st.markdown("### Impostazioni Generali")
        
        col1, col2 = st.columns(2)
        
        with col1:
            system_name = st.text_input("Nome Sistema", value="Process Mining System")
            timezone = st.selectbox("Fuso Orario", ["Europe/Rome", "UTC", "Europe/London", "America/New_York"])
            language = st.selectbox("Lingua", ["Italiano", "English", "Français", "Deutsch"])
        
        with col2:
            debug_mode = st.checkbox("Modalità Debug", value=False)
            auto_save = st.checkbox("Salvataggio Automatico", value=True)
            backup_enabled = st.checkbox("Backup Abilitato", value=True)
        
        st.markdown("### Limiti Sistema")
        col1, col2 = st.columns(2)
        
        with col1:
            max_concurrent_jobs = st.number_input("Job Concorrenti Max", min_value=1, max_value=100, value=10)
            max_memory_usage = st.number_input("Uso Massimo Memoria (GB)", min_value=1, max_value=128, value=16)
        
        with col2:
            max_file_size = st.number_input("Dimensione Max File (MB)", min_value=1, max_value=1000, value=100)
            retention_days = st.number_input("Giorni Retention Dati", min_value=1, max_value=3650, value=365)
        
        if st.form_submit_button("Salva Configurazione", type="primary"):
            st.success("Configurazione di sistema salvata con successo!")

def create_data_sources():
    """Crea la sezione sorgenti dati."""
    st.subheader("📊 Configurazione Sorgenti Dati")
    
    # Data sources configuration
    data_sources = st.session_state.get('data_sources', [
        {
            "name": "HubSpot CRM",
            "type": "CRM",
            "status": "Active",
            "connection_string": "hubspot://api.hubspot.com",
            "last_sync": "2 ore fa",
            "sync_frequency": "30 min"
        },
        {
            "name": "Database Warehouse",
            "type": "Database",
            "status": "Active",
            "connection_string": "postgresql://localhost:5432/warehouse",
            "last_sync": "1 ora fa",
            "sync_frequency": "1 ora"
        }
    ])
    
    for i, source in enumerate(data_sources):
        with st.expander(f"🔗 {source['name']} ({source['type']})"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Stato:** {source['status']}")
                st.write(f"**Ultimo Sync:** {source['last_sync']}")
                st.write(f"**Frequenza:** {source['sync_frequency']}")
            
            with col2:
                if st.button(f"🔄 Test Connessione", key=f"test_{i}"):
                    st.success(f"Connessione a {source['name']} riuscita!")
                
                if st.button(f"🗑️ Rimuovi", key=f"remove_{i}"):
                    st.warning(f"Sorgente dati {source['name']} rimossa!")

def create_privacy_settings():
    """Crea la sezione privacy e sicurezza."""
    st.subheader("🔒 Privacy e Sicurezza")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Crittografia Dati")
        encryption_enabled = st.checkbox("Crittografia Abilitata", value=True)
        encryption_algorithm = st.selectbox("Algoritmo Crittografia", ["AES-256", "RSA-2048", "ChaCha20"])
        
        st.markdown("### Anonimizzazione")
        anonymization_enabled = st.checkbox("Anonimizzazione Abilitata", value=True)
        anonymization_level = st.selectbox("Livello Anonimizzazione", ["Low", "Medium", "High"])
    
    with col2:
        st.markdown("### Accesso Dati")
        access_logging = st.checkbox("Logging Accessi Abilitato", value=True)
        audit_trail = st.checkbox("Audit Trail Abilitato", value=True)
        
        st.markdown("### Conservazione")
        data_retention = st.number_input("Periodo Conservazione (anni)", min_value=1, max_value=10, value=7)
        automatic_deletion = st.checkbox("Cancellazione Automatica Abilitata", value=True)
    
    if st.button("Salva Impostazioni Privacy", type="primary"):
        st.success("Impostazioni privacy salvate con successo!")

def create_notification_settings():
    """Crea la sezione notifiche."""
    st.subheader("📢 Impostazioni Notifiche")
    
    notification_types = [
        {"type": "Data Quality Issues", "enabled": True, "channels": ["Email", "Dashboard"]},
        {"type": "Process Anomalies", "enabled": True, "channels": ["Email", "SMS", "Dashboard"]},
        {"type": "System Alerts", "enabled": True, "channels": ["Email", "Dashboard"]},
        {"type": "Performance Warnings", "enabled": False, "channels": ["Dashboard"]},
        {"type": "Backup Status", "enabled": True, "channels": ["Email"]}
    ]
    
    for notification in notification_types:
        with st.expander(f"🔔 {notification['type']}"):
            enabled = st.checkbox("Abilitata", value=notification['enabled'], key=f"notif_{notification['type']}")
            
            if enabled:
                channels = st.multiselect(
                    "Canali di Notifica",
                    ["Email", "SMS", "Dashboard", "Push"],
                    default=notification['channels']
                )
                
                email_recipients = st.text_input("Destinatari Email", value="admin@company.com")
                sms_recipients = st.text_input("Destinatari SMS", value="+39 123 456 7890")

def create_integration_settings():
    """Crea la sezione integrazioni."""
    st.subheader("🔗 Configurazione Integrazioni")
    
    integrations = [
        {
            "name": "Slack",
            "status": "Connected",
            "webhook_url": "https://hooks.slack.com/services/...",
            "channels": ["#process-alerts", "#data-quality"]
        },
        {
            "name": "Microsoft Teams",
            "status": "Disconnected",
            "webhook_url": "",
            "channels": []
        },
        {
            "name": "Email SMTP",
            "status": "Connected",
            "smtp_server": "smtp.company.com",
            "port": 587
        }
    ]
    
    for integration in integrations:
        with st.expander(f"🔌 {integration['name']} - {integration['status']}"):
            if integration['status'] == "Connected":
                st.success(f"{integration['name']} connesso correttamente")
                
                if st.button(f"🔄 Riconnetti", key=f"reconnect_{integration['name']}"):
                    st.info(f"Riconnessione a {integration['name']} in corso...")
            else:
                st.warning(f"{integration['name']} non connesso")
                
                if st.button(f"🔗 Connetti", key=f"connect_{integration['name']}"):
                    st.success(f"{integration['name']} connesso!")

def create_backup_restore():
    """Crea la sezione backup e restore."""
    st.subheader("💾 Backup e Restore")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Backup Automatico")
        backup_schedule = st.selectbox("Frequenza Backup", ["Giornaliero", "Settimanale", "Mensile"])
        backup_time = st.time_input("Orario Backup", value=None)
        backup_location = st.text_input("Percorso Backup", value="/backups/process_mining/")
        
        if st.button("🔧 Configura Backup", type="primary"):
            st.success("Backup configurato con successo!")
    
    with col2:
        st.markdown("### Restore Dati")
        backup_files = [
            {"name": "backup_2024_01_15.zip", "size": "2.3 GB", "date": "15/01/2024"},
            {"name": "backup_2024_01_08.zip", "size": "2.1 GB", "date": "08/01/2024"},
            {"name": "backup_2024_01_01.zip", "size": "2.0 GB", "date": "01/01/2024"}
        ]
        
        selected_backup = st.selectbox("Seleziona Backup", [f"{b['name']} ({b['date']})" for b in backup_files])
        
        if st.button("🔄 Esegui Restore"):
            st.warning("Operazione di restore in corso. Non interrompere il processo!")
            st.progress(50)
            st.success("Restore completato con successo!")

def create_system_logs():
    """Crea la sezione log di sistema."""
    st.subheader("📋 Log di Sistema")
    
    log_level = st.selectbox("Livello Log", ["DEBUG", "INFO", "WARNING", "ERROR"])
    
    # Mock log data
    log_entries = [
        {"timestamp": "2024-01-15 10:30:15", "level": "INFO", "message": "Sistema avviato correttamente"},
        {"timestamp": "2024-01-15 10:25:30", "level": "WARNING", "message": "Connessione lenta al database"},
        {"timestamp": "2024-01-15 10:20:45", "level": "ERROR", "message": "Errore durante l'estrazione dati"},
        {"timestamp": "2024-01-15 10:15:20", "level": "INFO", "message": "Job ETL completato"},
        {"timestamp": "2024-01-15 10:10:10", "level": "DEBUG", "message": "Avvio processo mining"}
    ]
    
    for log in log_entries:
        log_color = {
            "DEBUG": "#6c757d",
            "INFO": "#28a745",
            "WARNING": "#ffc107",
            "ERROR": "#dc3545"
        }[log["level"]]
        
        st.markdown(f"""
        <div style="background: white; padding: 0.5rem; border-radius: 4px; border-left: 4px solid {log_color}; margin-bottom: 0.5rem;">
            <span style="color: {log_color}; font-weight: bold;">{log['level']}</span>
            <span style="margin-left: 1rem;">{log['timestamp']}</span>
            <br>
            <span style="color: #333;">{log['message']}</span>
        </div>
        """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🗑️ Cancella Log", type="primary"):
            st.success("Log cancellati con successo!")
    
    with col2:
        if st.button("📥 Scarica Log"):
            st.info("Download log in corso...")
    
    with col3:
        if st.button("🔍 Filtra Log"):
            st.info("Filtri applicati!")

def create_performance_tuning():
    """Crea la sezione tuning prestazioni."""
    st.subheader("⚡ Tuning Prestazioni")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Cache System")
        cache_enabled = st.checkbox("Cache Abilitata", value=True)
        cache_size = st.number_input("Dimensione Cache (MB)", min_value=100, max_value=10000, value=1000)
        cache_ttl = st.number_input("Tempo Cache (minuti)", min_value=1, max_value=1440, value=60)
        
        st.markdown("### Elaborazione Batch")
        batch_size = st.number_input("Dimensione Batch", min_value=100, max_value=10000, value=1000)
        parallel_processing = st.checkbox("Elaborazione Parallela", value=True)
    
    with col2:
        st.markdown("### Indicizzazione")
        index_optimization = st.checkbox("Ottimizzazione Indici", value=True)
        auto_index = st.checkbox("Indicizzazione Automatica", value=False)
        
        st.markdown("### Monitoraggio")
        performance_monitoring = st.checkbox("Monitoraggio Prestazioni", value=True)
        alert_threshold = st.slider("Soglia Alert (%)", min_value=50, max_value=100, value=80)
    
    if st.button("🔧 Applica Tuning", type="primary"):
        st.success("Tuning prestazioni applicato con successo!")

def create_user_management():
    """Crea la sezione gestione utenti."""
    st.subheader("👥 Gestione Utenti")
    
    users = [
        {"name": "Admin User", "role": "Administrator", "status": "Active", "last_login": "Oggi"},
        {"name": "Data Analyst", "role": "Analyst", "status": "Active", "last_login": "Ieri"},
        {"name": "Process Manager", "role": "Manager", "status": "Active", "last_login": "2 giorni fa"},
        {"name": "Viewer User", "role": "Viewer", "status": "Inactive", "last_login": "1 settimana fa"}
    ]
    
    for user in users:
        col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 2, 1])
        
        with col1:
            st.write(f"**{user['name']}**")
        
        with col2:
            st.write(f"Ruolo: {user['role']}")
        
        with col3:
            status_color = "🟢" if user["status"] == "Active" else "🔴"
            st.write(f"{status_color} {user['status']}")
        
        with col4:
            st.write(f"Ultimo Accesso: {user['last_login']}")
        
        with col5:
            if st.button("✏️", key=f"edit_{user['name']}"):
                st.info(f"Modifica utente {user['name']}")

def create_system_info():
    """Crea la sezione informazioni di sistema."""
    st.subheader("ℹ️ Informazioni Sistema")
    
    system_info = {
        "Versione Sistema": "2.1.3",
        "Versione Python": "3.10.15",
        "Database": "PostgreSQL 15.4",
        "Memoria Disponibile": "16 GB",
        "Spazio Disco": "500 GB / 1 TB",
        "Uptime": "7 giorni, 12 ore",
        "Licenza": "Enterprise Pro"
    }
    
    col1, col2 = st.columns(2)
    
    with col1:
        for key, value in list(system_info.items())[:4]:
            st.metric(key, value)
    
    with col2:
        for key, value in list(system_info.items())[4:]:
            st.metric(key, value)
    
    st.divider()
    
    # System health
    st.markdown("### Salute Sistema")
    
    health_metrics = [
        {"metric": "CPU Usage", "value": 45, "max": 100, "status": "good"},
        {"metric": "Memory Usage", "value": 68, "max": 100, "status": "warning"},
        {"metric": "Disk Usage", "value": 32, "max": 100, "status": "good"},
        {"metric": "Database Connections", "value": 12, "max": 100, "status": "good"}
    ]
    
    cols = st.columns(4)
    
    for i, metric in enumerate(health_metrics):
        with cols[i]:
            status_color = {
                "good": "#28a745",
                "warning": "#ffc107",
                "critical": "#dc3545"
            }[metric["status"]]
            
            st.metric(
                metric["metric"],
                f"{metric['value']}%",
                delta=f"{metric['value'] - 50:+}%"
            )
            st.progress(metric["value"] / 100)

def render_settings():
    """Renderizza la pagina impostazioni completa."""
    try:
        # System configuration
        create_system_configuration()
        st.divider()
        
        # Data sources
        create_data_sources()
        st.divider()
        
        # Privacy settings
        create_privacy_settings()
        st.divider()
        
        # Notification settings
        create_notification_settings()
        st.divider()
        
        # Integration settings
        create_integration_settings()
        st.divider()
        
        # Backup and restore
        create_backup_restore()
        st.divider()
        
        # System logs
        create_system_logs()
        st.divider()
        
        # Performance tuning
        create_performance_tuning()
        st.divider()
        
        # User management
        create_user_management()
        st.divider()
        
        # System info
        create_system_info()
        
    except Exception as e:
        st.error(f"Errore nel rendering della pagina impostazioni: {e}")
        st.info("Controllare i log per maggiori dettagli.")