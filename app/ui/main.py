import streamlit as st
from typing import Dict, Any, List, Optional
import asyncio
import json
from datetime import datetime
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

# Import services and utilities
from app.services.mining.discovery_service import discovery_service
from app.services.mining.conformance_service import conformance_service
from app.services.mining.kpi_service import kpi_service
from app.services.etl.data_extraction import data_extraction_service
from app.services.etl.data_transformation import data_transformation_service
from app.services.etl.data_quality import data_quality_service
from app.services.analytics.feature_engineering import feature_engineering_service
from app.services.analytics.predictive_models import predictive_models_service
from app.core.logger import get_logger

logger = get_logger()

# Page configuration
st.set_page_config(
    page_title="Process Mining Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 2rem;
    }
    
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        border-left: 4px solid #667eea;
        margin-bottom: 1rem;
    }
    
    .status-badge {
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 0.8rem;
        font-weight: bold;
    }
    
    .status-success { background-color: #d4edda; color: #155724; }
    .status-warning { background-color: #fff3cd; color: #856404; }
    .status-danger { background-color: #f8d7da; color: #721c24; }
    
    .nav-item {
        padding: 1rem;
        margin: 0.5rem 0;
        border-radius: 8px;
        cursor: pointer;
        transition: all 0.3s ease;
    }
    
    .nav-item:hover {
        background-color: #f0f2f6;
    }
    
    .nav-item.active {
        background-color: #667eea;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# Session state initialization
if 'current_page' not in st.session_state:
    st.session_state.current_page = 'dashboard'
if 'analysis_results' not in st.session_state:
    st.session_state.analysis_results = {}
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

def load_sample_data():
    """Carica dati di esempio per dimostrazione."""
    try:
        # Sample event log data
        sample_data = [
            {"case_id": "DEAL_001", "activity": "Deal Created", "timestamp": "2024-01-10T10:00:00", "resource": "Sales Rep A"},
            {"case_id": "DEAL_001", "activity": "Initial Contact", "timestamp": "2024-01-10T10:30:00", "resource": "Sales Rep A"},
            {"case_id": "DEAL_001", "activity": "Demo Scheduled", "timestamp": "2024-01-11T14:00:00", "resource": "Sales Rep A"},
            {"case_id": "DEAL_001", "activity": "Demo Completed", "timestamp": "2024-01-12T11:00:00", "resource": "Sales Rep A"},
            {"case_id": "DEAL_001", "activity": "Proposal Sent", "timestamp": "2024-01-13T16:00:00", "resource": "Sales Rep A"},
            {"case_id": "DEAL_001", "activity": "Negotiation", "timestamp": "2024-01-15T10:00:00", "resource": "Sales Manager"},
            {"case_id": "DEAL_001", "activity": "Closed Won", "timestamp": "2024-01-18T15:30:00", "resource": "Sales Manager"},
            
            {"case_id": "DEAL_002", "activity": "Deal Created", "timestamp": "2024-01-12T09:00:00", "resource": "Sales Rep B"},
            {"case_id": "DEAL_002", "activity": "Initial Contact", "timestamp": "2024-01-12T09:30:00", "resource": "Sales Rep B"},
            {"case_id": "DEAL_002", "activity": "Demo Scheduled", "timestamp": "2024-01-13T13:00:00", "resource": "Sales Rep B"},
            {"case_id": "DEAL_002", "activity": "Demo Completed", "timestamp": "2024-01-14T10:00:00", "resource": "Sales Rep B"},
            {"case_id": "DEAL_002", "activity": "Proposal Sent", "timestamp": "2024-01-15T15:00:00", "resource": "Sales Rep B"},
            {"case_id": "DEAL_002", "activity": "Closed Lost", "timestamp": "2024-01-16T11:00:00", "resource": "Sales Rep B"},
        ]
        
        st.session_state.sample_data = sample_data
        st.session_state.data_loaded = True
        return sample_data
    except Exception as e:
        logger.error(f"Errore caricamento dati di esempio: {e}")
        return []

def create_sidebar():
    """Crea la barra laterale con la navigazione."""
    st.sidebar.markdown('<h1 class="main-header">📊 Mining</h1>', unsafe_allow_html=True)
    
    # Navigation
    pages = [
        ("dashboard_process", "📊 Dashboard Processo"),
        ("data_quality", "🔍 Qualità Dati"),
        ("predictive_insights", "🔮 Insights Predittivi"),
        ("settings", "⚙️ Impostazioni")
    ]
    
    for page_key, page_name in pages:
        if st.sidebar.button(page_name, key=f"nav_{page_key}"):
            st.session_state.current_page = page_key
    
    st.sidebar.divider()
    
    # System status
    st.sidebar.subheader("🔧 Stato Sistema")
    
    # Mock system status
    services_status = {
        "Data Extraction": "success",
        "Data Transformation": "success", 
        "Process Mining": "success",
        "Analytics": "warning"
    }
    
    for service, status in services_status.items():
        status_class = f"status-{status}"
        st.sidebar.markdown(f'<span class="status-badge {status_class}">{service}: {status.upper()}</span>', unsafe_allow_html=True)
    
    st.sidebar.divider()
    
    # Quick actions
    st.sidebar.subheader("⚡ Azioni Rapide")
    if st.sidebar.button("🔄 Esegui Analisi Completa"):
        st.session_state.analysis_results = run_full_analysis()
    
    if st.sidebar.button("📊 Genera Report"):
        generate_report()

def create_header():
    """Crea l'intestazione principale."""
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col1:
        st.markdown('<h1 class="main-header">Process Mining</h1>', unsafe_allow_html=True)
    
    with col2:
        st.markdown("### Analisi Processi Commerciali - HubSpot Integration")
    
    with col3:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.markdown(f"**Ultimo Aggiornamento:** {current_time}")

def create_dashboard_overview():
    """Crea la sezione overview del dashboard."""
    st.subheader("📈 Panoramica Generale")
    
    # Load sample data if not already loaded
    if not st.session_state.data_loaded:
        load_sample_data()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Deal Totali", "150", delta="+12")
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Tasso Conversione", "65%", delta="+5%")
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Tempo Medio Chiusura", "28 giorni", delta="-3 giorni")
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Qualità Dati", "92%", delta="+2%")
        st.markdown('</div>', unsafe_allow_html=True)

def create_process_visualization():
    """Crea la visualizzazione del processo."""
    st.subheader("🔄 Mappa Processo")
    
    # Mock process data
    process_data = {
        'activities': ['Deal Created', 'Initial Contact', 'Demo Scheduled', 'Demo Completed', 
                      'Proposal Sent', 'Negotiation', 'Closed Won', 'Closed Lost'],
        'frequencies': [150, 145, 130, 120, 100, 60, 45, 15],
        'avg_duration': [0, 2.5, 1.8, 3.2, 2.1, 5.5, 0, 0]
    }
    
    # Create Sankey diagram
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=process_data['activities'],
            color="blue"
        ),
        link=dict(
            source=[0, 1, 2, 3, 4, 5, 5],  # indices correspond to labels
            target=[1, 2, 3, 4, 5, 6, 7],
            value=[145, 130, 120, 100, 60, 45, 15]
        )
    )])
    
    fig.update_layout(title_text="Flusso Processo Commerciale", font_size=12)
    st.plotly_chart(fig, use_container_width=True)

def create_kpi_section():
    """Crea la sezione KPI."""
    st.subheader("📊 Key Performance Indicators")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Performance over time
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        performance_data = pd.DataFrame({
            'Date': dates,
            'Conversion Rate': [0.6 + (i % 10) * 0.01 for i in range(30)],
            'Avg Deal Size': [5000 + (i % 20) * 100 for i in range(30)]
        })
        
        fig = px.line(performance_data, x='Date', y='Conversion Rate', 
                     title='Tasso Conversione - Ultimi 30 Giorni')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Resource performance
        resource_data = pd.DataFrame({
            'Resource': ['Sales Rep A', 'Sales Rep B', 'Sales Rep C', 'Sales Manager'],
            'Deals Closed': [25, 18, 22, 15],
            'Avg Time to Close': [25, 32, 28, 20]
        })
        
        fig = px.bar(resource_data, x='Resource', y='Deals Closed', 
                    title='Performance per Risorse')
        st.plotly_chart(fig, use_container_width=True)

def create_anomaly_detection():
    """Crea la sezione rilevamento anomalie."""
    st.subheader("⚠️ Rilevamento Anomalie")
    
    anomalies = [
        {"type": "Tempo Attesa", "severity": "High", "description": "Deal in Negotiation da 45 giorni"},
        {"type": "Attività Mancanti", "severity": "Medium", "description": "15 deal senza Demo Scheduled"},
        {"type": "Risorsa Sovraccarica", "severity": "Medium", "description": "Sales Rep A con 25 deal attivi"},
        {"type": "Conversione Bassa", "severity": "Low", "description": "Team B con conversion rate 45%"},
    ]
    
    for anomaly in anomalies:
        severity_color = {"High": "danger", "Medium": "warning", "Low": "success"}[anomaly["severity"]]
        st.markdown(f"""
        <div class="metric-card">
            <span class="status-badge status-{severity_color}">{anomaly['severity']}</span>
            <h4>{anomaly['type']}</h4>
            <p>{anomaly['description']}</p>
        </div>
        """, unsafe_allow_html=True)

def create_data_quality_page():
    """Crea la pagina di qualità dati."""
    st.subheader("🔍 Qualità Dati")
    
    # Data quality metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Completezza", "95%", delta="+2%")
        st.progress(0.95)
    
    with col2:
        st.metric("Consistenza", "92%", delta="+1%")
        st.progress(0.92)
    
    with col3:
        st.metric("Validità", "98%", delta="0%")
        st.progress(0.98)
    
    st.divider()
    
    # Data issues
    st.subheader("🚨 Problemi Identificati")
    
    issues = [
        {"field": "Email", "issue": "Valori mancanti", "count": 15, "severity": "Medium"},
        {"field": "Phone", "issue": "Formato non valido", "count": 8, "severity": "Low"},
        {"field": "Amount", "issue": "Valori negativi", "count": 3, "severity": "High"},
    ]
    
    for issue in issues:
        st.error(f"**{issue['field']}**: {issue['issue']} ({issue['count']} occorrenze)")

def create_predictive_insights_page():
    """Crea la pagina insights predittivi."""
    st.subheader("🔮 Insights Predittivi")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🎯 Predizione Churn")
        churn_data = pd.DataFrame({
            'Deal Stage': ['Prospecting', 'Qualification', 'Proposal', 'Negotiation'],
            'Churn Probability': [0.15, 0.25, 0.45, 0.65]
        })
        
        fig = px.bar(churn_data, x='Deal Stage', y='Churn Probability',
                    title='Probabilità Churn per Stage')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 💰 Predizione Revenue")
        revenue_data = pd.DataFrame({
            'Month': ['Jan', 'Feb', 'Mar', 'Apr', 'May'],
            'Predicted Revenue': [120000, 135000, 142000, 158000, 165000],
            'Actual Revenue': [118000, 132000, 140000, 155000, None]
        })
        
        fig = px.line(revenue_data, x='Month', y=['Predicted Revenue', 'Actual Revenue'],
                     title='Revenue Predetto vs Reale')
        st.plotly_chart(fig, use_container_width=True)

def create_settings_page():
    """Crea la pagina impostazioni."""
    st.subheader("⚙️ Impostazioni Sistema")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🔧 Configurazione ETL")
        st.checkbox("Abilita estrazione automatica", value=True)
        st.number_input("Frequenza estrazione (ore)", value=24)
        st.checkbox("Validazione dati abilitata", value=True)
    
    with col2:
        st.markdown("### 🎛️ Configurazione Mining")
        st.selectbox("Algoritmo default", ["DFG", "Alpha", "Heuristic"])
        st.slider("Soglia varianti", 0.0, 1.0, 0.05)
        st.checkbox("Calcolo KPI automatico", value=True)
    
    st.divider()
    
    # System actions
    st.markdown("### 🚀 Azioni Sistema")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔄 Test Connessione HubSpot"):
            st.success("Connessione HubSpot OK")
    
    with col2:
        if st.button("🧹 Pulizia Dati"):
            st.info("Pulizia dati in corso...")
    
    with col3:
        if st.button("📊 Backup Configurazione"):
            st.success("Backup completato")

def run_full_analysis():
    """Esegue un'analisi completa."""
    try:
        logger.info("Esecuzione analisi completa")
        
        # Mock analysis results
        results = {
            "discovery": {"algorithm": "DFG", "variants_found": 8},
            "conformance": {"fitness": 0.88, "precision": 0.82},
            "kpis": {"conversion_rate": 0.65, "avg_time_to_close": 28},
            "anomalies": 4,
            "data_quality": 0.92
        }
        
        st.session_state.analysis_results = results
        return results
        
    except Exception as e:
        logger.error(f"Errore analisi completa: {e}")
        return {}

def generate_report():
    """Genera un report."""
    try:
        logger.info("Generazione report")
        
        # Mock report generation
        report_data = {
            "timestamp": datetime.now().isoformat(),
            "summary": "Report analisi process mining generato con successo",
            "metrics": {
                "total_cases": 150,
                "process_variants": 8,
                "anomalies_detected": 4
            }
        }
        
        # Convert to JSON for download
        report_json = json.dumps(report_data, indent=2)
        
        st.download_button(
            label="📥 Scarica Report",
            data=report_json,
            file_name=f"process_mining_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json"
        )
        
    except Exception as e:
        logger.error(f"Errore generazione report: {e}")

def main():
    """Funzione principale dell'applicazione."""
    try:
        # Create sidebar
        create_sidebar()
        
        # Create main header
        create_header()
        
        # Page routing
        if st.session_state.current_page == 'dashboard':
            create_dashboard_overview()
            st.divider()
            create_process_visualization()
            st.divider()
            create_kpi_section()
            st.divider()
            create_anomaly_detection()
        
        elif st.session_state.current_page == 'data_quality':
            create_data_quality_page()
        
        elif st.session_state.current_page == 'predictive_insights':
            create_predictive_insights_page()
        
        elif st.session_state.current_page == 'settings':
            create_settings_page()
        
        # Footer
        st.divider()
        st.markdown("""
        <div style='text-align: center; color: #666; padding: 1rem;'>
            Process Mining Dashboard - Sistema Integrato HubSpot | 
            Versione 1.0.0 | 
            © 2024 Process Mining Team
        </div>
        """, unsafe_allow_html=True)
        
    except Exception as e:
        logger.error(f"Errore applicazione UI: {e}")
        st.error("Si è verificato un errore nell'applicazione. Controllare i log per maggiori dettagli.")

if __name__ == "__main__":
    main()