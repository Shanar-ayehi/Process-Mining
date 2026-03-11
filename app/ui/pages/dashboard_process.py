import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json

def create_process_overview():
    """Crea la sezione overview del processo."""
    st.subheader("📈 Panoramica Processo")
    
    # Mock data
    overview_data = {
        "total_cases": 1247,
        "active_cases": 452,
        "completed_cases": 795,
        "avg_processing_time": "3.2 giorni",
        "process_efficiency": "87.5%"
    }
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Casi Totali", f"{overview_data['total_cases']:,}")
    
    with col2:
        st.metric("Casi Attivi", f"{overview_data['active_cases']:,}")
    
    with col3:
        st.metric("Casi Completati", f"{overview_data['completed_cases']:,}")
    
    with col4:
        st.metric("Tempo Medio", overview_data['avg_processing_time'])
    
    with col5:
        st.metric("Efficienza", overview_data['process_efficiency'])

def create_process_map():
    """Crea la mappa del processo."""
    st.subheader("🔄 Mappa Processo")
    
    # Process flow data
    activities = ['Inizio', 'Verifica', 'Approvazione', 'Esecuzione', 'Controllo', 'Fine']
    frequencies = [1247, 1180, 1050, 980, 950, 795]
    durations = [0, 1.2, 2.8, 1.5, 0.8, 0]
    
    # Create Sankey diagram
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=activities,
            color="#667eea"
        ),
        link=dict(
            source=[0, 1, 2, 3, 4],  # indices correspond to labels
            target=[1, 2, 3, 4, 5],
            value=frequencies[1:]
        )
    )])
    
    fig.update_layout(
        title_text="Flusso Processo",
        font_size=12,
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)

def create_performance_metrics():
    """Crea la sezione metriche performance."""
    st.subheader("📊 Metriche Performance")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Performance over time
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        performance_data = pd.DataFrame({
            'Date': dates,
            'Efficiency': [85 + (i % 10) for i in range(30)],
            'Throughput': [50 + (i % 20) for i in range(30)],
            'Quality': [92 + (i % 5) for i in range(30)]
        })
        
        fig = px.line(performance_data, x='Date', y=['Efficiency', 'Throughput', 'Quality'],
                     title='Performance nel Tempo')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Resource performance
        resource_data = pd.DataFrame({
            'Resource': ['Team A', 'Team B', 'Team C', 'Team D'],
            'Cases Handled': [320, 280, 245, 202],
            'Avg Time': [2.8, 3.5, 3.1, 4.2]
        })
        
        fig = px.bar(resource_data, x='Resource', y='Cases Handled',
                    title='Performance per Team')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

def create_bottleneck_analysis():
    """Crea l'analisi dei colli di bottiglia."""
    st.subheader("⚠️ Analisi Colli di Bottiglia")
    
    bottlenecks = [
        {
            "activity": "Approvazione",
            "severity": "High",
            "avg_wait_time": "2.8 giorni",
            "cases_affected": 170,
            "impact": "Ritardo significativo nel flusso"
        },
        {
            "activity": "Controllo Qualità",
            "severity": "Medium",
            "avg_wait_time": "0.8 giorni",
            "cases_affected": 950,
            "impact": "Aumento tempo totale"
        },
        {
            "activity": "Esecuzione",
            "severity": "Low",
            "avg_wait_time": "1.5 giorni",
            "cases_affected": 980,
            "impact": "Ottimizzabile"
        }
    ]
    
    for bottleneck in bottlenecks:
        severity_color = {
            "High": "#dc3545",
            "Medium": "#ffc107", 
            "Low": "#28a745"
        }[bottleneck["severity"]]
        
        st.markdown(f"""
        <div style="background: white; padding: 1rem; border-radius: 8px; border-left: 4px solid {severity_color}; margin-bottom: 1rem;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <h4 style="margin: 0; color: #333;">{bottleneck['activity']}</h4>
                <span style="background: {severity_color}; color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.8rem; font-weight: bold;">
                    {bottleneck['severity']}
                </span>
            </div>
            <p style="margin: 0.5rem 0 0 0; color: #666;">{bottleneck['impact']}</p>
            <div style="display: flex; gap: 2rem; margin-top: 0.5rem; font-size: 0.9rem;">
                <span><strong>Tempo Attesa:</strong> {bottleneck['avg_wait_time']}</span>
                <span><strong>Casi Affetti:</strong> {bottleneck['cases_affected']}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

def create_variant_analysis():
    """Crea l'analisi delle varianti di processo."""
    st.subheader("🔍 Analisi Varianti Processo")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Variant distribution
        variants_data = pd.DataFrame({
            'Variante': ['Variante Standard', 'Variante Rapida', 'Variante Complessa', 'Altre'],
            'Frequenza': [65, 20, 10, 5],
            'Cases': [810, 249, 125, 63]
        })
        
        fig = px.pie(variants_data, values='Frequenza', names='Variante',
                    title='Distribuzione Varianti Processo')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Variant performance
        variant_perf = pd.DataFrame({
            'Variante': ['Standard', 'Rapida', 'Complessa'],
            'Avg Time': [3.5, 2.1, 5.8],
            'Success Rate': [92, 95, 85],
            'Cost': [100, 85, 140]
        })
        
        fig = px.bar(variant_perf, x='Variante', y='Avg Time',
                    title='Tempo Medio per Variante')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

def create_compliance_monitoring():
    """Crea il monitoraggio compliance."""
    st.subheader("✅ Monitoraggio Compliance")
    
    compliance_metrics = [
        {"metric": "Tempi SLA", "value": 94.5, "target": 95, "status": "warning"},
        {"metric": "Autorizzazioni", "value": 100, "target": 100, "status": "success"},
        {"metric": "Documentazione", "value": 88.2, "target": 90, "status": "danger"},
        {"metric": "Audit Trail", "value": 99.1, "target": 98, "status": "success"}
    ]
    
    cols = st.columns(4)
    
    for i, metric in enumerate(compliance_metrics):
        with cols[i]:
            status_color = {
                "success": "#28a745",
                "warning": "#ffc107",
                "danger": "#dc3545"
            }[metric["status"]]
            
            st.metric(
                metric["metric"],
                f"{metric['value']}%",
                delta=f"{metric['value'] - metric['target']:+.1f}%"
            )
            st.progress(metric["value"] / 100)

def create_real_time_monitoring():
    """Crea il monitoraggio in tempo reale."""
    st.subheader("⏱️ Monitoraggio in Tempo Reale")
    
    # Real-time metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Casi in Elaborazione", "156", delta="+12")
    
    with col2:
        st.metric("Casi Bloccati", "8", delta="-2")
    
    with col3:
        st.metric("Tempo Medio Attesa", "2.3 ore", delta="-15 min")
    
    st.divider()
    
    # Real-time activity feed
    st.markdown("### 📋 Attività Recenti")
    
    recent_activities = [
        {"time": "10:30", "activity": "Nuovo caso creato", "status": "success"},
        {"time": "10:25", "activity": "Caso approvato", "status": "success"},
        {"time": "10:20", "activity": "Controllo qualità completato", "status": "success"},
        {"time": "10:15", "activity": "Caso in attesa approvazione", "status": "warning"},
        {"time": "10:10", "activity": "Errore elaborazione", "status": "danger"}
    ]
    
    for activity in recent_activities:
        status_icon = {
            "success": "✅",
            "warning": "⚠️",
            "danger": "❌"
        }[activity["status"]]
        
        st.markdown(f"{status_icon} **{activity['time']}** - {activity['activity']}")

def create_export_options():
    """Crea le opzioni di esportazione."""
    st.subheader("📤 Opzioni Esportazione")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📊 Esporta Dashboard", type="primary"):
            st.success("Dashboard esportata con successo!")
    
    with col2:
        if st.button("📈 Esporta Dati Grezzi"):
            st.info("Esportazione dati in corso...")
    
    with col3:
        if st.button("📋 Esporta Report PDF"):
            st.success("Report PDF generato!")

def render_dashboard():
    """Renderizza il dashboard processo completo."""
    try:
        # Overview section
        create_process_overview()
        st.divider()
        
        # Process visualization
        create_process_map()
        st.divider()
        
        # Performance metrics
        create_performance_metrics()
        st.divider()
        
        # Bottleneck analysis
        create_bottleneck_analysis()
        st.divider()
        
        # Variant analysis
        create_variant_analysis()
        st.divider()
        
        # Compliance monitoring
        create_compliance_monitoring()
        st.divider()
        
        # Real-time monitoring
        create_real_time_monitoring()
        st.divider()
        
        # Export options
        create_export_options()
        
    except Exception as e:
        st.error(f"Errore nel rendering del dashboard: {e}")
        st.info("Controllare i log per maggiori dettagli.")