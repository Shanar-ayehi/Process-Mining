import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json

def create_quality_overview():
    """Crea la sezione overview della qualità dati."""
    st.subheader("📊 Panoramica Qualità Dati")
    
    # Mock data
    quality_data = {
        "overall_score": 87.5,
        "total_records": 12470,
        "valid_records": 10850,
        "invalid_records": 1620,
        "completeness": 92.3,
        "accuracy": 89.1,
        "consistency": 85.7,
        "timeliness": 94.2
    }
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Punteggio Generale", f"{quality_data['overall_score']}%", delta="+2.1%")
    
    with col2:
        st.metric("Record Validi", f"{quality_data['valid_records']:,}", 
                 delta=f"+{quality_data['valid_records'] - 10000}")
    
    with col3:
        st.metric("Record Invalidi", f"{quality_data['invalid_records']:,}", 
                 delta=f"-{1800 - quality_data['invalid_records']}")
    
    with col4:
        st.metric("Record Totali", f"{quality_data['total_records']:,}")

def create_quality_dimensions():
    """Crea la sezione dimensioni qualità."""
    st.subheader("🔍 Dimensioni Qualità")
    
    dimensions = [
        {"name": "Completezza", "score": 92.3, "target": 95, "status": "warning"},
        {"name": "Accuratezza", "score": 89.1, "target": 90, "status": "warning"},
        {"name": "Consistenza", "score": 85.7, "target": 85, "status": "success"},
        {"name": "Tempestività", "score": 94.2, "target": 95, "status": "warning"},
        {"name": "Unicità", "score": 96.8, "target": 95, "status": "success"},
        {"name": "Validità", "score": 88.4, "target": 90, "status": "danger"}
    ]
    
    cols = st.columns(3)
    
    for i, dim in enumerate(dimensions):
        with cols[i % 3]:
            status_color = {
                "success": "#28a745",
                "warning": "#ffc107", 
                "danger": "#dc3545"
            }[dim["status"]]
            
            st.metric(
                dim["name"],
                f"{dim['score']}%",
                delta=f"{dim['score'] - dim['target']:+.1f}%"
            )
            st.progress(dim["score"] / 100)

def create_data_issues():
    """Crea la sezione problemi dati."""
    st.subheader("⚠️ Problemi Dati Identificati")
    
    issues = [
        {
            "type": "Valori Mancanti",
            "severity": "High",
            "count": 1247,
            "affected_fields": ["email", "phone", "address"],
            "description": "Campi obbligatori non compilati"
        },
        {
            "type": "Formato Errato",
            "severity": "Medium", 
            "count": 356,
            "affected_fields": ["date", "amount"],
            "description": "Formato dati non conforme agli standard"
        },
        {
            "type": "Duplicati",
            "severity": "Low",
            "count": 45,
            "affected_fields": ["customer_id"],
            "description": "Record duplicati rilevati"
        },
        {
            "type": "Outliers",
            "severity": "Medium",
            "count": 89,
            "affected_fields": ["amount", "age"],
            "description": "Valori anomali fuori range accettabile"
        }
    ]
    
    for issue in issues:
        severity_color = {
            "High": "#dc3545",
            "Medium": "#ffc107",
            "Low": "#28a745"
        }[issue["severity"]]
        
        with st.expander(f"{issue['type']} ({issue['count']} record)"):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown(f"**Gravità:** {issue['severity']}")
                st.markdown(f"**Campi Affetti:** {', '.join(issue['affected_fields'])}")
                st.markdown(f"**Descrizione:** {issue['description']}")
            
            with col2:
                st.markdown(f"""
                <div style="text-align: right;">
                    <span style="background: {severity_color}; color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.8rem; font-weight: bold;">
                        {issue['severity']}
                    </span>
                </div>
                """, unsafe_allow_html=True)

def create_quality_trends():
    """Crea la sezione trend qualità."""
    st.subheader("📈 Trend Qualità nel Tempo")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Quality trend over time
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        quality_trend = pd.DataFrame({
            'Date': dates,
            'Overall Quality': [85 + (i % 10) for i in range(30)],
            'Completeness': [90 + (i % 5) for i in range(30)],
            'Accuracy': [88 + (i % 8) for i in range(30)],
            'Consistency': [82 + (i % 12) for i in range(30)]
        })
        
        fig = px.line(quality_trend, x='Date', y=['Overall Quality', 'Completeness', 'Accuracy', 'Consistency'],
                     title='Qualità Dati nel Tempo')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Issue trend
        issue_trend = pd.DataFrame({
            'Date': dates,
            'Missing Values': [100 + (i % 50) for i in range(30)],
            'Format Errors': [30 + (i % 20) for i in range(30)],
            'Duplicates': [5 + (i % 10) for i in range(30)],
            'Outliers': [20 + (i % 15) for i in range(30)]
        })
        
        fig = px.area(issue_trend, x='Date', y=['Missing Values', 'Format Errors', 'Duplicates', 'Outliers'],
                     title='Problemi Dati nel Tempo')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

def create_data_profiling():
    """Crea la sezione profiling dati."""
    st.subheader("🔍 Profiling Dati")
    
    # Mock profiling data
    profiling_data = [
        {"field": "customer_id", "type": "string", "unique": 98.5, "null": 0.0, "empty": 1.5},
        {"field": "email", "type": "string", "unique": 95.2, "null": 2.1, "empty": 2.7},
        {"field": "phone", "type": "string", "unique": 91.8, "null": 5.3, "empty": 2.9},
        {"field": "amount", "type": "number", "unique": 78.4, "null": 0.0, "empty": 0.0},
        {"field": "date", "type": "date", "unique": 99.1, "null": 0.5, "empty": 0.4},
        {"field": "status", "type": "string", "unique": 25.6, "null": 0.0, "empty": 0.0}
    ]
    
    df_profiling = pd.DataFrame(profiling_data)
    
    # Display as table
    st.dataframe(
        df_profiling,
        column_config={
            "field": "Campo",
            "type": "Tipo",
            "unique": st.column_config.NumberColumn("Unicità (%)", format="%.1f%%"),
            "null": st.column_config.NumberColumn("Null (%)", format="%.1f%%"),
            "empty": st.column_config.NumberColumn("Vuoti (%)", format="%.1f%%")
        },
        hide_index=True,
        use_container_width=True
    )

def create_quality_rules():
    """Crea la sezione regole qualità."""
    st.subheader("📋 Regole Qualità Dati")
    
    rules = [
        {"rule": "Email Formato Valid", "status": "Active", "violations": 124, "last_check": "2 ore fa"},
        {"rule": "Campi Obbligatori", "status": "Active", "violations": 89, "last_check": "1 ora fa"},
        {"rule": "Range Importi", "status": "Active", "violations": 15, "last_check": "30 min fa"},
        {"rule": "Formato Date", "status": "Active", "violations": 45, "last_check": "4 ore fa"},
        {"rule": "Unicità ID", "status": "Active", "violations": 0, "last_check": "1 giorno fa"}
    ]
    
    for rule in rules:
        col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 2])
        
        with col1:
            st.write(f"**{rule['rule']}**")
        
        with col2:
            status_color = "🟢" if rule["status"] == "Active" else "🔴"
            st.write(f"{status_color} {rule['status']}")
        
        with col3:
            st.metric("Violazioni", rule["violations"])
        
        with col4:
            if st.button("🔄", key=f"check_{rule['rule']}"):
                st.success(f"Regola {rule['rule']} verificata!")
        
        with col5:
            st.write(f"Ultimo controllo: {rule['last_check']}")

def create_data_quality_actions():
    """Crea la sezione azioni qualità dati."""
    st.subheader("🛠️ Azioni Miglioramento Qualità")
    
    actions = [
        {
            "action": "Pulizia Valori Mancanti",
            "priority": "High",
            "status": "In Progress",
            "affected_records": 1247,
            "estimated_time": "2 ore"
        },
        {
            "action": "Correzione Formato Date",
            "priority": "Medium", 
            "status": "Pending",
            "affected_records": 45,
            "estimated_time": "30 min"
        },
        {
            "action": "Rimozione Duplicati",
            "priority": "Low",
            "status": "Pending",
            "affected_records": 45,
            "estimated_time": "15 min"
        },
        {
            "action": "Validazione Importi",
            "priority": "Medium",
            "status": "Pending", 
            "affected_records": 89,
            "estimated_time": "45 min"
        }
    ]
    
    for action in actions:
        priority_color = {
            "High": "#dc3545",
            "Medium": "#ffc107",
            "Low": "#28a745"
        }[action["priority"]]
        
        status_color = {
            "In Progress": "#ffc107",
            "Pending": "#6c757d",
            "Completed": "#28a745"
        }[action["status"]]
        
        st.markdown(f"""
        <div style="background: white; padding: 1rem; border-radius: 8px; border-left: 4px solid {priority_color}; margin-bottom: 1rem;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <h4 style="margin: 0; color: #333;">{action['action']}</h4>
                <div style="display: flex; gap: 1rem; align-items: center;">
                    <span style="background: {status_color}; color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.8rem; font-weight: bold;">
                        {action['status']}
                    </span>
                    <span style="background: {priority_color}; color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.8rem; font-weight: bold;">
                        {action['priority']}
                    </span>
                </div>
            </div>
            <div style="display: flex; gap: 2rem; margin-top: 0.5rem; font-size: 0.9rem;">
                <span><strong>Record Affetti:</strong> {action['affected_records']}</span>
                <span><strong>Tempo Stimato:</strong> {action['estimated_time']}</span>
            </div>
            <div style="margin-top: 0.5rem;">
                <button style="background: #007bff; color: white; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer;">
                    Avvia Azione
                </button>
            </div>
        </div>
        """, unsafe_allow_html=True)

def create_quality_reports():
    """Crea la sezione report qualità."""
    st.subheader("📋 Report Qualità Dati")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📊 Report Completo", type="primary"):
            st.success("Report qualità generato!")
    
    with col2:
        if st.button("📈 Trend Report"):
            st.info("Report trend in generazione...")
    
    with col3:
        if st.button("⚠️ Report Problemi"):
            st.warning("Report problemi generato!")

def render_data_quality():
    """Renderizza la pagina qualità dati completa."""
    try:
        # Overview section
        create_quality_overview()
        st.divider()
        
        # Quality dimensions
        create_quality_dimensions()
        st.divider()
        
        # Data issues
        create_data_issues()
        st.divider()
        
        # Quality trends
        create_quality_trends()
        st.divider()
        
        # Data profiling
        create_data_profiling()
        st.divider()
        
        # Quality rules
        create_quality_rules()
        st.divider()
        
        # Quality actions
        create_data_quality_actions()
        st.divider()
        
        # Quality reports
        create_quality_reports()
        
    except Exception as e:
        st.error(f"Errore nel rendering della pagina qualità dati: {e}")
        st.info("Controllare i log per maggiori dettagli.")