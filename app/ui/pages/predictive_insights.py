import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json
import numpy as np

def create_predictions_overview():
    """Crea la sezione overview delle predizioni."""
    st.subheader("🔮 Panoramica Predizioni")
    
    # Mock prediction data
    predictions_data = {
        "total_predictions": 1247,
        "accuracy_score": 87.5,
        "model_version": "v2.1.3",
        "last_updated": "2 ore fa",
        "cases_predicted": 892,
        "cases_confirmed": 785,
        "cases_pending": 107
    }
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Predizioni Totali", f"{predictions_data['total_predictions']:,}")
    
    with col2:
        st.metric("Accuratezza Modello", f"{predictions_data['accuracy_score']}%", delta="+1.2%")
    
    with col3:
        st.metric("Casi Predetti", f"{predictions_data['cases_predicted']:,}")
    
    with col4:
        st.metric("Casi Confermati", f"{predictions_data['cases_confirmed']:,}")

def create_risk_analysis():
    """Crea la sezione analisi rischi."""
    st.subheader("⚠️ Analisi Rischio Predittiva")
    
    risk_data = [
        {
            "case_id": "CASE-001",
            "risk_level": "High",
            "probability": 85.2,
            "predicted_outcome": "Ritardo > 5 giorni",
            "confidence": 92.1,
            "factors": ["Storia precedenti ritardi", "Complessità processo", "Risorsa assegnata"]
        },
        {
            "case_id": "CASE-002",
            "risk_level": "Medium",
            "probability": 62.8,
            "predicted_outcome": "Ritardo 2-5 giorni",
            "confidence": 78.5,
            "factors": ["Nuovo cliente", "Documentazione incompleta"]
        },
        {
            "case_id": "CASE-003",
            "risk_level": "Low",
            "probability": 23.4,
            "predicted_outcome": "Completamento standard",
            "confidence": 95.2,
            "factors": ["Cliente storico", "Processo standard"]
        }
    ]
    
    for risk in risk_data:
        risk_color = {
            "High": "#dc3545",
            "Medium": "#ffc107",
            "Low": "#28a745"
        }[risk["risk_level"]]
        
        with st.expander(f"🚨 {risk['case_id']} - Rischio {risk['risk_level']} ({risk['probability']:.1f}%)"):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown(f"**Esito Predetto:** {risk['predicted_outcome']}")
                st.markdown(f"**Fattori di Rischio:**")
                for factor in risk["factors"]:
                    st.markdown(f"- {factor}")
            
            with col2:
                st.markdown(f"""
                <div style="text-align: right;">
                    <span style="background: {risk_color}; color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.8rem; font-weight: bold;">
                        {risk['risk_level']}
                    </span>
                    <br><br>
                    <div style="margin-top: 1rem;">
                        <strong>Confidenza:</strong><br>
                        <span style="font-size: 1.2rem; color: {risk_color};">{risk['confidence']:.1f}%</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

def create_trend_predictions():
    """Crea la sezione predizioni trend."""
    st.subheader("📈 Predizioni Trend e Andamento")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Volume predictions over time
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        volume_data = pd.DataFrame({
            'Date': dates,
            'Predicted Cases': [100 + (i % 30) for i in range(30)],
            'Actual Cases': [95 + (i % 25) for i in range(30)],
            'Accuracy': [85 + (i % 10) for i in range(30)]
        })
        
        fig = px.line(volume_data, x='Date', y=['Predicted Cases', 'Actual Cases'],
                     title='Volume Predizioni vs Reale')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Risk distribution
        risk_distribution = pd.DataFrame({
            'Risk Level': ['Low', 'Medium', 'High'],
            'Count': [650, 350, 247],
            'Percentage': [52.1, 28.1, 19.8]
        })
        
        fig = px.pie(risk_distribution, values='Count', names='Risk Level',
                    title='Distribuzione Rischio Predittivo')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

def create_model_performance():
    """Crea la sezione performance modello."""
    st.subheader("🤖 Performance Modelli Predittivi")
    
    models_data = [
        {
            "model": "Random Forest",
            "accuracy": 87.5,
            "precision": 85.2,
            "recall": 89.1,
            "f1_score": 87.1,
            "status": "Active"
        },
        {
            "model": "Neural Network",
            "accuracy": 89.2,
            "precision": 87.8,
            "recall": 90.5,
            "f1_score": 89.1,
            "status": "Active"
        },
        {
            "model": "Gradient Boosting",
            "accuracy": 86.7,
            "precision": 84.3,
            "recall": 88.2,
            "f1_score": 86.2,
            "status": "Active"
        },
        {
            "model": "Logistic Regression",
            "accuracy": 82.3,
            "precision": 80.1,
            "recall": 84.7,
            "f1_score": 82.4,
            "status": "Inactive"
        }
    ]
    
    df_models = pd.DataFrame(models_data)
    
    # Display model performance
    st.dataframe(
        df_models,
        column_config={
            "model": "Modello",
            "accuracy": st.column_config.NumberColumn("Accuratezza (%)", format="%.1f%%"),
            "precision": st.column_config.NumberColumn("Precisione (%)", format="%.1f%%"),
            "recall": st.column_config.NumberColumn("Recall (%)", format="%.1f%%"),
            "f1_score": st.column_config.NumberColumn("F1 Score (%)", format="%.1f%%"),
            "status": "Stato"
        },
        hide_index=True,
        use_container_width=True
    )

def create_feature_importance():
    """Crea la sezione importanza feature."""
    st.subheader("🔍 Importanza Feature Predittive")
    
    # Feature importance data
    features = [
        {"feature": "Storia Processi Precedenti", "importance": 25.4, "impact": "High"},
        {"feature": "Complessità Caso", "importance": 18.7, "impact": "High"},
        {"feature": "Risorsa Assegnata", "importance": 15.2, "impact": "Medium"},
        {"feature": "Tempo Attesa Iniziale", "importance": 12.8, "impact": "Medium"},
        {"feature": "Tipo Cliente", "importance": 11.3, "impact": "Low"},
        {"feature": "Documentazione Completa", "importance": 9.1, "impact": "Low"},
        {"feature": "Orari Lavorativi", "importance": 7.5, "impact": "Low"}
    ]
    
    df_features = pd.DataFrame(features)
    
    # Create horizontal bar chart
    fig = px.bar(df_features, x='importance', y='feature', orientation='h',
                 title='Importanza Feature nel Modello Predittivo',
                 color='impact',
                 color_discrete_map={'High': '#dc3545', 'Medium': '#ffc107', 'Low': '#28a745'})
    
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

def create_anomaly_detection():
    """Crea la sezione rilevamento anomalie."""
    st.subheader("🚨 Rilevamento Anomalie Predittive")
    
    anomalies = [
        {
            "case_id": "ANOM-001",
            "type": "Tempo Attesa Anomalo",
            "severity": "High",
            "value": "15 giorni",
            "expected": "< 5 giorni",
            "confidence": 94.2,
            "description": "Tempo di attesa significativamente superiore alla media"
        },
        {
            "case_id": "ANOM-002",
            "type": "Pattern Inconsistente",
            "severity": "Medium",
            "value": "3 approvazioni",
            "expected": "1-2 approvazioni",
            "confidence": 87.5,
            "description": "Numero di approvazioni fuori dal range normale"
        },
        {
            "case_id": "ANOM-003",
            "type": "Risorsa Non Disponibile",
            "severity": "Low",
            "value": "Risorsa X",
            "expected": "Risorse standard",
            "confidence": 78.9,
            "description": "Assegnazione risorsa non prevista dal modello"
        }
    ]
    
    for anomaly in anomalies:
        severity_color = {
            "High": "#dc3545",
            "Medium": "#ffc107",
            "Low": "#28a745"
        }[anomaly["severity"]]
        
        st.markdown(f"""
        <div style="background: white; padding: 1rem; border-radius: 8px; border-left: 4px solid {severity_color}; margin-bottom: 1rem;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <h4 style="margin: 0; color: #333;">{anomaly['case_id']} - {anomaly['type']}</h4>
                <span style="background: {severity_color}; color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.8rem; font-weight: bold;">
                    {anomaly['severity']}
                </span>
            </div>
            <div style="display: flex; gap: 2rem; margin-top: 0.5rem; font-size: 0.9rem;">
                <span><strong>Valore:</strong> {anomaly['value']}</span>
                <span><strong>Atteso:</strong> {anomaly['expected']}</span>
                <span><strong>Confidenza:</strong> {anomaly['confidence']}%</span>
            </div>
            <p style="margin: 0.5rem 0 0 0; color: #666;">{anomaly['description']}</p>
        </div>
        """, unsafe_allow_html=True)

def create_predictive_alerts():
    """Crea la sezione alert predittivi."""
    st.subheader("⚠️ Alert Predittivi")
    
    alerts = [
        {
            "alert": "Rischio Congestione Processi",
            "severity": "High",
            "cases_affected": 156,
            "predicted_impact": "Aumento tempo medio del 40%",
            "recommended_action": "Riallocare risorse al team di approvazione"
        },
        {
            "alert": "Pattern Anomalo Rilevato",
            "severity": "Medium",
            "cases_affected": 45,
            "predicted_impact": "Possibili errori di processo",
            "recommended_action": "Verificare documentazione e procedure"
        },
        {
            "alert": "Ottimizzazione Possibile",
            "severity": "Low",
            "cases_affected": 89,
            "predicted_impact": "Riduzione tempo del 15%",
            "recommended_action": "Considerare automazione passi ripetitivi"
        }
    ]
    
    for alert in alerts:
        severity_color = {
            "High": "#dc3545",
            "Medium": "#ffc107",
            "Low": "#28a745"
        }[alert["severity"]]
        
        st.markdown(f"""
        <div style="background: white; padding: 1rem; border-radius: 8px; border-left: 4px solid {severity_color}; margin-bottom: 1rem;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <h4 style="margin: 0; color: #333;">{alert['alert']}</h4>
                <span style="background: {severity_color}; color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.8rem; font-weight: bold;">
                    {alert['severity']}
                </span>
            </div>
            <div style="display: flex; gap: 2rem; margin-top: 0.5rem; font-size: 0.9rem;">
                <span><strong>Casi Affetti:</strong> {alert['cases_affected']}</span>
                <span><strong>Impatto Previsto:</strong> {alert['predicted_impact']}</span>
            </div>
            <div style="margin-top: 0.5rem;">
                <strong>Azione Raccomandata:</strong> {alert['recommended_action']}
            </div>
        </div>
        """, unsafe_allow_html=True)

def create_forecast_scenarios():
    """Crea la sezione scenari forecast."""
    st.subheader("🔮 Scenari Forecast Futuri")
    
    scenarios = [
        {
            "scenario": "Scenario Base",
            "timeframe": "30 giorni",
            "predicted_cases": 1250,
            "avg_processing_time": "3.2 giorni",
            "confidence": 87.5
        },
        {
            "scenario": "Scenario Ottimistico",
            "timeframe": "30 giorni",
            "predicted_cases": 1100,
            "avg_processing_time": "2.8 giorni",
            "confidence": 82.3
        },
        {
            "scenario": "Scenario Pessimistico",
            "timeframe": "30 giorni",
            "predicted_cases": 1400,
            "avg_processing_time": "4.1 giorni",
            "confidence": 79.8
        }
    ]
    
    col1, col2, col3 = st.columns(3)
    
    for i, scenario in enumerate(scenarios):
        with [col1, col2, col3][i]:
            st.markdown(f"### {scenario['scenario']}")
            st.metric("Casi Predetti", f"{scenario['predicted_cases']:,}")
            st.metric("Tempo Medio", scenario['avg_processing_time'])
            st.metric("Confidenza", f"{scenario['confidence']}%")

def create_predictive_reports():
    """Crea la sezione report predittivi."""
    st.subheader("📋 Report Predittivi")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📊 Report Risk Analysis", type="primary"):
            st.success("Report analisi rischio generato!")
    
    with col2:
        if st.button("📈 Report Trend Forecast"):
            st.info("Report trend forecast in generazione...")
    
    with col3:
        if st.button("🤖 Report Model Performance"):
            st.warning("Report performance modelli generato!")

def render_predictive_insights():
    """Renderizza la pagina insights predittivi completa."""
    try:
        # Overview section
        create_predictions_overview()
        st.divider()
        
        # Risk analysis
        create_risk_analysis()
        st.divider()
        
        # Trend predictions
        create_trend_predictions()
        st.divider()
        
        # Model performance
        create_model_performance()
        st.divider()
        
        # Feature importance
        create_feature_importance()
        st.divider()
        
        # Anomaly detection
        create_anomaly_detection()
        st.divider()
        
        # Predictive alerts
        create_predictive_alerts()
        st.divider()
        
        # Forecast scenarios
        create_forecast_scenarios()
        st.divider()
        
        # Predictive reports
        create_predictive_reports()
        
    except Exception as e:
        st.error(f"Errore nel rendering della pagina insights predittivi: {e}")
        st.info("Controllare i log per maggiori dettagli.")