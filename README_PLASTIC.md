# Process Mining System - Sistema FastAPI

## Panoramica

Questo è un sistema Process Mining completo basato su **FastAPI** (backend) e **React** (frontend) che integra HubSpot tramite OAuth 2.0 per l'analisi dei processi aziendali con funzionalità avanzate di simulazione What-If.

Il sistema è progettato per:

- **Integrazione HubSpot**: Connessione OAuth 2.0 con HubSpot CRM inclusi workflow di automazione
- **ETL Pipeline**: Estrazione, trasformazione e caricamento dati
- **Process Discovery**: Algoritmi PM4Py per analisi processi con mapping automazioni
- **Simulazione What-If**: Motore SimPy per analisi ipotetiche su tempi e automazioni
- **Dashboard Interattiva**: Canvas React Flow con sidebar What-If
- **Data Quality**: Validazione e controllo qualità dati
- **Privacy by Design**: Pseudonimizzazione e compliance GDPR

## Architettura del Sistema

```
Process-Mining/
├── main.py                          # Entry point FastAPI
├── app/
│   ├── api/                         # API REST (FastAPI)
│   │   ├── main.py                  # App FastAPI principale
│   │   ├── routes_connector.py      # Endpoint connettore HubSpot (incluso workflows)
│   │   ├── routes_mining.py         # Endpoint mining processi (DFG con automazioni)
│   │   ├── routes_analytics.py      # Endpoint analytics (simulazione What-If)
│   │   ├── routes_dq.py             # Endpoint data quality
│   │   ├── routes_process_management.py  # Endpoint gestione processi
│   │   ├── routes/auth.py           # Endpoint autenticazione OAuth (scope automation)
│   │   └── routes_external_cards.py # Endpoint external cards
│   ├── connectors/                  # Connettori esterni
│   │   ├── hubspot_client.py        # Client HubSpot OAuth 2.0 (incluso get_workflows)
│   │   └── hubspot_mapper.py        # Mapping dati HubSpot
│   ├── core/                        # Funzionalità core
│   │   ├── config.py                # Configurazione centralizzata
│   │   ├── database.py              # Setup database SQLite
│   │   ├── logger.py                # Logging
│   │   ├── privacy.py               # Gestione privacy GDPR
│   │   ├── bootstrap.py             # Bootstrap sistema
│   │   └── integration.py           # Test integrazione
│   ├── services/                    # Logica business
│   │   ├── etl/                     # Servizi ETL
│   │   │   ├── data_extraction.py   # Estrazione dati HubSpot (incluso workflows)
│   │   │   ├── data_transformation.py # Trasformazione dati
│   │   │   ├── data_quality.py      # Validazione qualità
│   │   │   └── privacy_governance.py # Governance privacy
│   │   ├── mining/                  # Servizi mining
│   │   │   ├── discovery_service.py # Process Discovery (mapping automazioni sui nodi)
│   │   │   ├── conformance_service.py # Conformance Checking
│   │   │   └── kpi_service.py       # Calcolo KPI
│   │   └── analytics/               # Servizi analytics
│   │       └── simulation_service.py # Simulation Engine (SimPy per What-If)
│   ├── tasks/                       # Task asincroni (Celery)
│   │   ├── worker.py                # Worker Celery
│   │   ├── etl_task.py              # Task ETL (incluso workflow)
│   │   ├── mining_task.py           # Task mining
│   │   ├── analytics_task.py        # Task simulazione What-If
│   │   └── dq_task.py               # Task data quality
│   └── models/                      # Modelli database
│       └── auth.py                  # Modelli autenticazione
├── frontend/                        # React Frontend
│   ├── src/
│   │   ├── components/
│   │   │   ├── ProcessList.tsx      # Lista processi
│   │   │   ├── ProcessAnalysis.tsx  # Canvas React Flow full-screen
│   │   │   ├── CustomNode.tsx       # Nodo custom con badge automazioni ⚡️
│   │   │   ├── WhatIfSidebar.tsx    # Sidebar What-If Analysis
│   │   │   └── ProcessDetail.tsx    # Dettaglio processo
│   │   ├── App.tsx                  # Routing principale con ReactFlowProvider
│   │   └── index.css                # Stili (inclusi React Flow)
│   └── package.json                 # Dipendenze (incluso @xyflow/react)
├── data/                            # Directory dati
│   ├── raw/                         # Dati grezzi (inclusi workflow JSON)
│   ├── processed/                   # Dati processati
│   └── warehouse/                   # Data warehouse
├── logs/                            # Log sistema
├── docker-compose.yml               # Configurazione Docker
├── pyproject.toml                   # Configurazione Python (incluso SimPy)
└── README.md                        # Documentazione principale
```

## Caratteristiche Implementate

### 🔐 Autenticazione OAuth 2.0
- **HubSpot OAuth**: Autenticazione completa con HubSpot
- **Scope Automation**: Accesso ai workflow di HubSpot
- **Token Management**: Gestione automatica refresh token
- **Database SQLite**: Storage token e sessioni
- **Sicurezza**: HTTPS, CORS, gestione errori

### 📊 Estrazione Dati HubSpot
- **Deal Extraction**: Estrazione deal con cronologia
- **Contact Extraction**: Estrazione contatti
- **Company Extraction**: Estrazione aziende
- **Pipeline Stages**: Estrazione fasi pipeline
- **Workflow Extraction**: Estrazione workflow di automazione
- **Paginazione Automatica**: Gestione grandi volumi dati

### 🔍 Process Discovery
- **DFG (Directly-Follows Graph)**: Generazione grafi processo con automazioni mappate
- **Alpha Miner**: Discovery con algoritmo Alpha
- **Heuristic Miner**: Discovery con euristica
- **Inductive Miner**: Discovery induttivo
- **Variant Analysis**: Analisi varianti processo
- **Badge Automazioni**: Visualizzazione ⚡️ sui nodi con workflow attivi

### 🎯 Simulazione What-If (SimPy)
- **Simulation Engine**: Motore SimPy per analisi ipotetiche
- **Modifica Tempi**: Slider moltiplicatore tempo (10%-200%)
- **Modifica Probabilità**: Override probabilità transizione
- **Gestione Automazioni**: Disabilitazione/override automazioni HubSpot
- **Task Asincroni**: Esecuzione simulazioni con Celery
- **Progress Indicator**: Barra avanzamento simulazione

### 🖥️ Dashboard Interattiva (React Flow)
- **Canvas Full-Screen**: Visualizzazione grafo processo a schermo intero
- **Nodi Custom**: Nodi con badge automazioni e tempo medio
- **Filtro Frequenza**: Slider continuo 0-100% per filtrare archi
- **Sidebar What-If**: Pannello laterale per analisi ipotetiche
- **MiniMap**: Navigazione stile Google Maps
- **Controls**: Zoom, pan, fit view
- **Background**: Griglia per riferimento visivo

### ✅ Data Quality
- **Schema Validation**: Validazione schema event log
- **Completeness Check**: Controllo completezza dati
- **Consistency Check**: Controllo consistenza
- **Quality Reports**: Report dettagliati qualità

### 🔒 Privacy e GDPR
- **Pseudonimizzazione**: Hash email con salt
- **Data Retention**: Policy retention automatica
- **Audit Log**: Tracciamento accessi dati
- **GDPR Compliance**: Validazione compliance

### ⚙️ Task Asincroni (Celery)
- **ETL Tasks**: Elaborazione asincrona ETL
- **Mining Tasks**: Calcoli mining in background
- **Simulation Tasks**: Simulazioni What-If asincrone
- **Data Quality Tasks**: Controlli qualità asincroni
- **Redis Broker**: Gestione code messaggi

## Installazione e Configurazione

### Prerequisiti
- Python 3.12+
- Node.js 18+
- Docker & Docker Compose (opzionale)
- HubSpot API Key (per integrazione reale)

### Setup Rapido

1. **Clona il repository**
   ```bash
   git clone <repository-url>
   cd Process-Mining
   ```

2. **Installa dipendenze backend**
   ```bash
   poetry install
   ```

3. **Installa dipendenze frontend**
   ```bash
   cd frontend
   npm install
   cd ..
   ```

4. **Configura ambiente**
   ```bash
   # Backend (.env)
   HUBSPOT_CLIENT_ID=your_client_id
   HUBSPOT_CLIENT_SECRET=your_client_secret
   HUBSPOT_REDIRECT_URI=http://localhost:8000/api/v1/auth/callback
   
   # Frontend (frontend/.env)
   echo "VITE_API_URL=http://localhost:8000/api/v1" > frontend/.env
   ```

5. **Avvia sistema**
   ```bash
   # Backend
   python main.py
   
   # Frontend (nuovo terminale)
   cd frontend && npm run dev
   
   # Worker Celery (opzionale)
   poetry run celery -A app.tasks.worker.celery_app worker --loglevel=info
   ```

### Docker Setup

```bash
# Avvia tutti i servizi
docker-compose up -d

# Verifica stato
docker-compose ps

# Log
docker-compose logs -f
```

## API Endpoints

### Autenticazione
- `GET /api/v1/auth/hubspot/login` - Inizio OAuth (scope `automation`)
- `GET /api/v1/auth/callback` - Callback OAuth
- `POST /api/v1/auth/refresh` - Refresh token
- `GET /api/v1/auth/status` - Stato autenticazione

### Connessione HubSpot
- `GET /api/v1/connector/deals` - Lista deal
- `GET /api/v1/connector/contacts` - Lista contatti
- `GET /api/v1/connector/companies` - Lista aziende
- `GET /api/v1/connector/pipeline-stages` - Fasi pipeline
- `GET /api/v1/connector/workflows` - Workflow HubSpot

### Mining Processi
- `POST /api/v1/mining/discover` - Process Discovery
- `GET /api/v1/mining/discover/dfg-with-automations/{id}` - DFG con automazioni
- `POST /api/v1/mining/conformance` - Conformance Checking
- `GET /api/v1/mining/variants` - Analisi varianti
- `GET /api/v1/mining/kpi` - Calcolo KPI

### Analytics e Simulazione
- `POST /api/v1/analytics/simulate` - Simulazione What-If
- `POST /api/v1/analytics/simulate/compare` - Confronto scenari
- `GET /api/v1/analytics/health` - Health check analytics

### Data Quality
- `POST /api/v1/dq/validate` - Validazione dati
- `GET /api/v1/dq/report` - Report qualità
- `POST /api/v1/dq/fix` - Correzione dati

### Gestione Processi
- `GET /api/v1/processes` - Lista processi
- `GET /api/v1/processes/{id}` - Dettaglio processo
- `POST /api/v1/processes/analyze` - Analisi processo

## Configurazione

### Variabili Ambiente Backend
```bash
# HubSpot
HUBSPOT_CLIENT_ID=your_client_id
HUBSPOT_CLIENT_SECRET=your_client_secret
HUBSPOT_REDIRECT_URI=http://localhost:8000/api/v1/auth/callback

# Database
DATABASE_URL=sqlite:///./app/data/process_mining.db

# Celery
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0

# Privacy
EMAIL_HASH_SALT=your_salt_here
DATA_RETENTION_DAYS=365
PSEUDONYMIZATION_ENABLED=true
```

### Variabili Ambiente Frontend
```bash
# API Backend
VITE_API_URL=http://localhost:8000/api/v1
```

### Configurazione HubSpot
Il sistema richiede un'app HubSpot configurata con:
- **Scopes**: `crm.objects.deals.read`, `crm.objects.contacts.read`, `crm.objects.companies.read`, `automation`
- **Redirect URI**: Configurato in HubSpot Developer Portal

## Testing

### Test Sistema
```bash
# Test integrazione completo
python -c "from app.core.integration import run_full_system_test_sync; run_full_system_test_sync()"

# Test unitari
poetry run pytest tests/

# Test specifici
poetry run pytest tests/test_etl.py -v
```

### Test API
```bash
# Health check
curl http://localhost:8000/health

# Test autenticazione
curl http://localhost:8000/api/v1/auth/status
```

## Monitoraggio

### Log
- `logs/app.log`: Log principale applicazione
- `logs/integration_tests/`: Risultati test integrazione
- `logs/bootstrap_results/`: Risultati bootstrap

### Health Check
```bash
# Verifica stato sistema
curl http://localhost:8000/health

# Stato servizi
curl http://localhost:8000/
```

## Deployment

### Produzione
```bash
# Build Docker
docker build -t process-mining:latest .

# Deploy
docker-compose -f docker-compose.prod.yml up -d

# Health check
curl https://your-domain.com/health
```

### Configurazione Reverse Proxy (Nginx)
```nginx
server {
    listen 80;
    server_name your-domain.com;
    
    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

## Contribuire

1. Fork repository
2. Crea branch feature: `git checkout -b feature/nome-feature`
3. Commit modifiche: `git commit -m 'Aggiunta feature X'`
4. Push branch: `git push origin feature/nome-feature`
5. Apri Pull Request

## Licenza

MIT License - Vedere file LICENSE per dettagli.

## Supporto

- **Issues**: GitHub Issues
- **Documentazione**: README.md e docs/
- **Test**: `poetry run pytest tests/`