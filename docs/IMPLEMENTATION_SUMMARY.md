# Summary: Implementazione Process Mining con HubSpot Integration

## Panoramica del Progetto

Sistema Process Mining completo basato su **FastAPI** (backend) e **React** (frontend) che integra HubSpot tramite OAuth 2.0 per l'analisi dei processi aziendali con funzionalità avanzate di simulazione What-If.

## Architettura del Sistema

### Backend (FastAPI)
- **Linguaggio**: Python 3.12
- **Framework**: FastAPI + Uvicorn
- **Database**: SQLite
- **Porta**: 8000
- **Struttura**: Modulare con separazione tra API, servizi e core

### Frontend (React)
- **Framework**: React 18 + TypeScript
- **Build Tool**: Vite
- **Librerie**: @xyflow/react (React Flow), Material-UI, Axios
- **Porta**: 5173 (default Vite)

### Integrazione HubSpot
- **Metodo**: OAuth 2.0
- **Scope**: Incluso `automation` per workflow
- **Client**: `app/connectors/hubspot_client.py`
- **Database**: SQLite per storage token

### Process Mining
- **Library**: PM4Py
- **Algoritmi**: DFG, Alpha Miner, Heuristic Miner, Inductive Miner
- **Analisi**: Conformance Checking, Variant Analysis, KPI Calculation
- **Integrazione**: Mapping automazioni sui nodi del grafo

### Simulation Engine
- **Library**: SimPy
- **Funzionalità**: What-If Analysis asincrona
- **Modifiche**: Tempi, probabilità, automazioni
- **Task**: Esecuzione asincrona con Celery

## Componenti Implementati

### 1. API REST (FastAPI)

#### Core System
- **FastAPI App**: `app/api/main.py`
- **Database**: SQLite con SQLAlchemy
- **Configurazione**: Centralizzata in `app/core/config.py`
- **Logger**: Logging strutturato
- **Sicurezza**: CORS, gestione errori

#### API Endpoints
- **Autenticazione**: OAuth 2.0 flow completo (scope `automation`)
- **Connector**: Estrazione dati HubSpot (incluso workflows)
- **Mining**: Process Discovery con automazioni mappate
- **Analytics**: Simulazione What-If
- **Data Quality**: Validazione e report
- **Process Management**: Gestione processi

### 2. Connettori

#### HubSpot Client (`app/connectors/hubspot_client.py`)
- **OAuth 2.0**: Autenticazione completa
- **Token Management**: Refresh automatico
- **Rate Limiting**: Gestione limiti API
- **Retry**: Backoff esponenziale
- **Metodi**:
  - `get_deals()`: Estrazione deal
  - `get_contacts()`: Estrazione contatti
  - `get_companies()`: Estrazione aziende
  - `get_pipeline_stages()`: Estrazione pipeline
  - `get_timeline_events()`: Estrazione eventi
  - `get_workflows()`: Estrazione workflow di automazione

#### HubSpot Mapper (`app/connectors/hubspot_mapper.py`)
- Mapping proprietà HubSpot → event log
- Supporto diversi template pipeline
- Conversione formato dati

### 3. Servizi ETL

#### Data Extraction (`app/services/etl/data_extraction.py`)
- Estrazione deal con cronologia
- Estrazione contatti e aziende
- Estrazione workflow di automazione
- Paginazione automatica
- Salvataggio JSON con timestamp

#### Data Transformation (`app/services/etl/data_transformation.py`)
- Conversione JSON → event log
- Validazione schema
- Pseudonimizzazione (opzionale)
- Salvataggio Parquet

#### Data Quality (`app/services/etl/data_quality.py`)
- Validazione schema event log
- Controllo completezza dati
- Controllo consistenza
- Generazione report qualità

#### Privacy Governance (`app/services/etl/privacy_governance.py`)
- Pseudonimizzazione email
- Data retention policy
- Audit log accessi
- Validazione GDPR compliance

### 4. Servizi Mining

#### Discovery Service (`app/services/mining/discovery_service.py`)
- **DFG**: Directly-Follows Graph con automazioni mappate
- **Alpha Miner**: Discovery con algoritmo Alpha
- **Heuristic Miner**: Discovery con euristica
- **Inductive Miner**: Discovery induttivo
- **Variant Analysis**: Analisi varianti processo
- **Mapping Automazioni**: `_map_workflows_to_nodes()` per associare workflow ai nodi

#### Conformance Service (`app/services/mining/conformance_service.py`)
- Conformance checking DFG
- Conformance checking Petri Net
- Deviation pattern detection
- Fitness e precision calculation

#### KPI Service (`app/services/mining/kpi_service.py`)
- Calcolo KPI processo
- Metriche performance
- Analisi colli di bottiglia

### 5. Servizi Analytics

#### Simulation Service (`app/services/analytics/simulation_service.py`)
- **Motore SimPy**: Simulazione processi discreta
- **What-If Analysis**: Modifica tempi e probabilità
- **Gestione Automazioni**: Disabilitazione/override workflow
- **Calcolo Metriche**: Cycle time medio, improvement percentage
- **Riproducibilità**: Seed configurabile per risultati deterministici

### 6. Task Asincroni (Celery)

#### Worker (`app/tasks/worker.py`)
- Configurazione Celery
- Redis come broker
- Task concurrency
- Monitoring task

#### Task ETL (`app/tasks/etl_task.py`)
- Estrazione dati asincrona (incluso workflow)
- Monitoraggio nuovi file
- Elaborazione batch

#### Task Mining (`app/tasks/mining_task.py`)
- Process discovery asincrono
- Conformance checking asincrono
- Calcolo KPI asincrono

#### Task Analytics (`app/tasks/analytics_task.py`)
- Simulazione What-If asincrona
- Confronto scenari
- Progress tracking

#### Task Data Quality (`app/tasks/dq_task.py`)
- Validazione asincrona
- Report qualità periodici

### 7. Core System

#### Configurazione (`app/core/config.py`)
- Variabili ambiente
- Path directory
- Configurazione database
- Privacy settings
- Mining settings

#### Database (`app/core/database.py`)
- Setup SQLite
- Sessioni asincrone
- Creazione tabelle

#### Privacy (`app/core/privacy.py`)
- Hash email con salt
- Pseudonimizzazione DataFrame
- Data retention
- Audit log

#### Bootstrap (`app/core/bootstrap.py`)
- Setup directory
- Validazione configurazione
- Auto-discovery HubSpot

#### Integration (`app/core/integration.py`)
- Test sistema integrato
- Health check componenti
- Monitoring stato

### 8. Frontend React

#### Componenti
- **ProcessList**: Lista processi con ricerca e statistiche
- **ProcessAnalysis**: Canvas React Flow full-screen con grafo interattivo
- **CustomNode**: Nodo custom con badge automazioni ⚡️
- **WhatIfSidebar**: Sidebar per analisi What-If con slider e toggle
- **ProcessDetail**: Dettaglio processo

#### Funzionalità Dashboard
- **React Flow Canvas**: Visualizzazione grafo processo
- **Filtro Frequenza**: Slider continuo 0-100%
- **Badge Automazioni**: ⚡️ sui nodi con workflow attivi
- **Sidebar What-If**: Controlli per simulazione
- **MiniMap**: Navigazione stile Google Maps
- **Controls**: Zoom, pan, fit view

## API Endpoints Implementati

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

## Tecnologie Utilizzate

### Backend Stack
- **Python 3.12**: Linguaggio principale
- **FastAPI**: Framework web asincrono
- **SQLite**: Database
- **SQLAlchemy**: ORM
- **Pydantic**: Validazione dati
- **PM4Py**: Process Mining library
- **SimPy**: Simulation library
- **Celery**: Task asincroni
- **Redis**: Message broker
- **httpx**: HTTP client asincrono

### Frontend Stack
- **React 18**: UI framework
- **TypeScript**: Type safety
- **Vite**: Build tool
- **@xyflow/react**: Grafi interattivi (React Flow)
- **Material-UI**: Componenti UI
- **Axios**: HTTP client

### DevOps
- **Docker**: Containerizzazione
- **Docker Compose**: Orchestrazione
- **Poetry**: Dependency management (Python)
- **npm**: Dependency management (Node.js)

## Struttura File System

```
Process-Mining/
├── main.py                          # Entry point FastAPI
├── app/
│   ├── api/                         # API REST
│   │   ├── main.py                  # App FastAPI
│   │   ├── routes_connector.py      # Endpoint HubSpot (incluso workflows)
│   │   ├── routes_mining.py         # Endpoint mining (DFG con automazioni)
│   │   ├── routes_analytics.py      # Endpoint analytics (simulazione)
│   │   ├── routes_dq.py             # Endpoint data quality
│   │   ├── routes_process_management.py  # Gestione processi
│   │   ├── routes/auth.py           # Autenticazione OAuth
│   │   └── routes_external_cards.py # External cards
│   ├── connectors/                  # Connettori esterni
│   │   ├── hubspot_client.py        # Client HubSpot OAuth (get_workflows)
│   │   └── hubspot_mapper.py        # Mapping dati
│   ├── core/                        # Funzionalità core
│   │   ├── config.py                # Configurazione
│   │   ├── database.py              # Database SQLite
│   │   ├── logger.py                # Logging
│   │   ├── privacy.py               # Privacy GDPR
│   │   ├── bootstrap.py             # Bootstrap sistema
│   │   └── integration.py           # Test integrazione
│   ├── services/                    # Logica business
│   │   ├── etl/                     # Servizi ETL (incluso workflows)
│   │   ├── mining/                  # Servizi mining (mapping automazioni)
│   │   └── analytics/               # Servizi analytics (simulazione)
│   ├── tasks/                       # Task Celery (incluso analytics)
│   └── models/                      # Modelli database
├── frontend/                        # React Frontend
│   ├── src/
│   │   ├── components/
│   │   │   ├── ProcessList.tsx      # Lista processi
│   │   │   ├── ProcessAnalysis.tsx  # Canvas React Flow
│   │   │   ├── CustomNode.tsx       # Nodo custom con badge
│   │   │   ├── WhatIfSidebar.tsx    # Sidebar What-If
│   │   │   └── ProcessDetail.tsx    # Dettaglio processo
│   │   ├── App.tsx                  # Routing
│   │   └── index.css                # Stili
│   └── package.json                 # Dipendenze
├── data/                            # Directory dati
├── logs/                            # Log sistema
├── docker-compose.yml               # Configurazione Docker
├── pyproject.toml                   # Configurazione Python
└── README.md                        # Documentazione
```

## Feature Implementate

### ✅ Completed Features

1. **Backend FastAPI**
   - [x] API REST completa
   - [x] Database SQLite
   - [x] Autenticazione OAuth 2.0 (scope automation)
   - [x] Logging e monitoring
   - [x] Error handling

2. **Integrazione HubSpot**
   - [x] Client OAuth 2.0
   - [x] Estrazione deal, contatti, aziende
   - [x] Pipeline stages
   - [x] Timeline events
   - [x] **Workflow extraction** (get_workflows)
   - [x] Rate limiting

3. **ETL Pipeline**
   - [x] Estrazione dati (incluso workflows)
   - [x] Trasformazione event log
   - [x] Validazione qualità
   - [x] Privacy governance

4. **Process Mining**
   - [x] Process Discovery (DFG, Alpha, Heuristic, Inductive)
   - [x] **Mapping automazioni sui nodi**
   - [x] Conformance Checking
   - [x] Variant Analysis
   - [x] KPI Calculation

5. **Simulazione What-If**
   - [x] **Simulation Engine (SimPy)**
   - [x] **Modifica tempi nodi**
   - [x] **Modifica probabilità transizione**
   - [x] **Disabilitazione automazioni**
   - [x] **Override delay automazioni**
   - [x] **Task asincroni simulazione**

6. **Dashboard Interattiva**
   - [x] **React Flow Canvas**
   - [x] **Nodi custom con badge automazioni**
   - [x] **Filtro frequenza archi**
   - [x] **Sidebar What-If Analysis**
   - [x] **MiniMap e Controls**
   - [x] **Progress indicator simulazione**

7. **Task Asincroni**
   - [x] Celery worker
   - [x] ETL tasks (incluso workflows)
   - [x] Mining tasks
   - [x] **Analytics tasks (simulazione)**
   - [x] Data quality tasks

8. **Privacy e GDPR**
   - [x] Pseudonimizzazione email
   - [x] Data retention policy
   - [x] Audit log
   - [x] Compliance validation

## Configurazione

### Variabili Ambiente Backend
```bash
# HubSpot OAuth
HUBSPOT_CLIENT_ID=your_client_id
HUBSPOT_CLIENT_SECRET=your_client_secret
HUBSPOT_REDIRECT_URI=http://localhost:8000/api/v1/auth/callback

# Database
DATABASE_URL=sqlite:///./app/data/process_mining.db

# Celery
CELERY_BROKER_URL=redis://localhost:6379/0

# Privacy
EMAIL_HASH_SALT=your_salt_here
DATA_RETENTION_DAYS=365
```

### Variabili Ambiente Frontend
```bash
# API Backend
VITE_API_URL=http://localhost:8000/api/v1
```

## Testing

### Test Implementati
- Unit test per servizi ETL
- Unit test per servizi mining
- Integration test sistema completo
- Test API endpoints

### Comandi Test
```bash
# Test unitari
poetry run pytest tests/

# Test specifici
poetry run pytest tests/test_etl.py -v
poetry run pytest tests/test_mining.py -v

# Test integrazione
python -c "from app.core.integration import run_full_system_test_sync; run_full_system_test_sync()"
```

## Deploy

### Docker
```bash
# Avvia tutti i servizi
docker-compose up -d

# Verifica stato
docker-compose ps

# Log
docker-compose logs -f
```

### Produzione
```bash
# Build immagine
docker build -t process-mining:latest .

# Deploy
docker-compose -f docker-compose.prod.yml up -d
```

## Monitoraggio

### Health Check
```bash
# Verifica salute sistema
curl http://localhost:8000/health

# Stato endpoint
curl http://localhost:8000/
```

### Log
- `logs/app.log`: Log principale
- `logs/integration_tests/`: Risultati test
- `logs/bootstrap_results/`: Risultati bootstrap

## Prossimi Passi

### Miglioramenti Futuri
1. **Database Migliorato**: PostgreSQL per produzione
2. **Autenticazione Avanzata**: JWT refresh token, multi-factor
3. **Monitoring**: Dashboard Prometheus/Grafana
4. **Testing**: Copertura test > 80%
5. **Documentazione**: API docs con Swagger/ReDoc

### Estensioni
1. **Multi-tenant**: Supporto multi-azienda
2. **Real-time**: WebSocket per aggiornamenti live
3. **ML Integration**: Predizioni con scikit-learn
4. **Export**: PDF/Excel reports
5. **Mobile**: App React Native

## Conclusioni

Il sistema Process Mining è **completamente implementato** e **funzionante** con:

- ✅ **Backend FastAPI** completo con tutti gli endpoint
- ✅ **Frontend React** con Dashboard interattiva
- ✅ **Integrazione HubSpot** OAuth 2.0 con workflows
- ✅ **ETL Pipeline** funzionante
- ✅ **Process Mining** con PM4Py
- ✅ **Simulazione What-If** con SimPy
- ✅ **Dashboard Interattiva** con React Flow
- ✅ **Data Quality** e Privacy
- ✅ **Task asincroni** con Celery
- ✅ **Docker** deployment ready

Il sistema è pronto per essere **deployato in produzione** e utilizzato per l'analisi dei processi aziendali.