# Process Mining System - FastAPI Backend & React Frontend

## Descrizione del Progetto

Sistema Process Mining completo basato su **FastAPI** (backend) e **React** (frontend) per l'analisi dei processi aziendali con integrazione HubSpot tramite OAuth 2.0.

**Stack Tecnologico:**
- **Backend**: Python 3.12, FastAPI + Uvicorn, PM4Py, SimPy, Celery + Redis, SQLite
- **Frontend**: React 18, TypeScript, @xyflow/react (React Flow), Material-UI, Vite
- **Integrazione**: HubSpot OAuth 2.0 con scope `automation` per workflow

## Architettura del Sistema

### Componenti Principali

1. **API REST (FastAPI)**
   - Endpoint per autenticazione OAuth 2.0
   - Connessione HubSpot CRM con estrazione workflow
   - Process Discovery e Conformance Checking
   - Simulation Engine per What-If Analysis
   - Data Quality e Privacy Governance

2. **Dashboard Globale (React + React Flow)**
   - Canvas interattivo full-screen con grafo processo
   - Nodi custom con badge automazioni HubSpot
   - Filtro frequenza archi (slider continuo)
   - Sidebar What-If Analysis
   - MiniMap, Controls e Background per navigazione

3. **ETL Pipeline**
   - Estrazione dati da HubSpot (Deal, Contact, Company, Workflows)
   - Trasformazione in event log
   - Validazione qualità dati
   - Pseudonimizzazione GDPR

4. **Process Mining Engine**
   - DFG (Directly-Follows Graph) con automazioni mappate
   - Alpha Miner, Heuristic Miner, Inductive Miner
   - Analisi varianti processo
   - Calcolo KPI

5. **Simulation Engine (SimPy)**
   - Simulazione What-If Analysis asincrona
   - Modifica tempi di esecuzione nodi
   - Modifica probabilità di transizione
   - Disabilitazione/override automazioni HubSpot
   - Task asincroni con Celery

6. **Task Asincroni (Celery + Redis)**
   - Elaborazione ETL in background
   - Mining processi asincrono
   - Simulazione What-If asincrona
   - Controllo qualità periodico

7. **Privacy e GDPR**
   - Pseudonimizzazione email
   - Data retention policy
   - Audit log accessi
   - Validazione compliance

## Struttura del Progetto

```
Process-Mining/
├── main.py                          # Entry point FastAPI
├── app/
│   ├── api/                         # API REST
│   │   ├── main.py                  # App FastAPI
│   │   ├── routes_connector.py      # Endpoint HubSpot (incluso workflow)
│   │   ├── routes_mining.py         # Endpoint mining (incluso DFG con automazioni)
│   │   ├── routes_analytics.py      # Endpoint analytics (simulazione What-If)
│   │   ├── routes_dq.py             # Endpoint data quality
│   │   ├── routes_process_management.py  # Gestione processi
│   │   ├── routes/auth.py           # Autenticazione OAuth (scope automation)
│   │   └── routes_external_cards.py # External cards
│   ├── connectors/                  # Connettori esterni
│   │   ├── hubspot_client.py        # Client HubSpot OAuth (incluso get_workflows)
│   │   └── hubspot_mapper.py        # Mapping dati
│   ├── core/                        # Funzionalità core
│   │   ├── config.py                # Configurazione
│   │   ├── database.py              # Database SQLite
│   │   ├── logger.py                # Logging
│   │   ├── privacy.py               # Privacy GDPR
│   │   ├── bootstrap.py             # Bootstrap sistema
│   │   └── integration.py           # Test integrazione
│   ├── services/                    # Logica business
│   │   ├── etl/                     # Servizi ETL
│   │   │   ├── data_extraction.py   # Estrazione dati (incluso workflow)
│   │   │   ├── data_transformation.py # Trasformazione
│   │   │   ├── data_quality.py      # Qualità dati
│   │   │   └── privacy_governance.py # Privacy
│   │   ├── mining/                  # Servizi mining
│   │   │   ├── discovery_service.py # Process Discovery (con mapping automazioni)
│   │   │   ├── conformance_service.py # Conformance
│   │   │   └── kpi_service.py       # Calcolo KPI
│   │   └── analytics/               # Servizi analytics
│   │       └── simulation_service.py # Simulation Engine (SimPy)
│   ├── tasks/                       # Task Celery
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
│   │   │   ├── ProcessAnalysis.tsx  # Canvas React Flow
│   │   │   ├── CustomNode.tsx       # Nodo custom con badge automazioni
│   │   │   ├── WhatIfSidebar.tsx    # Sidebar What-If Analysis
│   │   │   └── ProcessDetail.tsx    # Dettaglio processo
│   │   ├── App.tsx                  # Routing principale
│   │   └── index.css                # Stili (inclusi React Flow)
│   └── package.json                 # Dipendenze (incluso @xyflow/react)
├── data/                            # Directory dati
│   ├── raw/                         # Dati grezzi (inclusi workflow JSON)
│   ├── processed/                   # Dati processati
│   └── warehouse/                   # Data warehouse
├── logs/                            # Log sistema
├── docker-compose.yml               # Configurazione Docker
├── pyproject.toml                   # Configurazione Python (incluso SimPy)
└── README.md                        # Questo file
```

## Installazione e Setup

### Prerequisiti
- Python 3.12+
- Node.js 18+
- Docker & Docker Compose (opzionale)
- HubSpot Developer Account (per integrazione reale)

### Setup Backend

1. **Clona repository**
   ```bash
   git clone <repository-url>
   cd Process-Mining
   ```

2. **Installa dipendenze Python**
   ```bash
   poetry install
   ```

3. **Configura ambiente**
   ```bash
   # Crea file .env
   HUBSPOT_CLIENT_ID=your_client_id
   HUBSPOT_CLIENT_SECRET=your_client_secret
   HUBSPOT_REDIRECT_URI=http://localhost:8000/api/v1/auth/callback
   DATABASE_URL=sqlite:///./app/data/process_mining.db
   CELERY_BROKER_URL=redis://localhost:6379/0
   EMAIL_HASH_SALT=your_salt_here
   ```

4. **Avvia backend**
   ```bash
   python main.py
   ```

### Setup Frontend

1. **Entra nella directory frontend**
   ```bash
   cd frontend
   ```

2. **Installa dipendenze Node**
   ```bash
   npm install
   ```

3. **Configura variabili ambiente**
   ```bash
   # Crea file .env nella directory frontend/
   echo "VITE_API_BASE_URL=http://localhost:8000/api/v1" > .env
   echo "VITE_API_URL=http://localhost:8000/api/v1" >> .env
   ```

4. **Avvia frontend**
   ```bash
   npm run dev
   ```

### Autenticazione OAuth 2.0

Il sistema utilizza **OAuth 2.0 con HubSpot** per l'autenticazione. Il flusso è il seguente:

1. **Login**: L'utente accede al frontend e viene reindirizzato a HubSpot per l'autorizzazione
2. **Callback**: HubSpot reindirizza a `/auth/success` con un JWT token
3. **Salvataggio**: Il token viene salvato in `localStorage` come `'token'`
4. **Utilizzo**: Ogni chiamata API invia automaticamente il token tramite un **Axios Request Interceptor**
5. **Verifica**: Il backend verifica il token JWT su ogni richiesta protetta

#### Configurazione Frontend

Il frontend utilizza un **ProtectedRoute sincrono** che:
- Controlla la presenza del token in `localStorage` all'avvio
- Se il token è presente, consente l'accesso immediatamente (no chiamata API)
- Se il token è assente, reindirizza al login HubSpot
- L'autorizzazione reale avviene quando le singole API restituiscono 401 se il token è scaduto/invalido

#### Variabili d'Ambiente Frontend

| Variabile | Descrizione | Default |
|-----------|-------------|---------|
| `VITE_API_BASE_URL` | URL base del backend API | `http://localhost:8000/api/v1` |
| `VITE_API_URL` | URL completo per redirect auth | `http://localhost:8000/api/v1` |
| `VITE_HUBSPOT_CLIENT_ID` | Client ID app HubSpot | - |
| `VITE_HUBSPOT_REDIRECT_URI` | URI redirect OAuth | Auto-generato |

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

## Configurazione HubSpot

### Scopes Necessari
```bash
crm.objects.deals.read
crm.objects.deals.write
crm.objects.contacts.read
crm.objects.contacts.write
crm.objects.companies.read
timeline
settings.users.read
automation
```

### Configurazione App
1. Crea app su [developers.hubspot.com](https://developers.hubspot.com)
2. Configura OAuth 2.0 con tutti gli scopes
3. Configura Redirect URI: `http://localhost:8000/api/v1/auth/callback`

## Testing

### Test Backend
```bash
# Test integrazione completo
python -c "from app.core.integration import run_full_system_test_sync; run_full_system_test_sync()"

# Test unitari
poetry run pytest tests/

# Test specifici
poetry run pytest tests/test_etl.py -v
poetry run pytest tests/test_mining.py -v
```

### Test API
```bash
# Health check
curl http://localhost:8000/health

# Test endpoint
curl http://localhost:8000/api/v1/auth/status
```

## Deployment

### Docker Production
```bash
# Build immagine
docker build -t process-mining:latest .

# Deploy con docker-compose
docker-compose -f docker-compose.prod.yml up -d

# Health check
curl https://your-domain.com/health
```

### Reverse Proxy (Nginx)
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
2. Crea branch: `git checkout -b feature/nome-feature`
3. Commit: `git commit -m 'Aggiunta feature X'`
4. Push: `git push origin feature/nome-feature`
5. Pull Request

## Licenza

MIT License - Vedere file LICENSE

## Riferimenti

- **FastAPI**: https://fastapi.tiangolo.com
- **PM4Py**: https://pm4py.fit.fraunhofer.de
- **SimPy**: https://simpy.readthedocs.io
- **React Flow**: https://reactflow.dev
- **Celery**: https://docs.celeryq.dev
- **HubSpot API**: https://developers.hubspot.com