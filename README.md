# Process Mining System - FastAPI Backend & React Frontend

## Project Description

Process Mining Platform based on **FastAPI** (backend) & **React** (frontend) for the analysis of corporate processes w/Hubspot Integration through OAUTH 2.0.

**Tech Stack:**
- **Backend**: Python 3.12, FastAPI + Uvicorn, PM4Py, SimPy, Celery + Redis, SQLite
- **Frontend**: React 18, TypeScript, @xyflow/react (React Flow), Material-UI, Vite
- **Integration**: HubSpot OAuth 2.0 w/scope `automation` for workflow

## System Architecture

### Main Components

1. **API REST (FastAPI)**
   - Endpoint for OAuth 2.0 authentication;
   - Connection to HubSpot CRM with workflow extraction;
   - Process Discovery & Conformance Checking;
   - Simulation Engine for What-If Analysis;
   - Data Quality & Privacy Governance.

2. **Global Dashboard (React + React Flow)**
   - Full-screen interactive Canvas with process graph;
   - Custom nodes with flagging system for HubSpot Automations;
   - Filter for archs frequency (continuous slider);
   - Sidebar with What-If Analysis.

3. **ETL Pipeline**
   - Data extraction from HubSpot (Deal, Contact, Company, Workflows);
   - transformation in Event Logs;
   - Data quality validation;
   - Pseudonymization as for GDPR regulations.

4. **Process Mining Engine**
   - DFG (Directly-Follows Graph) w/mapped automations;
   - Alpha Miner, Heuristic Miner, Inductive Miner;
   - Process Variants Analysis;
   - KPI calculation.

5. **Simulation Engine (SimPy)**
   - Asynchronous What-If Analysis:
      - Timing;
      - on/off Automations;
      - Transition probability.

6. **Asynchronous Tasks (Celery + Redis)**
   - ETL elaboration in background;
   - Asynchronous Process mining;
   - Asynchronous What-if Analysis.

7. **Privacy & GDPR**
   - Pseudonymization for emails;
   - Data retention policy;
   - Audit log for log-ins;

## Project Structure

```
Process-Mining/
├── main.py                          # Entry point FastAPI
├── app/
│   ├── api/                         # API REST
│   │   ├── main.py                  # App FastAPI
│   │   ├── routes_connector.py      # Endpoint HubSpot (workflow included)
│   │   ├── routes_mining.py         # Endpoint mining (DFG w/automations included)
│   │   ├── routes_analytics.py      # Endpoint analytics (What-If simulation)
│   │   ├── routes_dq.py             # Endpoint data quality
│   │   ├── routes_process_management.py  # Process Management
│   │   ├── routes/auth.py           # OAuth (scope automation)
│   │   └── routes_external_cards.py # External cards
│   ├── connectors/                  # External connectors
│   │   ├── hubspot_client.py        # Client HubSpot OAuth (get_workflows included)
│   │   └── hubspot_mapper.py        # Data mapping
│   ├── core/                        # Core Functionalities
│   │   ├── config.py                # Config
│   │   ├── database.py              # Database SQLite
│   │   ├── logger.py                # Logging
│   │   ├── privacy.py               # Privacy GDPR
│   │   ├── bootstrap.py             # System Bootstrap
│   │   └── integration.py           # Integration Test
│   ├── services/                    # Business Logic
│   │   ├── etl/                     # ETL Services
│   │   │   ├── data_extraction.py   # Data Extraction (Workflow included)
│   │   │   ├── data_transformation.py # Transformation
│   │   │   ├── data_quality.py      # Data Quality
│   │   │   └── privacy_governance.py # Privacy
│   │   ├── mining/                  # Mining Services
│   │   │   ├── discovery_service.py # Process Discovery (w/automation mapping)
│   │   │   ├── conformance_service.py # Conformance
│   │   │   └── kpi_service.py       # KPI Calc
│   │   └── analytics/               # Analytics Services
│   │       └── simulation_service.py # Simulation Engine (SimPy)
│   ├── tasks/                       # Celery tasks
│   │   ├── worker.py                # Celery Worker
│   │   ├── etl_task.py              # ETL tasks(workflow included)
│   │   ├── mining_task.py           # mining tasks
│   │   ├── analytics_task.py        # What-If Simulation Task
│   │   └── dq_task.py               # Data quality task
│   └── models/                      # Database models
│       └── auth.py                  # AUTH Models
├── frontend/                        # React Frontend
│   ├── src/
│   │   ├── components/
│   │   │   ├── ProcessList.tsx      # Process Lists
│   │   │   ├── ProcessAnalysis.tsx  # Canvas React Flow
│   │   │   ├── CustomNode.tsx       # Custom node with Automation Flagging
│   │   │   ├── WhatIfSidebar.tsx    # Sidebar What-If Analysis
│   │   │   └── ProcessDetail.tsx    # Process Detail
│   │   ├── App.tsx                  # Main Routing
│   │   └── index.css                # Styles (React Flow included)
│   └── package.json                 # Dependencies (@xyflow/react included)
├── data/                            # Directory for datas
│   ├── raw/                         # Raw Datas
│   ├── processed/                   # Processed Datas
│   └── warehouse/                   # Data warehouse
├── logs/                            # System Logs
├── docker-compose.yml               # Docker Config
├── pyproject.toml                   # Python Project Config
└── README.md                        # This File
```

## Setup

### Prerequisites
- Python 3.12+
- Node.js 18+
- Docker & Docker Compose (optional)
- HubSpot Developer Account (for proper Integration)

### Setup Backend

1. **Clone repository**
   ```bash
   git clone <repository-url>
   cd Process-Mining
   ```

2. **Install Python Dependencies**
   ```bash
   poetry install
   ```

3. **ENV Config**
   ```bash
   # Create .env file
   HUBSPOT_CLIENT_ID=your_client_id
   HUBSPOT_CLIENT_SECRET=your_client_secret
   HUBSPOT_REDIRECT_URI=http://localhost:8000/api/v1/auth/callback
   DATABASE_URL=sqlite:///./app/data/process_mining.db
   CELERY_BROKER_URL=redis://localhost:6379/0
   EMAIL_HASH_SALT=your_salt_here
   ```

4. **Start backend**
   ```bash
   python main.py
   ```

### Setup Frontend

1. **Get in Frontend dir**
   ```bash
   cd frontend
   ```

2. **Install Node Dependencies**
   ```bash
   npm install
   ```

3. **Config ENV Variables**
   ```bash
   # Create .env file in frontend/ dir
   echo "VITE_API_BASE_URL=http://localhost:8000/api/v1" > .env
   echo "VITE_API_URL=http://localhost:8000/api/v1" >> .env
   ```

4. **Start frontend**
   ```bash
   npm run dev
   ```

### OAuth 2.0

The system uses **OAuth 2.0 with HubSpot** for authentication. The flow is as follows:

1. **Login**: The user accesses the frontend and is redirected to HubSpot for authorization
2. **Callback**: HubSpot redirects to `/auth/success` with a JWT token
3. **Saving**: The token is saved in `localStorage` as `'token'`
4. **Usage**: Every API call automatically sends the token via an **Axios Request Interceptor**
5. **Verification**: The backend verifies the JWT token on every protected request

#### Frontend Config

The frontend uses a **synchronous ProtectedRoute** that:
- Checks for the presence of the token in `localStorage` upon startup
- If the token is present, it grants access immediately (no API call)
- If the token is absent, it redirects to the HubSpot login
- The actual authorization occurs when individual APIs return 401 if the token is expired/invalid

#### Frontend Environment Variables

| Var | DESC | Default |
|-----------|-------------|---------|
| `VITE_API_BASE_URL` | URL of backend API | `http://localhost:8000/api/v1` |
| `VITE_API_URL` | Complete URL for auth redirect | `http://localhost:8000/api/v1` |
| `VITE_HUBSPOT_CLIENT_ID` | Client ID app HubSpot | - |
| `VITE_HUBSPOT_REDIRECT_URI` | URI redirect OAuth | auto-generating |

### Docker Setup

```bash
# starts all services
docker-compose up -d

# state verification
docker-compose ps

# Log
docker-compose logs -f
```

## API Endpoints

### Auth
- `GET /api/v1/auth/hubspot/login` - Beginning OAuth (scope `automation`)
- `GET /api/v1/auth/callback` - Callback OAuth
- `POST /api/v1/auth/refresh` - Refresh token
- `GET /api/v1/auth/status` - state auth

### HubSpot Connection
- `GET /api/v1/connector/deals` - deal list
- `GET /api/v1/connector/contacts` - contact list
- `GET /api/v1/connector/companies` - company list
- `GET /api/v1/connector/pipeline-stages` - pipeline phases
- `GET /api/v1/connector/workflows` - Workflow HubSpot

### Process Mining
- `POST /api/v1/mining/discover` - Process Discovery
- `GET /api/v1/mining/discover/dfg-with-automations/{id}` - DFG with automations
- `POST /api/v1/mining/conformance` - Conformance Checking
- `GET /api/v1/mining/variants` - Variant analysis
- `GET /api/v1/mining/kpi` - KPI calculation

### Analytics & Simulation
- `POST /api/v1/analytics/simulate` - What-If Sim
- `POST /api/v1/analytics/simulate/compare` - Compare scenarios
- `GET /api/v1/analytics/health` - Health check analytics

### Data Quality
- `POST /api/v1/dq/validate` - Data Validation
- `GET /api/v1/dq/report` - Quality report
- `POST /api/v1/dq/fix` - Data fix

### Process Management
- `GET /api/v1/processes` - process list
- `GET /api/v1/processes/{id}` - process detail
- `POST /api/v1/processes/analyze` - process analysis

## HubSpot config

### Necessary Scopes
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

### App Config
1. Create an app on [developers.hubspot.com](https://developers.hubspot.com)
2. Configure OAuth 2.0 with all scopes
3. Configure Redirect URI: `http://localhost:8000/api/v1/auth/callback`

## Testing

### Test Backend
```bash
# Full integration test
python -c "from app.core.integration import run_full_system_test_sync; run_full_system_test_sync()"

# Unit tests
poetry run pytest tests/

# Specific tests
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
# Build image
docker build -t process-mining:latest .

# Deploy with docker-compose
docker-compose -f docker-compose.prod.yml up -d

# Health check http://localhost:8000/api/v1/auth/callback` 
curl https://your-domain.com/health
```

## License

MIT License - see LICENSE file

## References

- **FastAPI**: https://fastapi.tiangolo.com
- **PM4Py**: https://pm4py.fit.fraunhofer.de
- **SimPy**: https://simpy.readthedocs.io
- **React Flow**: https://reactflow.dev
- **Celery**: https://docs.celeryq.dev
- **HubSpot API**: https://developers.hubspot.com
