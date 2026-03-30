# Process Mining System - FastAPI Backend

## Descrizione del Progetto

Sistema Process Mining basato su **FastAPI** per l'analisi dei processi aziendali, con integrazione HubSpot tramite OAuth 2.0.

**Stack Tecnologico:**
- Python 3.12
- FastAPI + Uvicorn
- PM4Py (Process Mining)
- Celery + Redis (Task asincroni)
- SQLite (Database)
- Docker (Containerizzazione)

## Architettura del Sistema

### Componenti Principali

1. **API REST (FastAPI)**
   - Endpoint per autenticazione OAuth 2.0
   - Connessione HubSpot CRM
   - Process Discovery e Conformance Checking
   - Data Quality e Privacy Governance

2. **ETL Pipeline**
   - Estrazione dati da HubSpot (Deal, Contact, Company)
   - Trasformazione in event log
   - Validazione qualità dati
   - Pseudonimizzazione GDPR

3. **Process Mining Engine**
   - DFG (Directly-Follows Graph)
   - Alpha Miner, Heuristic Miner, Inductive Miner
   - Analisi varianti processo
   - Calcolo KPI

4. **Task Asincroni (Celery)**
   - Elaborazione ETL in background
   - Mining processi asincrono
   - Controllo qualità periodico
   - Redis come message broker

5. **Privacy e GDPR**
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
│   │   ├── routes_connector.py      # Endpoint HubSpot
│   │   ├── routes_mining.py         # Endpoint mining
│   │   ├── routes_dq.py             # Endpoint data quality
│   │   ├── routes_process_management.py  # Gestione processi
│   │   ├── routes/auth.py           # Autenticazione OAuth
│   │   └── routes_external_cards.py # External cards
│   ├── connectors/                  # Connettori esterni
│   │   ├── hubspot_client.py        # Client HubSpot OAuth
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
│   │   │   ├── data_extraction.py   # Estrazione dati
│   │   │   ├── data_transformation.py # Trasformazione
│   │   │   ├── data_quality.py      # Qualità dati
│   │   │   └── privacy_governance.py # Privacy
│   │   └── mining/                  # Servizi mining
│   │       ├── discovery_service.py # Process Discovery
│   │       ├── conformance_service.py # Conformance
│   │       └── kpi_service.py       # Calcolo KPI
│   ├── tasks/                       # Task Celery
│   │   ├── worker.py                # Worker Celery
│   │   ├── etl_task.py              # Task ETL
│   │   ├── mining_task.py           # Task mining
│   │   └── dq_task.py               # Task data quality
│   └── models/                      # Modelli database
│       └── auth.py                  # Modelli autenticazione
├── data/                            # Directory dati
│   ├── raw/                         # Dati grezzi
│   ├── processed/                   # Dati processati
│   └── warehouse/                   # Data warehouse
├── logs/                            # Log sistema
├── docker-compose.yml               # Configurazione Docker
├── pyproject.toml                   # Configurazione Python
└── README.md                        # Questo file
```

## Installazione e Setup

### Prerequisiti
- Python 3.12+
- Docker & Docker Compose (opzionale)
- HubSpot Developer Account (per integrazione reale)

### Setup Locale

1. **Clona repository**
   ```bash
   git clone <repository-url>
   cd Process-Mining
   ```

2. **Installa dipendenze**
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

4. **Avvia sistema**
   ```bash
   # Avvia API FastAPI
   python main.py
   
   # Avvia worker Celery (opzionale)
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
- `GET /api/v1/auth/hubspot/login` - Inizio OAuth
- `GET /api/v1/auth/callback` - Callback OAuth
- `POST /api/v1/auth/refresh` - Refresh token
- `GET /api/v1/auth/status` - Stato autenticazione

### Connessione HubSpot
- `GET /api/v1/connector/deals` - Lista deal
- `GET /api/v1/connector/contacts` - Lista contatti
- `GET /api/v1/connector/companies` - Lista aziende
- `GET /api/v1/connector/pipeline-stages` - Fasi pipeline

### Mining Processi
- `POST /api/v1/mining/discover` - Process Discovery
- `POST /api/v1/mining/conformance` - Conformance Checking
- `GET /api/v1/mining/variants` - Analisi varianti
- `GET /api/v1/mining/kpi` - Calcolo KPI

### Data Quality
- `POST /api/v1/dq/validate` - Validazione dati
- `GET /api/v1/dq/report` - Report qualità
- `POST /api/v1/dq/fix` - Correzione dati

### Gestione Processi
- `GET /api/v1/processes` - Lista processi
- `GET /api/v1/processes/{id}` - Dettaglio processo
- `POST /api/v1/processes/analyze` - Analisi processo

## Configurazione

### Variabili Ambiente
```bash
# HubSpot OAuth
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

### Configurazione HubSpot
1. Crea app su [developers.hubspot.com](https://developers.hubspot.com)
2. Configura OAuth 2.0 con scopes:
   - `crm.objects.deals.read`
   - `crm.objects.contacts.read`
   - `crm.objects.companies.read`
3. Configura Redirect URI: `http://localhost:8000/api/v1/auth/callback`

## Testing

### Test Sistema
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

## Monitoraggio

### Log Sistema
- `logs/app.log`: Log principale
- `logs/integration_tests/`: Risultati test
- `logs/bootstrap_results/`: Risultati bootstrap

### Health Check
```bash
# Verifica stato sistema
curl http://localhost:8000/health

# Stato endpoint
curl http://localhost:8000/
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

## Troubleshooting

### Problemi Comuni

#### Autenticazione HubSpot
```bash
# Verifica variabili
echo $HUBSPOT_CLIENT_ID
echo $HUBSPOT_CLIENT_SECRET

# Test connessione
curl http://localhost:8000/api/v1/auth/status
```

#### Database SQLite
```bash
# Verifica database
ls -la app/data/

# Ricrea database
rm app/data/process_mining.db
python main.py
```

#### Celery Worker
```bash
# Verifica Redis
redis-cli ping

# Avvia worker debug
poetry run celery -A app.tasks.worker.celery_app worker --loglevel=debug
```

### Debug Mode
```bash
# Log dettagliato
export LOG_LEVEL=DEBUG
python main.py

# Test specifico
poetry run pytest tests/test_etl.py::test_extraction -v -s
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
- **Celery**: https://docs.celeryq.dev
- **HubSpot API**: https://developers.hubspot.com