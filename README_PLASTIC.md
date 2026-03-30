# Process Mining System - Sistema FastAPI

## Panoramica

Questo è un sistema Process Mining basato su **FastAPI** che integra HubSpot tramite OAuth 2.0 per l'analisi dei processi aziendali. Il sistema è progettato per:

- **Integrazione HubSpot**: Connessione OAuth 2.0 con HubSpot CRM
- **ETL Pipeline**: Estrazione, trasformazione e caricamento dati
- **Process Discovery**: Algoritmi PM4Py per analisi processi
- **Data Quality**: Validazione e controllo qualità dati
- **Privacy by Design**: Pseudonimizzazione e compliance GDPR

## Architettura del Sistema

```
Process-Mining/
├── main.py                          # Entry point FastAPI
├── app/
│   ├── api/                         # API REST (FastAPI)
│   │   ├── main.py                  # App FastAPI principale
│   │   ├── routes_connector.py      # Endpoint connettore HubSpot
│   │   ├── routes_mining.py         # Endpoint mining processi
│   │   ├── routes_dq.py             # Endpoint data quality
│   │   ├── routes_process_management.py  # Endpoint gestione processi
│   │   ├── routes/auth.py           # Endpoint autenticazione OAuth
│   │   └── routes_external_cards.py # Endpoint external cards
│   ├── connectors/                  # Connettori esterni
│   │   ├── hubspot_client.py        # Client HubSpot OAuth 2.0
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
│   │   │   ├── data_extraction.py   # Estrazione dati HubSpot
│   │   │   ├── data_transformation.py # Trasformazione dati
│   │   │   ├── data_quality.py      # Validazione qualità
│   │   │   └── privacy_governance.py # Governance privacy
│   │   └── mining/                  # Servizi mining
│   │       ├── discovery_service.py # Process Discovery
│   │       ├── conformance_service.py # Conformance Checking
│   │       └── kpi_service.py       # Calcolo KPI
│   ├── tasks/                       # Task asincroni (Celery)
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
└── README.md                        # Documentazione principale
```

## Caratteristiche Implementate

### 🔐 Autenticazione OAuth 2.0
- **HubSpot OAuth**: Autenticazione completa con HubSpot
- **Token Management**: Gestione automatica refresh token
- **Database SQLite**: Storage token e sessioni
- **Sicurezza**: HTTPS, CORS, gestione errori

### 📊 Estrazione Dati HubSpot
- **Deal Extraction**: Estrazione deal con cronologia
- **Contact Extraction**: Estrazione contatti
- **Company Extraction**: Estrazione aziende
- **Pipeline Stages**: Estrazione fasi pipeline
- **Paginazione Automatica**: Gestione grandi volumi dati

### 🔍 Process Discovery
- **DFG (Directly-Follows Graph)**: Generazione grafi processo
- **Alpha Miner**: Discovery con algoritmo Alpha
- **Heuristic Miner**: Discovery con euristica
- **Inductive Miner**: Discovery induttivo
- **Variant Analysis**: Analisi varianti processo

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
- **Data Quality Tasks**: Controlli qualità asincroni
- **Redis Broker**: Gestione code messaggi

## Installazione e Configurazione

### Prerequisiti
- Python 3.12+
- Docker & Docker Compose (opzionale)
- HubSpot API Key (per integrazione reale)

### Setup Rapido

1. **Clona il repository**
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
   cp .env.example .env
   # Configura variabili HubSpot
   HUBSPOT_CLIENT_ID=your_client_id
   HUBSPOT_CLIENT_SECRET=your_client_secret
   HUBSPOT_REDIRECT_URI=http://localhost:8000/api/v1/auth/callback
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

### Configurazione HubSpot
Il sistema richiede un'app HubSpot configurata con:
- **Scopes**: `crm.objects.deals.read`, `crm.objects.contacts.read`, `crm.objects.companies.read`
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

## Troubleshooting

### Problemi Comuni

#### Errore Autenticazione HubSpot
```bash
# Verifica variabili ambiente
echo $HUBSPOT_CLIENT_ID
echo $HUBSPOT_CLIENT_SECRET

# Test connessione
curl http://localhost:8000/api/v1/auth/status
```

#### Errore Database
```bash
# Verifica database
ls -la app/data/

# Ricrea database
rm app/data/process_mining.db
python main.py
```

#### Errore Celery Worker
```bash
# Verifica Redis
redis-cli ping

# Avvia worker manuale
poetry run celery -A app.tasks.worker.celery_app worker --loglevel=debug
```

### Debug
```bash
# Log dettagliato
export LOG_LEVEL=DEBUG
python main.py

# Test specifico
poetry run pytest tests/test_etl.py::test_extraction -v -s
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