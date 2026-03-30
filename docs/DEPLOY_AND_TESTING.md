# Deploy e Testing - Process Mining System

## Panoramica

Procedura per deployare e testare il sistema Process Mining basato su FastAPI con integrazione HubSpot.

## Architettura Deploy

Il sistema è composto da 2 componenti principali:
1. **Backend FastAPI** (Port 8000)
2. **Redis** (per task Celery)

**Nota:** Il sistema usa SQLite come database (non PostgreSQL).

## Prerequisiti

### Tecnici
- Python 3.12+
- Docker & Docker Compose
- HubSpot Developer Account (per integrazione reale)

### HubSpot
- App HubSpot con OAuth 2.0 configurato
- Scopes: `crm.objects.deals.read`, `crm.objects.contacts.read`, `crm.objects.companies.read`
- Redirect URI: `http://localhost:8000/api/v1/auth/callback`

## Opzioni di Deploy

### Opzione 1: Docker Compose (Consigliata)

#### 1.1 Configurazione Ambiente
```bash
# Crea file .env
HUBSPOT_CLIENT_ID=your_client_id
HUBSPOT_CLIENT_SECRET=your_client_secret
HUBSPOT_REDIRECT_URI=http://localhost:8000/api/v1/auth/callback
DATABASE_URL=sqlite:///./app/data/process_mining.db
CELERY_BROKER_URL=redis://redis:6379/0
EMAIL_HASH_SALT=your_salt_here
DATA_RETENTION_DAYS=365
```

#### 1.2 Avvio con Docker Compose
```bash
# Avvia tutti i servizi
docker-compose up -d

# Verifica stato
docker-compose ps

# Log
docker-compose logs -f
```

#### 1.3 Verifica Deploy
```bash
# Test backend API
curl http://localhost:8000/health

# Test API
curl http://localhost:8000/api/v1/auth/status
```

### Opzione 2: Deploy Manuale

#### 2.1 Backend
```bash
# Installa dipendenze
poetry install

# Configura ambiente
cp .env.example .env
# Modifica .env con i tuoi valori

# Avvia backend
python main.py
```

#### 2.2 Celery Worker (Opzionale)
```bash
# In un altro terminale
poetry run celery -A app.tasks.worker.celery_app worker --loglevel=info
```

## Testing del Sistema

### Test 1: Health Check
```bash
# Test salute sistema
curl http://localhost:8000/health

# Risposta attesa
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z",
  "services": {
    "api": "running",
    "database": "connected",
    "hubspot": "available"
  }
}
```

### Test 2: API Endpoints
```bash
# Test endpoint root
curl http://localhost:8000/

# Test autenticazione
curl http://localhost:8000/api/v1/auth/status

# Test connettore (richiede autenticazione)
curl http://localhost:8000/api/v1/connector/deals
```

### Test 3: Autenticazione OAuth
1. **Accedi a URL OAuth:**
   ```bash
   curl http://localhost:8000/api/v1/auth/hubspot/login
   ```
   Questo reindirizzerà a HubSpot per l'autorizzazione.

2. **Dopo autorizzazione:**
   - Verrai reindirizzato a `http://localhost:8000/api/v1/auth/callback`
   - Il token verrà salvato nel database SQLite

3. **Verifica stato:**
   ```bash
   curl http://localhost:8000/api/v1/auth/status
   ```

### Test 4: Estrazione Dati
```bash
# Test estrazione deal (richiede autenticazione)
curl http://localhost:8000/api/v1/connector/deals

# Test estrazione contatti
curl http://localhost:8000/api/v1/connector/contacts

# Test estrazione aziende
curl http://localhost:8000/api/v1/connector/companies
```

### Test 5: Process Mining
```bash
# Test discovery (richiede dati)
curl -X POST http://localhost:8000/api/v1/mining/discover \
  -H "Content-Type: application/json" \
  -d '{"algorithm": "dfg"}'

# Test conformance checking
curl -X POST http://localhost:8000/api/v1/mining/conformance \
  -H "Content-Type: application/json" \
  -d '{"model_type": "petri_net"}'

# Test KPI
curl http://localhost:8000/api/v1/mining/kpi
```

### Test 6: Data Quality
```bash
# Test validazione
curl -X POST http://localhost:8000/api/v1/dq/validate \
  -H "Content-Type: application/json" \
  -d '{"file_path": "data/processed/event_log.parquet"}'

# Test report qualità
curl http://localhost:8000/api/v1/dq/report
```

## Test Integrazione Completo

### Script Test Automatico
```bash
# Test sistema integrato
python -c "
from app.core.integration import run_full_system_test_sync
result = run_full_system_test_sync()
print('Test completato:', result['success'])
"
```

### Test Manuale Passo-Passo

1. **Avvia sistema:**
   ```bash
   docker-compose up -d
   ```

2. **Attendi avvio:**
   ```bash
   # Attendi 10 secondi per avvio completo
   sleep 10
   ```

3. **Test health check:**
   ```bash
   curl http://localhost:8000/health
   ```

4. **Test autenticazione:**
   ```bash
   curl http://localhost:8000/api/v1/auth/status
   ```

5. **Test endpoint principali:**
   ```bash
   curl http://localhost:8000/api/v1/connector/deals
   curl http://localhost:8000/api/v1/mining/kpi
   ```

## Monitoring e Logging

### Log Sistema
```bash
# Log backend
docker-compose logs -f app

# Log Celery worker
docker-compose logs -f worker

# Log Redis
docker-compose logs -f redis
```

### File di Log
- `logs/app.log`: Log principale applicazione
- `logs/integration_tests/`: Risultati test integrazione
- `logs/bootstrap_results/`: Risultati bootstrap

### Health Check
```bash
# Verifica salute sistema
curl http://localhost:8000/health

# Verifica endpoint
curl http://localhost:8000/

# Stato autenticazione
curl http://localhost:8000/api/v1/auth/status
```

## Troubleshooting

### Problemi Comuni

#### 1. Errore Autenticazione HubSpot
```bash
# Verifica variabili ambiente
echo $HUBSPOT_CLIENT_ID
echo $HUBSPOT_CLIENT_SECRET

# Test connessione
curl http://localhost:8000/api/v1/auth/status

# Soluzione:
# - Verifica credenziali HubSpot
# - Controlla redirect URI
# - Verifica scopes configurati
```

#### 2. Errore Database SQLite
```bash
# Verifica database
ls -la app/data/

# Ricrea database
rm app/data/process_mining.db
python main.py

# Soluzione:
# - Verifica permessi directory
# - Controlla spazio disco
# - Verifica path database in config
```

#### 3. Errore Celery Worker
```bash
# Verifica Redis
redis-cli ping

# Avvia worker manuale
poetry run celery -A app.tasks.worker.celery_app worker --loglevel=debug

# Soluzione:
# - Verifica Redis running
# - Controlla configurazione broker
# - Verifica import task
```

#### 4. Errore API HubSpot
```bash
# Test connessione HubSpot
curl http://localhost:8000/api/v1/connector/deals

# Soluzione:
# - Verifica token OAuth
# - Controlla rate limiting
# - Verifica scopes permessi
```

### Debug Mode
```bash
# Log dettagliato
export LOG_LEVEL=DEBUG
python main.py

# Test specifico
poetry run pytest tests/test_etl.py::test_extraction -v -s

# Debug Celery
poetry run celery -A app.tasks.worker.celery_app worker --loglevel=debug
```

## Performance Testing

### Load Testing Backend
```bash
# Installa Artillery
npm install -g artillery

# Crea test file
cat > load-test.yml << EOF
config:
  target: 'http://localhost:8000'
  phases:
    - duration: 60
      arrivalRate: 10
      name: "Warm up"
    - duration: 120
      arrivalRate: 50
      name: "Ramp up load"

scenarios:
  - name: "API Health Check"
    flow:
      - get:
          url: "/health"
      - get:
          url: "/"
EOF

# Esegui test
artillery run load-test.yml
```

### Memory Testing
```bash
# Monitora memoria
docker stats

# Test memoria backend
python -c "
import psutil
import os
process = psutil.Process(os.getpid())
print(f'Memoria: {process.memory_info().rss / 1024 / 1024:.2f} MB')
"
```

## Security Testing

### CORS Testing
```bash
# Test CORS headers
curl -I -X OPTIONS http://localhost:8000/api/v1/processes \
  -H "Origin: http://localhost:3000"
```

### Authentication Testing
```bash
# Test senza auth
curl http://localhost:8000/api/v1/processes

# Test con auth (se configurato)
curl http://localhost:8000/api/v1/processes \
  -H "Authorization: Bearer your_token"
```

## Deploy Produzione

### Docker Production
```bash
# Build immagine
docker build -t process-mining:latest .

# Deploy
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

## Checklist Deploy Completo

### Pre-Deploy
- [ ] Variabili ambiente configurate
- [ ] HubSpot app configurata
- [ ] Database SQLite creato
- [ ] Redis disponibile
- [ ] Test unitari passati

### Deploy
- [ ] Docker build completato
- [ ] Servizi avviati
- [ ] Health check OK
- [ ] API endpoints funzionanti
- [ ] Autenticazione OAuth funzionante

### Post-Deploy
- [ ] Monitoraggio attivo
- [ ] Logging configurato
- [ ] Backup configurato
- [ ] Documentazione aggiornata

## Supporto Tecnico

### Contatti
- **Issues**: GitHub Issues
- **Documentazione**: README.md
- **Test**: `poetry run pytest tests/`

### Troubleshooting
1. Controlla log sistema
2. Verifica configurazione
3. Test endpoint singolarmente
4. Consulta documentazione API