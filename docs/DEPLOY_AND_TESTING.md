# Deploy e Testing

Procedura completa per deployare e testare il sistema Process Mining con integrazione HubSpot.

## Panoramica del Deploy

Il sistema è composto da 3 componenti principali:
1. **Backend FastAPI** (Port 8000)
2. **Frontend React** (Port 3000)
3. **Database PostgreSQL** (Port 5432)

## Prerequisiti per il Deploy

### Tecnici
- [x] Python 3.10+
- [x] Node.js 18+
- [x] Docker & Docker Compose
- [x] PostgreSQL 14+
- [x] HTTPS certificate (per produzione)

### HubSpot
- [x] Account HubSpot con permessi admin
- [x] Accesso a External Cards
- [x] API Key HubSpot (se necessaria)

## Opzioni di Deploy

### Opzione 1: Docker Compose (Consigliata)

#### 1.1 Configurazione Ambiente
```bash
# Crea file .env per le variabili d'ambiente
cp .env.example .env

# Modifica .env con i tuoi valori
nano .env
```

Contenuto tipico di `.env`:
```bash
# Backend
DATABASE_URL=postgresql://user:password@postgres:5432/process_mining
HUBSPOT_API_KEY=tua-api-key
HUBSPOT_CLIENT_ID=tuo-client-id
HUBSPOT_CLIENT_SECRET=tuo-client-secret

# Frontend
VITE_API_URL=https://backend-tuodominio.com/api/v1
VITE_HUBSPOT_CLIENT_ID=tuo-client-id

# Database
POSTGRES_DB=process_mining
POSTGRES_USER=user
POSTGRES_PASSWORD=password
```

#### 1.2 Avvio con Docker Compose
```bash
# Avvia tutti i servizi
docker-compose up -d

# Verifica stato servizi
docker-compose ps

# Controlla log
docker-compose logs -f
```

#### 1.3 Verifica Deploy
```bash
# Test backend API
curl https://backend-tuodominio.com/api/v1/health

# Test frontend
curl https://frontend-tuodominio.com

# Test database
docker-compose exec postgres psql -U user -d process_mining -c "SELECT version();"
```

### Opzione 2: Deploy Separato

#### 2.1 Backend su Render.com
1. **Crea account su Render.com**
2. **Connetti repository GitHub**
3. **Crea Web Service**:
   - Build Command: `pip install -r requirements.txt && python main.py --mode full`
   - Start Command: `uvicorn app.api.main:app --host 0.0.0.0 --port $PORT`
   - Environment: Production

4. **Configura variabili d'ambiente**:
   - `DATABASE_URL`: PostgreSQL connection string
   - `HUBSPOT_API_KEY`: HubSpot API Key
   - `HUBSPOT_CLIENT_ID`: OAuth Client ID
   - `HUBSPOT_CLIENT_SECRET`: OAuth Client Secret

#### 2.2 Frontend su Vercel
1. **Crea account su Vercel.com**
2. **Importa repository**
3. **Configura build**:
   - Framework: Vite
   - Build Command: `npm run build`
   - Output Directory: `dist`
   - Install Command: `npm install`

4. **Configura variabili d'ambiente**:
   - `VITE_API_URL`: URL backend
   - `VITE_HUBSPOT_CLIENT_ID`: OAuth Client ID

### Opzione 3: Deploy su Railway

#### 3.1 Backend su Railway
1. **Crea account su Railway.app**
2. **Importa repository**
3. **Configura variabili d'ambiente**
4. **Deploy automatico**

#### 3.2 Frontend su Railway
1. **Crea nuovo servizio**
2. **Importa frontend repository**
3. **Configura build e variabili**

## Configurazione HTTPS

### Certificati SSL
```bash
# Opzione 1: Let's Encrypt (consigliato)
sudo apt install certbot
sudo certbot certonly --standalone -d tuo-dominio.com

# Opzione 2: Cloudflare SSL
# Configura SSL in Cloudflare dashboard
```

### Configurazione Reverse Proxy (Nginx)
```nginx
# /etc/nginx/sites-available/process-mining
server {
    listen 80;
    server_name tuo-dominio.com;
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl;
    server_name tuo-dominio.com;

    ssl_certificate /etc/letsencrypt/live/tuo-dominio.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/tuo-dominio.com/privkey.pem;

    # Backend
    location /api/ {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Frontend
    location / {
        proxy_pass http://localhost:3000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

## Testing del Sistema

### Test 1: Backend API Testing

#### 1.1 Test Endpoint Base
```bash
# Test health check
curl -X GET "https://backend-tuodominio.com/api/v1/health"

# Test process list
curl -X GET "https://backend-tuodominio.com/api/v1/processes"

# Test process detail
curl -X GET "https://backend-tuodominio.com/api/v1/processes/test-process"
```

#### 1.2 Test Process Analysis
```bash
# Avvia analisi processo
curl -X POST "https://backend-tuodominio.com/api/v1/processes/test-process/analyze"

# Controlla stato analisi
curl -X GET "https://backend-tuodominio.com/api/v1/processes/test-process/analysis/status"

# Ottieni risultati
curl -X GET "https://backend-tuodominio.com/api/v1/processes/test-process/analysis/results"
```

#### 1.3 Test HubSpot Integration
```bash
# Test HubSpot connection
curl -X GET "https://backend-tuodominio.com/api/v1/hubspot/test-connection"

# Test data extraction
curl -X GET "https://backend-tuodominio.com/api/v1/hubspot/deals"
```

### Test 2: Frontend Testing

#### 2.1 Test Componenti Base
```bash
cd frontend

# Test unit
npm test

# Test integrazione
npm run test:integration

# Test E2E
npm run test:e2e
```

#### 2.2 Test Manuali
1. **Process List Page**:
   - Verifica caricamento lista processi
   - Test ricerca e filtro
   - Test avvio analisi

2. **Process Detail Page**:
   - Verifica visualizzazione dettagli
   - Test varianti processo
   - Test metriche KPI

3. **Analysis Page**:
   - Verifica stato analisi
   - Test visualizzazioni grafiche
   - Test download report

### Test 3: HubSpot Integration Testing

#### 3.1 Test External Card
1. **Crea External Card in HubSpot**
2. **Configura iframe URL**
3. **Test visualizzazione**
4. **Test funzionalità**

#### 3.2 Test OAuth 2.0
```bash
# Test OAuth flow
curl -X GET "https://frontend-tuodominio.com/auth/hubspot"

# Test token refresh
curl -X POST "https://backend-tuodominio.com/api/v1/auth/refresh"
```

#### 3.3 Test Parametri HubSpot
```bash
# Test passaggio parametri
curl -X GET "https://frontend-tuodominio.com?dealId=12345&contactId=67890"
```

### Test 4: Performance Testing

#### 4.1 Load Testing Backend
```bash
# Installa Artillery
npm install -g artillery

# Crea test file
cat > load-test.yml << EOF
config:
  target: 'https://backend-tuodominio.com'
  phases:
    - duration: 60
      arrivalRate: 10
      name: "Warm up"
    - duration: 120
      arrivalRate: 50
      name: "Ramp up load"
    - duration: 300
      arrivalRate: 100
      name: "Sustained load"

scenarios:
  - name: "Process Analysis"
    weight: 70
    flow:
      - get:
          url: "/api/v1/processes"
      - post:
          url: "/api/v1/processes/test-process/analyze"
      - get:
          url: "/api/v1/processes/test-process/analysis/status"

  - name: "Process Details"
    weight: 30
    flow:
      - get:
          url: "/api/v1/processes/test-process"
      - get:
          url: "/api/v1/processes/test-process/variants"
EOF

# Esegui test
artillery run load-test.yml
```

#### 4.2 Frontend Performance
```bash
# Test bundle size
npm run build -- --analyze

# Test performance
npm run test:performance

# Lighthouse CI
npm install -g @lhci/cli
lhci autorun
```

### Test 5: Security Testing

#### 5.1 CORS Testing
```bash
# Test CORS headers
curl -I -X OPTIONS "https://backend-tuodominio.com/api/v1/processes" \
  -H "Origin: https://malicious-site.com"
```

#### 5.2 Authentication Testing
```bash
# Test senza auth
curl -X GET "https://backend-tuodominio.com/api/v1/processes"

# Test con token valido
curl -X GET "https://backend-tuodominio.com/api/v1/processes" \
  -H "Authorization: Bearer valid-token"

# Test con token invalido
curl -X GET "https://backend-tuodominio.com/api/v1/processes" \
  -H "Authorization: Bearer invalid-token"
```

#### 5.3 Input Validation Testing
```bash
# Test SQL injection
curl -X GET "https://backend-tuodominio.com/api/v1/processes/'; DROP TABLE processes; --"

# Test XSS
curl -X POST "https://backend-tuodominio.com/api/v1/processes" \
  -H "Content-Type: application/json" \
  -d '{"name": "<script>alert(1)</script>"}'
```

## Monitoring e Logging

### Backend Monitoring
```python
# Aggiungi monitoring nel backend
from prometheus_client import Counter, Histogram, start_http_server

# Metrics
REQUEST_COUNT = Counter('requests_total', 'Total requests', ['method', 'endpoint'])
REQUEST_DURATION = Histogram('request_duration_seconds', 'Request duration')

@app.middleware("http")
async def monitor_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time
    
    REQUEST_COUNT.labels(method=request.method, endpoint=request.url.path).inc()
    REQUEST_DURATION.observe(duration)
    
    return response
```

### Frontend Monitoring
```typescript
// Aggiungi monitoring nel frontend
import { datadogRum } from '@datadog/browser-rum';

datadogRum.init({
  applicationId: 'tuo-app-id',
  clientToken: 'tuo-client-token',
  site: 'datadoghq.com',
  service: 'process-mining-frontend',
  env: 'production',
  version: '1.0.0',
  sessionSampleRate: 100,
  sessionReplaySampleRate: 20,
  trackUserInteractions: true,
  trackResources: true,
  trackLongTasks: true,
});
```

### Log Aggregation
```bash
# Configura log rotation
sudo nano /etc/logrotate.d/process-mining

# Contenuto
/var/log/process-mining/*.log {
    daily
    missingok
    rotate 52
    compress
    delaycompress
    notifempty
    create 644 root root
}
```

## Troubleshooting

### Problemi Comuni

#### Backend non risponde
```bash
# Controlla stato servizi
docker-compose ps

# Controlla log
docker-compose logs backend

# Test connessione database
docker-compose exec backend python -c "import psycopg2; conn = psycopg2.connect('postgresql://user:password@postgres:5432/process_mining'); print('OK')"
```

#### Frontend non carica
```bash
# Controlla build
cd frontend && npm run build

# Controlla variabili d'ambiente
echo $VITE_API_URL

# Test connettività API
curl $VITE_API_URL/health
```

#### HubSpot integration fallisce
```bash
# Test CORS
curl -I -X OPTIONS $BACKEND_URL/api/v1/processes \
  -H "Origin: https://app.hubspot.com"

# Test CSP headers
curl -I $BACKEND_URL/api/v1/processes
```

### Debug Tools

#### Backend Debug
```bash
# Avvia backend in modalità debug
python main.py --mode full --debug

# Attiva logging dettagliato
export LOG_LEVEL=DEBUG
```

#### Frontend Debug
```bash
# Avvia frontend in modalità sviluppo
cd frontend && npm run dev

# Abilita React DevTools
# Installa estensione browser React DevTools
```

#### Database Debug
```bash
# Accedi al database
docker-compose exec postgres psql -U user -d process_mining

# Controlla tabelle
\dt

# Controlla dati
SELECT * FROM processes LIMIT 10;
```

## Checklist Deploy Completo

### Pre-Deploy
- [ ] Tutti i test passano
- [ ] Variabili d'ambiente configurate
- [ ] Database configurato
- [ ] HTTPS configurato
- [ ] CORS configurato
- [ ] CSP headers configurati

### Deploy
- [ ] Backend deployato
- [ ] Frontend deployato
- [ ] Database accessibile
- [ ] API testate
- [ ] Frontend testato
- [ ] HubSpot integration configurata

### Post-Deploy
- [ ] Monitoraggio attivo
- [ ] Logging configurato
- [ ] Backup configurati
- [ ] Documentazione aggiornata
- [ ] Team informato

### Testing Completo
- [ ] Unit test passati
- [ ] Integration test passati
- [ ] E2E test passati
- [ ] Performance test passati
- [ ] Security test passati
- [ ] User acceptance test passati

## Supporto e Manutenzione

### Supporto Tecnico
- **Orari**: Lun-Ven 9:00-18:00
- **Canali**: Email, GitHub Issues, Slack
- **SLA**: Risposta entro 24h

### Manutenzione
- **Aggiornamenti**: Settimanali
- **Backup**: Giornalieri
- **Monitoring**: 24/7
- **Security**: Mensili

### Documentazione
- **API Docs**: `/docs` endpoint
- **User Guide**: `docs/USER_GUIDE.md`
- **Dev Guide**: `docs/DEVELOPMENT.md`
- **Troubleshooting**: `docs/TROUBLESHOOTING.md`