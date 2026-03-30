# Integrazione HubSpot - Process Mining System

## Panoramica

Guida per integrare il sistema Process Mining con HubSpot tramite OAuth 2.0.

## Architettura Integrazione

```
HubSpot API (OAuth 2.0)
    ↓
HubSpotClient (app/connectors/hubspot_client.py)
    ↓
DataExtractionService (app/services/etl/data_extraction.py)
    ↓
Process Mining Engine (app/services/mining/)
    ↓
FastAPI Endpoints (app/api/)
```

## Prerequisiti

### HubSpot Developer Account
1. **Crea account**: [developers.hubspot.com](https://developers.hubspot.com)
2. **Crea app**: Settings → Integrations → Private Apps
3. **Configura OAuth**:
   - Redirect URI: `http://localhost:8000/api/v1/auth/callback`
   - Scopes necessari:
     - `crm.objects.deals.read`
     - `crm.objects.contacts.read`
     - `crm.objects.companies.read`
     - `timeline.events.read`
     - `settings.user.read`

### Variabili Ambiente
```bash
HUBSPOT_CLIENT_ID=your_client_id
HUBSPOT_CLIENT_SECRET=your_client_secret
HUBSPOT_REDIRECT_URI=http://localhost:8000/api/v1/auth/callback
```

## Implementazione

### 1. Client HubSpot OAuth
**File:** `app/connectors/hubspot_client.py`

Il client gestisce:
- Autenticazione OAuth 2.0
- Gestione token (access + refresh)
- Rate limiting automatico
- Retry con backoff esponenziale

**Metodi principali:**
```python
# Estrazione deal
await client.get_deals(limit=100)
await client.get_all_deals()  # Con paginazione

# Estrazione contatti
await client.get_contacts(limit=100)

# Estrazione aziende
await client.get_companies(limit=100)

# Pipeline stages
await client.get_pipeline_stages()

# Timeline events
await client.get_timeline_events("deals", deal_id)
```

### 2. Autenticazione OAuth
**File:** `app/api/routes/auth.py`

**Endpoints:**
- `GET /api/v1/auth/hubspot/login` - Inizio flusso OAuth
- `GET /api/v1/auth/callback` - Gestione callback
- `POST /api/v1/auth/refresh` - Refresh token
- `GET /api/v1/auth/status` - Verifica stato

**Flusso OAuth:**
1. Utente accede a `/api/v1/auth/hubspot/login`
2. Reindirizzamento a HubSpot per autorizzazione
3. HubSpot reindirizza a `/api/v1/auth/callback`
4. Token salvato nel database SQLite
5. Redirect al frontend con JWT

### 3. Estrazione Dati
**File:** `app/services/etl/data_extraction.py`

**Metodi:**
```python
# Estrazione completa
await service.extract_all_data(save_to_file=True)

# Estrazione specifica
await service.extract_deals_with_history(properties_with_history=["dealstage"])
await service.extract_contacts()
await service.extract_companies()
await service.extract_pipeline_stages()
```

**Salvataggio:**
- Dati salvati in `data/raw/`
- Formato JSON con timestamp
- Paginazione automatica

### 4. Trasformazione Dati
**File:** `app/services/etl/data_transformation.py`

**Processo:**
1. Lettura JSON grezzi
2. Conversione in event log
3. Validazione schema
4. Pseudonimizzazione (opzionale)
5. Salvataggio Parquet

**Output:**
- `data/processed/event_log.parquet`
- Formato standard PM4Py
- Attributi: case_id, activity, timestamp, resource

### 5. Process Mining
**File:** `app/services/mining/`

**Discovery:**
```python
# DFG (Directly-Follows Graph)
result = discovery_service.discover_dfg(df)

# Alpha Miner
result = discovery_service.discover_alpha_miner(df)

# Heuristic Miner
result = discovery_service.discover_heuristic_miner(df, dependency_threshold=0.5)

# Inductive Miner
result = discovery_service.discover_inductive_miner(df)
```

**Conformance:**
```python
# Conformance checking
result = conformance_service.check_conformance_dfg(df, theoretical_dfg)
```

**KPI:**
```python
# Calcolo KPI
kpi = kpi_service.calculate_process_kpis(df)
```

## API Endpoints

### Connessione HubSpot
```bash
# Lista deal
GET /api/v1/connector/deals

# Dettaglio deal
GET /api/v1/connector/deals/{deal_id}

# Lista contatti
GET /api/v1/connector/contacts

# Lista aziende
GET /api/v1/connector/companies

# Pipeline stages
GET /api/v1/connector/pipeline-stages
```

### Mining Processi
```bash
# Process Discovery
POST /api/v1/mining/discover
{
  "algorithm": "dfg",  # o "alpha", "heuristic", "inductive"
  "data_path": "data/processed/event_log.parquet"
}

# Conformance Checking
POST /api/v1/mining/conformance
{
  "model_type": "petri_net",
  "data_path": "data/processed/event_log.parquet"
}

# Calcolo KPI
GET /api/v1/mining/kpi

# Analisi varianti
GET /api/v1/mining/variants
```

### Data Quality
```bash
# Validazione dati
POST /api/v1/dq/validate
{
  "file_path": "data/processed/event_log.parquet"
}

# Report qualità
GET /api/v1/dq/report
```

## Configurazione

### 1. Configurazione HubSpot
**File:** `app/core/hubspot_config.py`

```python
# Configurazione dinamica
hubspot_config_manager.set_data_structure({
    "stage_field": "dealstage",
    "pipeline_field": "pipeline",
    "owner_field": "hubspot_owner_id",
    "created_field": "createdate",
    "closed_field": "closedate"
})
```

### 2. Configurazione Privacy
**File:** `app/core/config.py`

```python
# Privacy settings
email_hash_salt = "your_salt_here"
data_retention_days = 365
pseudonymization_enabled = true
```

### 3. Configurazione Mining
```python
# Mining settings
mining_default_variant_threshold = 0.05  # 5%
conformance_checking_enabled = true
```

## Testing

### Test Autenticazione
```bash
# Test flusso OAuth
curl http://localhost:8000/api/v1/auth/hubspot/login

# Verifica stato
curl http://localhost:8000/api/v1/auth/status
```

### Test Estrazione
```bash
# Test estrazione deal
curl http://localhost:8000/api/v1/connector/deals

# Test estrazione contatti
curl http://localhost:8000/api/v1/connector/contacts
```

### Test Mining
```bash
# Test discovery
curl -X POST http://localhost:8000/api/v1/mining/discover \
  -H "Content-Type: application/json" \
  -d '{"algorithm": "dfg"}'

# Test KPI
curl http://localhost:8000/api/v1/mining/kpi
```

## Troubleshooting

### Errore Autenticazione
```bash
# Verifica variabili
echo $HUBSPOT_CLIENT_ID
echo $HUBSPOT_CLIENT_SECRET

# Test connessione
curl http://localhost:8000/api/v1/auth/status

# Soluzione:
# - Verifica credenziali
# - Controlla redirect URI
# - Verifica scopes
```

### Errore Estrazione
```bash
# Test API HubSpot
curl http://localhost:8000/api/v1/connector/deals

# Soluzione:
# - Verifica token OAuth
# - Controlla rate limiting
# - Verifica permessi scopes
```

### Errore Mining
```bash
# Test discovery
curl -X POST http://localhost:8000/api/v1/mining/discover

# Soluzione:
# - Verifica dati processati
# - Controlla formato event log
# - Verifica PM4Py installation
```

## Monitoraggio

### Log Estrazione
```bash
# Log backend
docker-compose logs -f app

# Log specifico estrazione
grep "extract" logs/app.log
```

### Health Check
```bash
# Verifica salute sistema
curl http://localhost:8000/health

# Verifica connessione HubSpot
curl http://localhost:8000/api/v1/auth/status
```

### Metriche
- Deal estratti
- Contatti estratti
- Aziende estratte
- Tempo risposta API
- Errori rate limiting

## Best Practices

### 1. Gestione Token
- Refresh automatico quando scaduto
- Storage sicuro in database
- Logging accessi

### 2. Rate Limiting
- Delay tra richieste (100ms)
- Retry con backoff
- Monitoraggio usage

### 3. Gestione Errori
- Retry automatico
- Log dettagliato
- Fallback graceful

### 4. Privacy
- Pseudonimizzazione email
- Data retention policy
- Audit log accessi

## Riferimenti

- **HubSpot API Docs**: https://developers.hubspot.com
- **OAuth 2.0 Guide**: https://developers.hubspot.com/docs/api/working-with-oauth
- **PM4Py Docs**: https://pm4py.fit.fraunhofer.de
- **FastAPI Docs**: https://fastapi.tiangolo.com