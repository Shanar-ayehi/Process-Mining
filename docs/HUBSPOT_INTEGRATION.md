# Integrazione HubSpot - Process Mining System

## Panoramica

Guida per integrare il sistema Process Mining con HubSpot tramite OAuth 2.0, inclusa l'estrazione dei workflow di automazione.

## Architettura Integrazione

```
HubSpot API (OAuth 2.0 + automation scope)
    ↓
HubSpotClient (app/connectors/hubspot_client.py)
    ↓
DataExtractionService (app/services/etl/data_extraction.py)
    ↓
DiscoveryService (app/services/mining/discovery_service.py)
    ↓
Process Mining Engine (con mapping automazioni sui nodi)
    ↓
FastAPI Endpoints (app/api/)
    ↓
React Frontend (Dashboard con badge automazioni)
```

## Prerequisiti

### HubSpot Developer Account
1. **Crea account**: [developers.hubspot.com](https://developers.hubspot.com)
2. **Crea app**: Settings → Integrations → Private Apps
3. **Configura OAuth**:
   - Redirect URI: `http://localhost:8000/api/v1/auth/callback`
   - **Scopes necessari** (incluso `automation`):
     - `crm.objects.deals.read`
     - `crm.objects.deals.write`
     - `crm.objects.contacts.read`
     - `crm.objects.contacts.write`
     - `crm.objects.companies.read`
     - `timeline.events.read`
     - `timeline.events.write`
     - `engagements.read`
     - `settings.user.read`
     - **`automation`** ← Necessario per workflow

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
- Autenticazione OAuth 2.0 con scope `automation`
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

# Workflow di automazione ← NUOVO
await client.get_workflows(limit=100)
```

### 2. Autenticazione OAuth
**File:** `app/api/routes/auth.py`

**Scopes configurati:**
```python
HUBSPOT_SCOPES = [
    "crm.objects.deals.read",
    "crm.objects.deals.write",
    "crm.objects.contacts.read",
    "crm.objects.contacts.write",
    "crm.objects.companies.read",
    "timeline",
    "settings.users.read",
    "automation"  # ← Nuovo scope per workflow
]
```

**Endpoints:**
- `GET /api/v1/auth/hubspot/login` - Inizio flusso OAuth
- `GET /api/v1/auth/callback` - Gestione callback
- `POST /api/v1/auth/refresh` - Refresh token
- `GET /api/v1/auth/status` - Verifica stato

**Flusso OAuth:**
1. Utente accede a `/api/v1/auth/hubspot/login`
2. Reindirizzamento a HubSpot per autorizzazione (con scope `automation`)
3. HubSpot reindirizza a `/api/v1/auth/callback`
4. Token salvato nel database SQLite
5. Redirect al frontend con JWT

### 3. Estrazione Dati
**File:** `app/services/etl/data_extraction.py`

**Metodi:**
```python
# Estrazione completa (incluso workflows)
await service.extract_all_data(save_to_file=True)

# Estrazione specifica
await service.extract_deals_with_history(properties_with_history=["dealstage"])
await service.extract_contacts()
await service.extract_companies()
await service.extract_pipeline_stages()

# Estrazione workflow ← NUOVO
await service.extract_workflows(save_to_file=True)
```

**Salvataggio:**
- Dati salvati in `data/raw/`
- Formato JSON con timestamp
- Workflows salvati come `hubspot_workflows_{timestamp}.json`

### 4. Mapping Automazioni sui Nodi
**File:** `app/services/mining/discovery_service.py`

**Metodo `_map_workflows_to_nodes()`:**
- Analizza i trigger dei workflow HubSpot
- Identifica i nodi del grafo corrispondenti (es. dealstage = "Negoziazione")
- Aggiunge attributo `automation_rules` ai nodi

**Struttura automation_rules:**
```json
{
  "automation_rules": [
    {
      "workflow_id": "123456",
      "workflow_name": "Notifica Manager - Negoziazione",
      "trigger_type": "PROPERTY_CHANGE",
      "trigger_property": "dealstage",
      "trigger_value": "Negoziazione",
      "actions": [
        {"type": "SEND_EMAIL", "delay_days": 0.0},
        {"type": "SET_PROPERTY", "delay_days": 0.5, "property": "owner", "value": "manager@company.com"}
      ]
    }
  ]
}
```

**Endpoint DFG con automazioni:**
```
GET /api/v1/mining/discover/dfg-with-automations/{portal_id}?include_performance=true
```

### 5. Trasformazione Dati
**File:** `app/services/etl/data_transformation.py`

**Processo:**
1. Lettura JSON grezzi (inclusi workflows)
2. Conversione in event log
3. Validazione schema
4. Pseudonimizzazione (opzionale)
5. Salvataggio Parquet

**Output:**
- `data/processed/event_log.parquet`
- Formato standard PM4Py
- Attributi: case_id, activity, timestamp, resource

### 6. Process Mining
**File:** `app/services/mining/`

**Discovery con automazioni:**
```python
# DFG (Directly-Follows Graph) con mapping automazioni
result = discovery_service.discover_dfg(df, workflows=workflows_list)
# result['graph_data'] contiene nodi con automation_rules

# Performance DFG con automazioni
result = discovery_service.discover_performance_dfg(df, workflows=workflows_list)
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

# Workflow HubSpot ← NUOVO
GET /api/v1/connector/workflows
```

### Mining Processi
```bash
# Process Discovery
POST /api/v1/mining/discover
{
  "algorithm": "dfg",
  "data_path": "data/processed/event_log.parquet"
}

# DFG con automazioni ← NUOVO
GET /api/v1/mining/discover/dfg-with-automations/{portal_id}?include_performance=true

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

### Analytics e Simulazione
```bash
# Simulazione What-If ← NUOVO
POST /api/v1/analytics/simulate
{
  "portal_id": "123456",
  "num_cases": 100,
  "modifications": {
    "Negoziazione": {
      "time_multiplier": 0.8,
      "disable_automation": true
    }
  }
}

# Confronto scenari ← NUOVO
POST /api/v1/analytics/simulate/compare
{
  "portal_id": "123456",
  "scenarios": [...],
  "num_cases": 100
}
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

# Test estrazione workflow ← NUOVO
curl http://localhost:8000/api/v1/connector/workflows
```

### Test Mining
```bash
# Test discovery
curl -X POST http://localhost:8000/api/v1/mining/discover \
  -H "Content-Type: application/json" \
  -d '{"algorithm": "dfg"}'

# Test DFG con automazioni ← NUOVO
curl http://localhost:8000/api/v1/mining/discover/dfg-with-automations/123456

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
# - Verifica scopes (incluso automation)
```

### Errore Estrazione Workflow
```bash
# Test API HubSpot workflows
curl http://localhost:8000/api/v1/connector/workflows

# Soluzione:
# - Verifica scope "automation" nelle credenziali OAuth
# - L'utente deve ri-autorizzare l'app con il nuovo scope
# - Controlla permessi admin su HubSpot
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

# Log workflow
grep "workflow" logs/app.log
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
- Workflow estratti ← NUOVO
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

### 5. Scope Automation ← NUOVO
- Richiedi scope `automation` solo se necessario
- Comunica chiaramente all'utente cosa verrà estratto
- Permetti all'utente di revocare l'accesso ai workflow

## Limitazioni API HubSpot Workflow

1. **Endpoint limitato**: Solo workflow creati nell'hub (non built-in)
2. **Trigger info parziale**: Trigger come oggetti complessi con condizioni annidate
3. **Nessun endpoint esecuzioni**: Non esiste endpoint per storico esecuzioni workflow
4. **Rate limit**: 100 richieste ogni 10 secondi per endpoint automation

## Riferimenti

- **HubSpot API Docs**: https://developers.hubspot.com
- **OAuth 2.0 Guide**: https://developers.hubspot.com/docs/api/working-with-oauth
- **Automation API**: https://developers.hubspot.com/docs/api/workflows
- **PM4Py Docs**: https://pm4py.fit.fraunhofer.de
- **SimPy Docs**: https://simpy.readthedocs.io
- **FastAPI Docs**: https://fastapi.tiangolo.com