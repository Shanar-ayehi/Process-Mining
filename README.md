# Process Mining & CRM Analysis Platform

> **Tesi di ITS:** Analisi Data-Driven per l'Ottimizzazione dei Flussi Aziendali  
> **Candidato:** Francesco Scuderi  
> **Stack:** Python 3.12, FastAPI, PM4Py, Polars, DuckDB, Pandera, Taipy, Celery, Docker

## 📋 Descrizione del Progetto

Questo progetto è un **Motore di Process Intelligence** progettato per colmare il divario tra i dati statici presenti nel CRM (HubSpot) e l'esecuzione reale dei processi aziendali, con un focus specifico sui deal "su misura" e ad alta complessità.

Il sistema estrae la cronologia esatta dei cambi di fase, valida la qualità del dato, applica algoritmi di *Process Discovery* per mappare i flussi reali (Root Cause Analysis sui colli di bottiglia) e re-integra i KPI avanzati direttamente all'interno di HubSpot tramite meccanismi di *Reverse ETL*.

---

## Architettura logica

### Layer funzionali principali

Si propone di vedere il sistema come composto da questi macro‑moduli.

1. **Connector HubSpot & sorgenti esterne**
   - Raccolta dati da HubSpot (Property History, Change Event Reports, Customer Journey export se disponibile).
   - Futuri connettori ad altri sistemi (billing, ERP, ticketing) per vista Lead‑to‑Cash end‑to‑end.

2. **Data Lake / Event Log Layer**
   - Normalizzazione dei dati in event log conformi (case id, activity, timestamp, attributi di contesto).
   - Storage in formato Parquet e indicizzazione su DuckDB.

3. **Data Quality & Governance Layer**
   - Regole di qualità del dato (completezza campi, coerenza sequenze, anomalie aggiornamenti).
   - Modulo di reconciliaton tra log interni e report/eventi nativi HubSpot.
   - Privacy & masking (pseudonimizzazione, policy di retention, audit log trasformazioni).

4. **Process Mining Engine**
   - Discovery, conformance checking, analisi varianti e performance (PM4Py).
   - Costruzione di KPI di processo avanzati.

5. **Analytics & Predictive Layer**
   - Feature engineering da event log per training di modelli predittivi.
   - Calcolo di score per deal/cliente (es. process_health_score) da esporre a valle.

6. **Delivery & Integration Layer**
   - API REST per front-end (Taipy o altro) e per integrazione con HubSpot (proprietà custom, workflow trigger).
   - Export verso DWH/BI e reverse ETL.

### Servizi principali per gli spunti chiave

Per mappare gli spunti elencati nella richiesta con componenti concreti, si suggeriscono i seguenti servizi.

- **Connector & Export Service**
  - Estrae/aggiorna event log da HubSpot.
  - Espone export in formati XES/CSV/Parquet.

- **Validation & Reconciliation Service**
  - Confronta sequenze di eventi ricostruite con Property Change Event Reports.
  - Evidenzia discrepanze, eventi mancanti, differenze di timestamp.

- **Data Quality Service**
  - Esegue regole di qualità (great-expectations/pandera) sugli event log.
  - Produce report di anomalie, record da correggere, suggerimenti di segmentazione/migliorie di input.

- **Process Mining Service**
  - Discovery (DFG, modelli di processo), varianti, colli di bottiglia.
  - Conformance checking rispetto a pipeline/stage configurate in HubSpot.

- **Root Cause & Insight Service**
  - Analisi dei colli di bottiglia con correlazioni per owner, segmento, sorgente lead, varianti.

- **Predictive Scoring Service**
  - Modelli per probabilità di chiusura, ritardo, rischio churn.
  - Scrittura risultati in HubSpot come proprietà custom + eventuali task/notification.

- **Integration & UX Bridge Service**
  - Endpoint specifici per essere richiamati da pulsanti/pannelli in HubSpot (es. "Analizza con Process Mining").
  - Logica per sincronizzare perimetro del journey report con analisi lato tuo motore.

## Nuovo file-tree del progetto

### Vista generale

Si mantiene il pattern a cartelle già descritto nel README, raffinando e rendendo più espliciti i layer per connector, governance, analytics e integrazioni.[^2]

```toml
process-mining-hubspot/
├── app/
│   ├── api/                 # Backend REST (FastAPI)
│   ├── connectors/          # Connettori verso HubSpot e altre sorgenti
│   ├── core/                # Config, logging, sicurezza, privacy
│   ├── domain/              # Modelli di dominio (event log, KPI, score)
│   ├── services/            # Logica di business (ETL, mining, dq, analytics)
│   ├── tasks/               # Task asincroni (Celery)
│   └── ui/                  # UI Taipy o altro front-end
├── data/
│   ├── raw/                 # Dati grezzi da HubSpot e altre sorgenti
│   ├── staged/              # Dati intermedi dopo pulizia/validazioni
│   ├── processed/           # Event log e tabelle pronte per mining/analytics
│   └── warehouse/           # Esportazioni per DWH/BI (opzionale)
├── logs/                    # Log applicativi e audit data governance
├── notebooks/               # Prototipi e analisi esplorative
├── tests/                   # Test automatizzati
├── docker-compose.yml
├── pyproject.toml
├── README.md
└── .env.example
```

### Dettaglio cartelle `app/`

#### app/core/

Responsabilità trasversali e infrastrutturali.

- `config.py`: configurazioni centralizzate (env, feature flags, integrazioni DWH).
- `logging.py`: setup di Loguru o logging standard.
- `security.py`: utilità per gestione API key, autenticazione verso API interne.
- `privacy.py`: funzioni per hashing/pseudonimizzazione, applicazione policy di masking, gestione retention.
- `exceptions.py`: eccezioni custom comuni.

#### app/domain/

Modelli concettuali e tipizzati usati da tutto il resto.

- `events.py`: definizioni di Event, Case, EventLog (tipi, enum per attività, ecc.).
- `kpi.py`: strutture per KPI di processo (lead time, rework, throughput, score di qualità dato).
- `scoring.py`: strutture per output dei modelli predittivi (es. process_health_score per deal).

#### app/connectors/

Integrazioni esterne.

- `hubspot_client.py`: client HTTP/SDK centralizzato per HubSpot (deal, contact, ticket, report).
- `hubspot_mapper.py`: mapping proprietà HubSpot → event log standard, incluso supporto per diversi template (Lead Management, Deal Pipeline, Ticketing).
- `warehouse_client.py`: client generico per DWH (Snowflake, BigQuery, ecc.), opzionale.

#### app/services/

Sottocartelle per responsabilità chiare.

```toml
app/services/
├── etl/          #  Estrazione, pulizia dati (Polars) e pseudonimizzazione (GDPR)
├── mining/       # Algoritmi PM4Py (Process Discovery e Conformance Checking)
├── dq/           # Data Quality (Pandera) per validare i log e rilevare anomalie
├── analytics/    # Machine Learning per feature engineering e analisi predittiva
└── integration/  # Reverse ETL per riscrivere KPI e alert direttamente su HubSpot
```

- `etl/`:
  - `hubspot_etl.py`: pipeline di estrazione, normalizzazione e salvataggio in `data/raw` e `data/processed`.
  - `merge_sources.py`: logica per fondere più sorgenti (HubSpot + billing, ecc.).
- `mining/`:
  - `discovery_service.py`: wrapper su PM4Py per DFG, modelli di processo, varianti.
  - `conformance_service.py`: confronto tra modello teorico (da pipeline HubSpot) e log reali.
- `dq/`:
  - `rules.py`: definizione regole di qualità (great-expectations/pandera).
  - `reconciliation.py`: confronti tra log interni e Property Change Event Reports / report nativi.
- `analytics/`:
  - `feature_engineering.py`: trasformazione event log in dataset tabellari per ML.
  - `predictive_models.py`: training e scoring (scikit-learn, xgboost).
- `integration/`:
  - `hubspot_sync.py`: scrittura proprietà custom, gestione reverse ETL verso HubSpot.
  - `journey_bridge.py`: logica per collegare un journey report con le analisi di process mining.

#### app/api/

- `main.py`: creazione FastAPI app, mounting delle router.
- `routes_connector.py`: endpoint per avviare/monitorare estrazioni da HubSpot.
- `routes_mining.py`: endpoint per lanciare discovery, conformance, analisi varianti.
- `routes_dq.py`: endpoint per report di data quality e reconciliation.
- `routes_analytics.py`: endpoint per calcolo/consultazione score e KPI.
- `schemas.py`: modelli Pydantic per input/output API.

#### app/tasks/

- `worker.py`: configurazione Celery.
- `jobs_etl.py`: task background per estrazioni ed ETL.
- `jobs_mining.py`: task per calcoli PM4Py.
- `jobs_dq.py`: task per controlli di data quality.
- `jobs_analytics.py`: task per training/scoring modelli.

#### app/ui/

Se si mantiene Taipy:

- `main.py`: entrypoint UI locale.
- `pages/`:
  - `dashboard_process.py`: visualizzazione DFG, varianti, colli di bottiglia.
  - `data_quality.py`: viste per anomalie, suggerimenti di correzione.
  - `predictive_insights.py`: viste su score, raccomandazioni.
- `assets/`: CSS, immagini, loghi.

### Cartelle dati e logging

#### data/

- `raw/`: dump JSON/CSV grezzi provenienti da API HubSpot e altre sorgenti.
- `staged/`: output intermedi dopo trasformazioni di base e controlli minimi.
- `processed/`: event log standardizzati (Parquet), tabelle KPI, dataset ML.
- `warehouse/`: esportazioni pronte per essere caricate su DWH esterni.

#### logs/

- `app.log`: log applicativi generali.
- `dq.log`: log specifici per regole di data quality e mismatch di reconciliation.
- `audit.log`: traccia delle trasformazioni applicate ai dati (utile per compliance).

### tests/

- `test_connectors.py`: test su chiamate HubSpot (con fixture/mock).
- `test_etl.py`: test su pulizia/normalizzazione dati.
- `test_mining.py`: test su discovery/conformance.
- `test_dq.py`: test su regole di data quality.
- `test_analytics.py`: test su feature engineering e modelli.
- `test_api.py`: test sugli endpoint FastAPI.

## 🛠️ Requisiti e Avvio Rapido (Setup)

Il progetto utilizza **Poetry** per il dependency management (garantendo l'assenza di conflitti tra PM4Py, Taipy e l'ecosistema data) e richiede **Python 3.12**.

1. **Clona il repository e attiva l'ambiente:**

   ```bash
   git clone https://github.com/tuo-repo/process-mining-hubspot.git
   cd process-mining-hubspot
   poetry env use py -3.12
   ```

2. **Installa le dipendenze:**

    ```bash
    poetry install
    ```

3. **Avvia l'infrastruttura (Redis / App / Worker):**

    ```bash
    docker-compose up --build -d
    ```
