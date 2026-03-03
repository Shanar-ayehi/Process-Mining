# Process Mining HubSpot

## Descrizione Progetto

*(Inserire qui una breve descrizione riassuntiva del progetto, degli obiettivi e del contesto)*

---

## File-Tree Strutturale del Progetto

```text
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

---

## Dettaglio Cartelle

### `app/core/`

Responsabilità trasversali e infrastrutturali.

* **`config.py`**: configurazioni centralizzate (env, feature flags, integrazioni DWH).
* **`logging.py`**: setup di Loguru o logging standard.
* **`security.py`**: utilità per gestione API key, autenticazione verso API interne.
* **`privacy.py`**: funzioni per hashing/pseudonimizzazione, applicazione policy di masking, gestione retention.
* **`exceptions.py`**: eccezioni custom comuni.

### `app/domain/`

Modelli concettuali e tipizzati usati da tutto il resto dell'architettura.

* **`events.py`**: definizioni di Event, Case, EventLog (tipi, enum per attività, ecc.).
* **`kpi.py`**: strutture per KPI di processo (lead time, rework, throughput, score di qualità dato).
* **`scoring.py`**: strutture per output dei modelli predittivi (es. *process_health_score* per deal).

### `app/connectors/`

Integrazioni esterne verso altre piattaforme.

* **`hubspot_client.py`**: client HTTP/SDK centralizzato per HubSpot (deal, contact, ticket, report).
* **`hubspot_mapper.py`**: mapping proprietà HubSpot → event log standard, incluso supporto per diversi template (Lead Management, Deal Pipeline, Ticketing).
* **`warehouse_client.py`**: client generico per DWH (Snowflake, BigQuery, ecc.), opzionale.

### `app/services/`

Logica di business divisa in sottocartelle per responsabilità chiare.

#### `etl/`

* **`hubspot_etl.py`**: pipeline di estrazione, normalizzazione e salvataggio in `data/raw` e `data/processed`.
* **`merge_sources.py`**: logica per fondere più sorgenti (HubSpot + billing, ecc.).

#### `mining/`

* **`discovery_service.py`**: wrapper su PM4Py per DFG, modelli di processo, varianti.
* **`conformance_service.py`**: confronto tra modello teorico (da pipeline HubSpot) e log reali.

#### `dq/` (Data Quality)

* **`rules.py`**: definizione regole di qualità (great-expectations/pandera).
* **`reconciliation.py`**: confronti tra log interni e Property Change Event Reports / report nativi.

#### `analytics/`

* **`feature_engineering.py`**: trasformazione event log in dataset tabellari per ML.
* **`predictive_models.py`**: training e scoring (scikit-learn, xgboost).

#### `integration/`

* **`hubspot_sync.py`**: scrittura proprietà custom, gestione reverse ETL verso HubSpot.
* **`journey_bridge.py`**: logica per collegare un journey report con le analisi di process mining.

### `app/api/`

Livello di esposizione del backend REST.

* **`main.py`**: creazione FastAPI app, mounting delle router.
* **`routes_connector.py`**: endpoint per avviare/monitorare estrazioni da HubSpot.
* **`routes_mining.py`**: endpoint per lanciare discovery, conformance, analisi varianti.
* **`routes_dq.py`**: endpoint per report di data quality e reconciliation.
* **`routes_analytics.py`**: endpoint per calcolo/consultazione score e KPI.
* **`schemas.py`**: modelli Pydantic per input/output API.

### `app/tasks/`

Gestione dell'elaborazione asincrona.

* **`worker.py`**: configurazione Celery.
* **`jobs_etl.py`**: task background per estrazioni ed ETL.
* **`jobs_mining.py`**: task per calcoli PM4Py.
* **`jobs_dq.py`**: task per controlli di data quality.
* **`jobs_analytics.py`**: task per training/scoring modelli.

### `app/ui/`

Sezione per il front-end (es. tramite framework Taipy).

* **`main.py`**: entrypoint UI locale.
* **`pages/`**:
  * **`dashboard_process.py`**: visualizzazione DFG, varianti, colli di bottiglia.
  * **`data_quality.py`**: viste per anomalie, suggerimenti di correzione.
  * **`predictive_insights.py`**: viste su score, raccomandazioni.
* **`assets/`**: file statici (CSS, immagini, loghi).

## Cartelle Dati, Logging e Test

### `data/`

Gestione del Data Lake e storage locale.

* **`raw/`**: dump JSON/CSV grezzi provenienti da API HubSpot e altre sorgenti.
* **`staged/`**: output intermedi dopo trasformazioni di base e controlli minimi.
* **`processed/`**: event log standardizzati (Parquet), tabelle KPI, dataset ML.
* **`warehouse/`**: esportazioni pronte per essere caricate su DWH esterni.

### `logs/`

Osservabilità e tracciabilità del sistema.

* **`app.log`**: log applicativi generali.
* **`dq.log`**: log specifici per regole di data quality e mismatch di reconciliation.
* **`audit.log`**: traccia delle trasformazioni applicate ai dati (utile per compliance GDPR).

### `tests/`

Test suite automatizzata.

* **`test_connectors.py`**: test su chiamate HubSpot (con fixture/mock).
* **`test_etl.py`**: test su pulizia/normalizzazione dati.
* **`test_mining.py`**: test su discovery/conformance.
* **`test_dq.py`**: test su regole di data quality.
* **`test_analytics.py`**: test su feature engineering e modelli.
* **`test_api.py`**: test sugli endpoint FastAPI.

---

## Conclusioni Operative

1. **Gestione Dipendenze:** Aggiornare il `pyproject.toml` introducendo gruppi di dipendenze distinti per ML, data quality, integrazione DWH e sviluppo, rimuovendo dipendenze HTTP ridondanti (`chardet`, `urllib3`) se non strettamente necessarie.
2. **Struttura Directory:** Rifinire la struttura delle cartelle seguendo il file‑tree proposto, con chiara separazione tra *connector*, *ETL*, *mining*, *data quality*, *analytics* e *integrazioni*.
3. **Data Governance:** Introdurre un modulo esplicito di data governance (privacy, quality, reconciliation, audit) per differenziarsi in modo netto rispetto a semplici toolkit accademici di process mining.
4. **Predisposizione Integrazioni:** Prevedere sin da ora hook e client per l'integrazione nativa con un Data Warehouse e per il Reverse ETL, nonché endpoint API dedicati per poter interagire direttamente dall'ambiente di HubSpot (es. tramite pulsanti o pannelli custom che richiamano le analisi di process mining).

