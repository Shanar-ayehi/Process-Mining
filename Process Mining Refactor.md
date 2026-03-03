# Refactoring progetto Process Mining CRM

## Obiettivo del documento

Questo documento propone una revisione dell’architettura logica, delle dipendenze Python e della struttura delle directory del progetto di process mining su CRM HubSpot, allineandolo agli spunti su connector+engine, data quality, governance, predittivo e integrazione con HubSpot ed eventuale data warehouse.[^1][^2][^3]

## Linee guida architetturali

- Trattare la piattaforma come **connector + motore di process intelligence specializzato HubSpot**, con estensione futura multi-sorgente.[^2][^3]
- Sfruttare al massimo ciò che HubSpot offre in termini di reportistica (Customer Journey, Sales Analytics, Property History) come **front-end business e sorgente/validazione dati**, evitando di duplicare dashboard generiche di funnel.[^3][^2]
- Concentrarsi su: discovery automatica, varianti, conformance checking, root cause analysis, viste end‑to‑end multi‑oggetto/sistema, data quality governance, modelli predittivi basati su pattern di processo, integrazione non invasiva con l’esperienza utente HubSpot.[^2][^3]

## Dipendenze Python: cosa tenere, togliere, aggiungere

### Dipendenze core attuali

Dal `pyproject.toml` risultano le seguenti dipendenze principali.[^1]

- fastapi, uvicorn: backend REST e server ASGI.
- celery, redis: task asincroni per ETL e mining.
- polars, duckdb: motore analitico (dataframe e OLAP in-process).
- pm4py: motore di process mining.
- taipy: UI in Python.
- pydantic-settings, python-dotenv: gestione configurazione.
- requests, tenacity: chiamate HTTP a HubSpot con retry/backoff.
- loguru: logging.
- chardet, urllib3: dipendenze legacy per compatibilità richieste HTTP.

### Raccomandazioni sulle dipendenze

#### Da mantenere

- **fastapi, uvicorn**: fondamentali per esporre API interne/esterne (es. trigger da HubSpot, integration iFrame/pannelli custom).
- **celery, redis**: chiave per pipeline ETL e mining non bloccanti, soprattutto con volumi storici di Property History.
- **polars, duckdb**: eccellente scelta per log evento, KPI e arricchimenti analitici.
- **pm4py**: motore principale per discovery, DFG, varianti, conformance.
- **pydantic-settings**: utile per centralizzare configurazioni (.env, variabili runtime).
- **requests, tenacity**: adeguate per dialogare con API HubSpot e data warehouse esterni.
- **loguru**: logging strutturato, utile per data quality e auditing.

#### Da rivalutare o semplificare

- **taipy**: mantenerla se l’obiettivo è una UI Python‑first; in alternativa valutare di isolarla in un modulo opzionale (es. `app/ui_taipy/`) e astrarre il layer di presentazione per permettere in futuro una UI web separata o integrazione diretta in HubSpot.
- **chardet, urllib3**: spesso sono gestiti come dipendenze transitive di `requests`; mantenerli solo se strettamente necessari per compatibilità specifiche, altrimenti rimuoverli dal `pyproject` e lasciare gestire a Poetry le versioni appropriate.

#### Da aggiungere

Per allineare il progetto agli spunti avanzati, si suggeriscono i seguenti gruppi di librerie.

1. **Data quality, governance e validazione event log**

- `great-expectations` o `pandera`: definizione di schemi e controlli su dataframe/event log (tipi, range, nullability, vincoli di qualità).
- `python-dateutil` (se non già transitiva): gestione robusta delle date in trasformazioni ETL.

2. **Integrazione con data warehouse e BI**

- `sqlalchemy` o driver specifici (es. `snowflake-connector-python`, `google-cloud-bigquery`, `databricks-sql-connector`) se si prevede connessione diretta a DWH esterni.
- `pyarrow`: standard de facto per scambio dati colonnari e integrazione con DuckDB e DWH.

3. **Machine learning / predittivo su pattern di processo**

- `scikit-learn`: modelli classici (classification/regression) per probability‑to‑win, tempo di chiusura, rischio churn.
- Eventuale `xgboost` o `lightgbm` per modelli gradient boosting efficienti su dataset tabellari da event log.

4. **Security, privacy e compliance**

- `cryptography` o `hashlib` (già in standard library) per hash/pseudonimizzazione; se la logica è già in place con hashlib non è necessario aggiungere altro, ma si può formalizzare un modulo `app/core/privacy.py`.

5. **Testing e qualità del codice**

- `pytest`, `pytest-asyncio`, `pytest-cov`: test automatizzati su ETL, mining, API.
- `mypy`, `ruff` o `flake8`: static analysis e linting.

### Esempio di nuova sezione dependencies in pyproject

Nel `pyproject.toml` si può quindi prevedere:

- gruppo `core` (default): dipendenze necessarie in produzione.
- gruppo `ml`, `dq`, `warehouse`, `dev` per componenti opzionali.

Esempio di organizzazione logica (senza riportare versioni specifiche):

```toml
[project]
dependencies = [
  "fastapi",
  "uvicorn",
  "celery",
  "redis",
  "polars",
  "duckdb",
  "pm4py",
  "taipy",
  "pydantic-settings",
  "requests",
  "python-dotenv",
  "loguru",
  "tenacity",
]

[dependency-groups]
ml = ["scikit-learn", "xgboost"]
dq = ["great-expectations", "python-dateutil"]
warehouse = ["sqlalchemy", "pyarrow"]
dev = ["pytest", "pytest-asyncio", "pytest-cov", "ruff"]
```

## Nuova architettura logica

### Layer funzionali principali

Si propone di vedere il sistema come composto da questi macro‑moduli.[^3][^2]

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

Per mappare gli spunti elencati nella richiesta con componenti concreti, si suggeriscono i seguenti servizi.[^2][^3]

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

```text
app/services/
├── etl/
├── mining/
├── dq/
├── analytics/
└── integration/
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

## Conclusioni operative

- Aggiornare il `pyproject.toml` introducendo gruppi di dipendenze per ML, data quality, integrazione DWH e sviluppo, rimuovendo dipendenze HTTP ridondanti (chardet, urllib3) se non necessarie.
- Rifinire la struttura delle cartelle seguendo il file‑tree proposto, con chiara separazione tra connector, ETL, mining, data quality, analytics e integrazioni.
- Introdurre un modulo esplicito di data governance (privacy, quality, reconciliation, audit) per differenziarsi rispetto a semplici toolkit di process mining.
- Prevedere sin da ora hook e client per integrazione con data warehouse e reverse ETL, nonché endpoint dedicati per l’integrazione nativa con HubSpot (pulsanti/pannelli che aprono le analisi di process mining).

---

## References

1. [pyproject.toml](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/115544385/2bc58f87-fa14-4fba-8b21-b8d8b55dca7f/pyproject.toml?AWSAccessKeyId=ASIA2F3EMEYESAVIVUQJ&Signature=CECYtEXIKRTZVHWGJlJzUccRpp4%3D&x-amz-security-token=IQoJb3JpZ2luX2VjEGAaCXVzLWVhc3QtMSJHMEUCIGlu%2FNvkcimeQXb9ky8QW3Qokk2Nt%2By7YTa0L3aWnrZXAiEAmRbJTlpNYxP9tjUV8Sk3eUBTLhqqIAZGDvvzFGJ3HoUq8wQIKRABGgw2OTk3NTMzMDk3MDUiDAl6CLUwV0eeBkNm2irQBCLT%2FtgikHUICAkuA5hqaMTjrkCr%2F385Fm3b0jJZpgxPiYf67h3MwuIlH16q%2Fn%2B62w%2F5Zx0dFCX6%2Bw8pwQ4iN9tSbgjI2j6Ln7F8rE8keBS13aW2gQ31eouXuc%2FPybLxZOLYUTKUGGR8twTrd1PnGnra17AGAiJpxGjPFoFm1s0MNMXsxejdQw%2Fb52QB8vQNS3ayXCGIYPPz95hk%2B8XUfnb2kH3XmAk6D1OyX5l7yAT66oRwbjyYSxfu9VV%2B90da6LEl9TfCS78Kb87oetz1Q4Z47%2Blgycqm25sECTOd63dI5u%2FK0rj1oqJq%2Fkv8X7H%2FR6JYQvZ29n%2FMwWY9atov0Y377Og%2FhUa0eJxc7JaMnsN%2B8DG%2Bquy8BcRRaVLCs2sWWoLOQ49y08Pn%2BlzkO8utsfQi4Hd167fcb%2FVF4P4vdh97ijIG3FpOqMHFB9Lrv7rKNDub6JWJzYi8NbIvU3uufbi0axSPa32QkuXC20VVvm5fBFSLekNiD3tT3euRwS%2BKIwewvdE0qkUoLLMlKK3%2BGlqwSR5SCV5VYTN8UAdEZCVDhTDujobqQYpmzffXTC5VwAiDHPjxNR%2BC3%2FM5uvSLvwTnwUAav9YcuAAEkEQrzlKOpD2kGsigrjJFBzqRlB5xWdKoUhkB4iZ7d7F8xH8UXJ8DPm4x3jMfDiJX2TXo0KYT%2F6NCuxMA%2BMXimf2q5rJ1M8Eik0VSb4%2FCx7qO0iP7O%2BvEkUNaSYBt08mnmxG%2BO6NaPaecWZcuNHpKXOTQD4KwaRpvFcwfgl2CQbox1%2BWjq%2B0wmdiBzQY6mAFm6Hi7tzUNpecLUomyIcmRo2R6P%2B3DmbZ6TcGUwm3bNJ5VhDgKo0PPAJXTtH2TfGtfIF%2F3TJxgBoCQ1gq3YWNa5p1NM%2FQZFvI3QYILrwOmVwKBKe8RXtvAnp8lEZwKRGfVwTaH6%2BqEqUAYV4zfk%2Bje0Flqr8PnsJ%2BXDDdkbwL1U09Bw4%2B4OpRsgBFm0jSDZ47cEY3MYvf1uw%3D%3D&Expires=1772125825) - [project]
name = "process-mining"
version = "0.1.0"
description = "Currently [23/02/2026] a web-a...

2. [README.md](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/115544385/b4dd1afb-e81d-4953-81ea-3f00da67fab4/README.md?AWSAccessKeyId=ASIA2F3EMEYESAVIVUQJ&Signature=n0FXRAo9n0ux8pVfAV8l3w%2BLlQQ%3D&x-amz-security-token=IQoJb3JpZ2luX2VjEGAaCXVzLWVhc3QtMSJHMEUCIGlu%2FNvkcimeQXb9ky8QW3Qokk2Nt%2By7YTa0L3aWnrZXAiEAmRbJTlpNYxP9tjUV8Sk3eUBTLhqqIAZGDvvzFGJ3HoUq8wQIKRABGgw2OTk3NTMzMDk3MDUiDAl6CLUwV0eeBkNm2irQBCLT%2FtgikHUICAkuA5hqaMTjrkCr%2F385Fm3b0jJZpgxPiYf67h3MwuIlH16q%2Fn%2B62w%2F5Zx0dFCX6%2Bw8pwQ4iN9tSbgjI2j6Ln7F8rE8keBS13aW2gQ31eouXuc%2FPybLxZOLYUTKUGGR8twTrd1PnGnra17AGAiJpxGjPFoFm1s0MNMXsxejdQw%2Fb52QB8vQNS3ayXCGIYPPz95hk%2B8XUfnb2kH3XmAk6D1OyX5l7yAT66oRwbjyYSxfu9VV%2B90da6LEl9TfCS78Kb87oetz1Q4Z47%2Blgycqm25sECTOd63dI5u%2FK0rj1oqJq%2Fkv8X7H%2FR6JYQvZ29n%2FMwWY9atov0Y377Og%2FhUa0eJxc7JaMnsN%2B8DG%2Bquy8BcRRaVLCs2sWWoLOQ49y08Pn%2BlzkO8utsfQi4Hd167fcb%2FVF4P4vdh97ijIG3FpOqMHFB9Lrv7rKNDub6JWJzYi8NbIvU3uufbi0axSPa32QkuXC20VVvm5fBFSLekNiD3tT3euRwS%2BKIwewvdE0qkUoLLMlKK3%2BGlqwSR5SCV5VYTN8UAdEZCVDhTDujobqQYpmzffXTC5VwAiDHPjxNR%2BC3%2FM5uvSLvwTnwUAav9YcuAAEkEQrzlKOpD2kGsigrjJFBzqRlB5xWdKoUhkB4iZ7d7F8xH8UXJ8DPm4x3jMfDiJX2TXo0KYT%2F6NCuxMA%2BMXimf2q5rJ1M8Eik0VSb4%2FCx7qO0iP7O%2BvEkUNaSYBt08mnmxG%2BO6NaPaecWZcuNHpKXOTQD4KwaRpvFcwfgl2CQbox1%2BWjq%2B0wmdiBzQY6mAFm6Hi7tzUNpecLUomyIcmRo2R6P%2B3DmbZ6TcGUwm3bNJ5VhDgKo0PPAJXTtH2TfGtfIF%2F3TJxgBoCQ1gq3YWNa5p1NM%2FQZFvI3QYILrwOmVwKBKe8RXtvAnp8lEZwKRGfVwTaH6%2BqEqUAYV4zfk%2Bje0Flqr8PnsJ%2BXDDdkbwL1U09Bw4%2B4OpRsgBFm0jSDZ47cEY3MYvf1uw%3D%3D&Expires=1772125825) - # Process Mining & CRM Analysis Platform 🚀

> **Tesi di ITS:** Analisi Data-Driven per l'Ottimizza...

3. [Tesi_ITS.pdf](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/115544385/b05708ac-5134-438b-b4c7-765dea60e56c/Tesi_ITS.pdf?AWSAccessKeyId=ASIA2F3EMEYESAVIVUQJ&Signature=eNiHJgWsiPjwVSO1BqX3j9K8yKA%3D&x-amz-security-token=IQoJb3JpZ2luX2VjEGAaCXVzLWVhc3QtMSJHMEUCIGlu%2FNvkcimeQXb9ky8QW3Qokk2Nt%2By7YTa0L3aWnrZXAiEAmRbJTlpNYxP9tjUV8Sk3eUBTLhqqIAZGDvvzFGJ3HoUq8wQIKRABGgw2OTk3NTMzMDk3MDUiDAl6CLUwV0eeBkNm2irQBCLT%2FtgikHUICAkuA5hqaMTjrkCr%2F385Fm3b0jJZpgxPiYf67h3MwuIlH16q%2Fn%2B62w%2F5Zx0dFCX6%2Bw8pwQ4iN9tSbgjI2j6Ln7F8rE8keBS13aW2gQ31eouXuc%2FPybLxZOLYUTKUGGR8twTrd1PnGnra17AGAiJpxGjPFoFm1s0MNMXsxejdQw%2Fb52QB8vQNS3ayXCGIYPPz95hk%2B8XUfnb2kH3XmAk6D1OyX5l7yAT66oRwbjyYSxfu9VV%2B90da6LEl9TfCS78Kb87oetz1Q4Z47%2Blgycqm25sECTOd63dI5u%2FK0rj1oqJq%2Fkv8X7H%2FR6JYQvZ29n%2FMwWY9atov0Y377Og%2FhUa0eJxc7JaMnsN%2B8DG%2Bquy8BcRRaVLCs2sWWoLOQ49y08Pn%2BlzkO8utsfQi4Hd167fcb%2FVF4P4vdh97ijIG3FpOqMHFB9Lrv7rKNDub6JWJzYi8NbIvU3uufbi0axSPa32QkuXC20VVvm5fBFSLekNiD3tT3euRwS%2BKIwewvdE0qkUoLLMlKK3%2BGlqwSR5SCV5VYTN8UAdEZCVDhTDujobqQYpmzffXTC5VwAiDHPjxNR%2BC3%2FM5uvSLvwTnwUAav9YcuAAEkEQrzlKOpD2kGsigrjJFBzqRlB5xWdKoUhkB4iZ7d7F8xH8UXJ8DPm4x3jMfDiJX2TXo0KYT%2F6NCuxMA%2BMXimf2q5rJ1M8Eik0VSb4%2FCx7qO0iP7O%2BvEkUNaSYBt08mnmxG%2BO6NaPaecWZcuNHpKXOTQD4KwaRpvFcwfgl2CQbox1%2BWjq%2B0wmdiBzQY6mAFm6Hi7tzUNpecLUomyIcmRo2R6P%2B3DmbZ6TcGUwm3bNJ5VhDgKo0PPAJXTtH2TfGtfIF%2F3TJxgBoCQ1gq3YWNa5p1NM%2FQZFvI3QYILrwOmVwKBKe8RXtvAnp8lEZwKRGfVwTaH6%2BqEqUAYV4zfk%2Bje0Flqr8PnsJ%2BXDDdkbwL1U09Bw4%2B4OpRsgBFm0jSDZ47cEY3MYvf1uw%3D%3D&Expires=1772125825) - ISTITUTO TECNOLOGICO SUPERIORE ACADEMY 
PER LE TECNOLOGIE DELLA INFORMAZIONE E DELLA COMUNICAZIONE 
...

