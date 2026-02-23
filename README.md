# Process Mining & CRM Analysis Platform 🚀

> **Tesi di ITS:** Analisi Data-Driven per l'Ottimizzazione dei Flussi Aziendali  
> **Candidato:** Francesco Scuderi  
> **Stack:** Python 3.12, FastAPI, PM4Py, Polars, DuckDB, Taipy, Celery, Docker

## 📋 Descrizione del Progetto

Questo progetto è una piattaforma software progettata per colmare il divario tra i dati statici presenti nel CRM (HubSpot) e l'esecuzione reale dei processi aziendali.

Utilizzando tecniche di **Process Mining**, il sistema estrae la **storia completa** delle opportunità di vendita (Deals), ricostruisce il grafo del processo (Directly-Follows Graph) e calcola KPI avanzati (colli di bottiglia, tassi di ri-lavorazione, tempi di attraversamento) che le dashboard standard del CRM non possono mostrare.

### ✨ Funzionalità Chiave
*   **Estrazione Storica:** Ricostruzione temporale degli eventi tramite API HubSpot *Property History*.
*   **Privacy by Design:** Anonimizzazione automatica (hashing) dei dati utente (GDPR compliance).
*   **Analisi Performante:** Utilizzo di **DuckDB** e **Polars** per l'elaborazione rapida di grandi volumi di dati.
*   **Architettura Asincrona:** Gestione dei task pesanti (ETL, Mining) tramite **Celery** e **Redis**.
*   **Dependency Management Moderno:** Gestione rigorosa delle librerie tramite **Poetry**.

---

## 📂 Struttura del Progetto

```text
process-mining-thesis/
├── .env                    # Variabili d'ambiente (API Key, Redis URL) - NON COMMITTARE
├── .gitignore              # Esclusioni Git
├── pyproject.toml          # Configurazione dipendenze (Poetry)
├── poetry.lock             # Lock file delle versioni esatte
├── docker-compose.yml      # Orchestrazione container con Volumi persistenti
├── README.md               # Documentazione
│
├── data/                   # STORAGE LOCALE (Persistente tramite Docker Volumes)
│   ├── raw/                # JSON grezzi con history da HubSpot
│   ├── processed/          # Event Logs in formato .parquet (Anonimizzati)
│   └── process_mining.db   # Database DuckDB
│
├── logs/                   # LOGGING DI SISTEMA
│   ├── app.log             # Log generali
│   └── errors.log          # Errori critici (es. API Rate Limits)
│
├── notebooks/              # Area di prototipazione (Jupyter)
│
├── app/                    # CODICE SORGENTE
│   ├── __init__.py
│   │
│   ├── core/               # Configurazioni
│   │   ├── config.py       # Pydantic Settings
│   │   ├── database.py     # Connessione DuckDB
│   │   └── logger.py       # Configurazione Loguru (Rotazione logs)
│   │
│   ├── connectors/         # Integrazioni
│   │   └── hubspot.py      # Client HubSpot con Retry (Tenacity) e History Fetching
│   │
│   ├── services/           # Logica di Business
│   │   ├── etl_service.py  # Cleaning, Flattening & GDPR Hashing
│   │   └── mining_service.py # PM4Py: Discovery & Conformance
│   │
│   ├── tasks/              # Task Asincroni (Celery)
│   │
│   ├── api/                # Backend REST (FastAPI)
│   │
│   └── ui/                 # Frontend (Taipy)
│
└── tests/                  # Unit Tests (Pytest)

# 📂 Struttura del Progetto

Di seguito viene illustrata l'organizzazione dei file e delle cartelle del progetto. L'architettura segue il pattern di separazione delle responsabilità (Separation of Concerns), distinguendo chiaramente tra logica di business, interfaccia utente e gestione dei dati.

## 📁 Root Directory (Livello Principale)

| File / Cartella | Descrizione |
| :--- | :--- |
| **`.env`** | File di configurazione contenente le variabili d'ambiente sensibili (es. `HUBSPOT_API_KEY`, `REDIS_URL`). **Non viene tracciato da Git.** |
| **`.gitignore`** | Elenco dei file e cartelle da escludere dal version control (es. `venv/`, `__pycache__/`, `data/`). |
| **`docker-compose.yml`** | File di orchestrazione dei container. Definisce i servizi: **Redis** (broker), **API** (backend) e **Worker** (Celery). |
| **`requirements.txt`** | Lista delle dipendenze Python necessarie (es. `fastapi`, `pm4py`, `celery`, `taipy`, `polars`). |
| **`README.md`** | Documentazione generale del progetto, istruzioni di installazione e avvio. |

---

## 📁 data/ (Storage Locale)
Questa cartella funge da "Data Lake" locale. È esclusa dal repository remoto per evitare conflitti e problemi di peso.

*   **`raw/`**: Contiene i file JSON grezzi scaricati direttamente dalle API di HubSpot (backup dei dati originali).
*   **`processed/`**: Contiene i file `.parquet` ottimizzati dopo la fase di pulizia (ETL).
*   **`process_mining.duckdb`**: Il database OLAP locale (DuckDB) dove risiedono le tabelle pronte per le query SQL veloci.

---

## 📁 notebooks/ (Laboratorio)
Area di sviluppo e prototipazione rapida.

*   **`*.ipynb`**: Jupyter Notebooks utilizzati per testare le chiamate API, sperimentare con gli algoritmi di PM4Py e visualizzare i dati in anteprima prima di implementare la logica definitiva nel codice sorgente.

---

## 📁 app/ (Codice Sorgente)
Il cuore dell'applicazione, suddiviso in moduli logici.

### 🔹 app/core/ (Configurazione)
*   **`config.py`**: Gestisce il caricamento delle impostazioni (es. legge il file `.env` tramite Pydantic).
*   **`database.py`**: Gestisce la connessione singleton al database DuckDB.

### 🔹 app/connectors/ (Integrazioni Esterne)
*   **`hubspot.py`**: Modulo dedicato alla comunicazione con HubSpot. Gestisce l'autenticazione, la paginazione delle richieste e il rispetto dei *Rate Limits* delle API.

### 🔹 app/services/ (Logica di Business)
Qui risiede l'intelligenza del software. Questi file sono puri e non dipendono né dal web server né dall'interfaccia grafica.
*   **`etl_service.py`**: Contiene le funzioni di pulizia dati (Data Cleaning) utilizzando **Polars**. Trasforma i JSON grezzi in *Event Logs* standardizzati. Dopodiché applica anche logica di Hashing per Login
*   **`mining_service.py`**: Wrappa la libreria **PM4Py**. Contiene le funzioni per calcolare il *Directly-Follows Graph (DFG)* e le statistiche di processo.

### 🔹 app/tasks/ (Gestione Asincrona)
Il livello che gestisce i lavori pesanti in background per non bloccare l'interfaccia.
*   **`worker.py`**: Inizializza l'istanza di **Celery** e la connette a **Redis**.
*   **`jobs.py`**: Definisce i task asincroni (decorati con `@celery.task`) che orchestrano i servizi di ETL e Mining.

### 🔹 app/api/ (Backend REST)
L'interfaccia tra il mondo esterno (UI) e la logica interna.
*   **`main.py`**: Punto di ingresso dell'applicazione **FastAPI**.
*   **`routes.py`**: Definisce gli endpoint HTTP (es. `POST /start-analysis`, `GET /status/{task_id}`).
*   **`schemas.py`**: Definisce i modelli Pydantic per la validazione dei dati in ingresso e uscita.

### 🔹 app/ui/ (Frontend)
L'interfaccia utente sviluppata in Python puro.
*   **`main.py`**: Punto di ingresso dell'applicazione **Taipy**.
*   **`pages/`**: Contiene il layout delle diverse schermate (Dashboard, Settings).
*   **`assets/`**: File statici (CSS, loghi, immagini).

---

## 📁 tests/
Contiene gli unit test e integration test (basati su `pytest`) per verificare che i servizi di ETL e Mining funzionino correttamente senza dover avviare l'intera applicazione.