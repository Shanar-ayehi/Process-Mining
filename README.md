process-mining-thesis/
├── .env                    # Variabili d'ambiente (API Key HubSpot, Redis URL)
├── .gitignore              # File da ignorare (venv, __pycache__, dati sensibili)
├── docker-compose.yml      # Orchestrazione container (Redis, App, Worker)
├── README.md               # Documentazione del progetto
├── requirements.txt        # Librerie Python
│
├── data/                   # STORAGE LOCALE (Ignorato da Git)
│   ├── raw/                # JSON grezzi scaricati da HubSpot
│   ├── processed/          # File .parquet o .csv puliti (Event Logs)
│   └── process_mining.db   # File database DuckDB
│
├── notebooks/              # Jupyter Notebooks per esperimenti e test veloci
│   ├── 01_hubspot_test.ipynb
│   └── 02_pm4py_proto.ipynb
│
├── app/                    # CODICE SORGENTE PRINCIPALE
│   ├── __init__.py
│   │
│   ├── core/               # Configurazioni globali
│   │   ├── __init__.py
│   │   ├── config.py       # Caricamento variabili .env (Pydantic Settings)
│   │   └── database.py     # Connessione a DuckDB
│   │
│   ├── connectors/         # Integrazioni esterne
│   │   ├── __init__.py
│   │   └── hubspot.py      # Client per chiamare le API di HubSpot
│   │
│   ├── services/           # LOGICA DI BUSINESS (Il cuore)
│   │   ├── __init__.py
│   │   ├── etl_service.py  # Pulisce i dati (Polars) -> crea Event Log
│   │   └── mining_service.py # Usa PM4Py per calcolare grafi e statistiche
│   │
│   ├── tasks/              # GESTIONE ASINCRONA (Celery)
│   │   ├── __init__.py
│   │   ├── worker.py       # Configurazione istanza Celery
│   │   └── jobs.py         # Le funzioni decorate con @celery.task
│   │
│   ├── api/                # BACKEND (FastAPI)
│   │   ├── __init__.py
│   │   ├── main.py         # Entry point FastAPI
│   │   ├── routes.py       # Endpoint (/start-analysis, /status)
│   │   └── schemas.py      # Modelli Pydantic (Input/Output dati)
│   │
│   └── ui/                 # FRONTEND (Taipy)
│       ├── __init__.py
│       ├── main.py         # Entry point Taipy
│       ├── pages/          # Pagine della dashboard
│       │   ├── dashboard.py
│       │   └── settings.py
│       └── assets/         # CSS, Immagini, Loghi
│
└── tests/                  # Unit tests (opzionale per MVP ma consigliato)

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
*   **`etl_service.py`**: Contiene le funzioni di pulizia dati (Data Cleaning) utilizzando **Polars**. Trasforma i JSON grezzi in *Event Logs* standardizzati.
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