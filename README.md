# Process Mining & CRM Analysis Platform 🚀

> **Tesi di ITS:** Analisi Data-Driven per l'Ottimizzazione dei Flussi Aziendali  
> **Candidato:** Francesco Scuderi  
> **Stack:** Python 3.12, FastAPI, PM4Py, Polars, DuckDB, Taipy, Celery, Docker

## 📋 Descrizione del Progetto

Questo progetto è una piattaforma software progettata per colmare il divario tra i dati statici presenti nel CRM (HubSpot) e l'esecuzione reale dei processi aziendali.

Utilizzando tecniche di **Process Mining**, il sistema estrae la **storia completa** delle opportunità di vendita (Deals), ricostruisce il grafo del processo (Directly-Follows Graph) e calcola KPI avanzati (colli di bottiglia, tassi di ri-lavorazione, tempi di attraversamento) che le dashboard standard del CRM non possono mostrare.

### ✨ Funzionalità Chiave
*   **Estrazione Storica:** Ricostruzione temporale degli eventi tramite API HubSpot *Property History*.
*   **Privacy by Design:** Anonimizzazione automatica (hashing) delle email degli operatori commerciali (GDPR compliance).
*   **Analisi Performante:** Utilizzo di **DuckDB** e **Polars** per l'elaborazione rapida di grandi volumi di dati.
*   **Architettura Asincrona:** Gestione dei task pesanti (ETL, Mining) tramite **Celery** e **Redis**.
*   **Dependency Management Moderno:** Gestione rigorosa delle librerie tramite **Poetry**.

---

## 📂 Struttura del Progetto (File Tree)

Di seguito viene illustrata l'organizzazione dei file e delle cartelle del progetto. L'architettura segue il pattern di separazione delle responsabilità (Separation of Concerns).

```text
process-mining-thesis/
├── .vscode/                # Impostazioni specifiche dell'editor VS Code
├── app/                    # CODICE SORGENTE PRINCIPALE
│   ├── api/                # Backend REST (FastAPI)
│   ├── connectors/         # Integrazioni esterne (API HubSpot)
│   ├── core/               # Configurazioni e Connessioni globali
│   ├── services/           # Logica di Business (ETL e Mining)
│   ├── tasks/              # Gestione code e task asincroni (Celery)
│   └── ui/                 # Interfaccia grafica Frontend (Taipy)
│
├── data/                   # STORAGE LOCALE (Persistente)
│   ├── processed/          # Event Logs analizzabili (es. file .parquet)
│   ├── raw/                # Dati grezzi o di simulazione
│   │   └── dummy_deals.json
│   ├── process_map.png     # Immagine generata dall'algoritmo DFG
│   └── process_mining.db   # Database analitico DuckDB
│
├── logs/                   # LOGGING DI SISTEMA (Generati da Loguru)
│   ├── app.log             # Log generali di sistema
│   └── errors.log          # File dedicato agli errori critici
│
├── notebooks/              # Laboratorio (Jupyter Notebooks)
│   ├── 01_hubspot_test.ipynb
│   └── 02_pm4py_proto.ipynb
│
├── tests/                  # TEST LOCALI
│   ├── test_etl.py         # Script per testare la pulizia dei dati
│   ├── test_mining.py      # Script per testare l'algoritmo PM4Py
│   └── test_ui.py          # Script per testare l'avvio della dashboard
│
├── .env                    # Variabili d'ambiente (API Key, ecc.) - IGNORATO DA GIT
├── .gitignore              # Regole di esclusione per i commit Git
├── docker-compose.yml      # Orchestrazione container Docker
├── poetry.lock             # Snapshot esatto delle versioni delle dipendenze
├── pyproject.toml          # File di configurazione di Poetry (Dipendenze)
├── README.md               # Questa documentazione
└── requirements.txt        # Legacy: file dipendenze vecchio formato (sostituito da Poetry)
```

## 📂 Struttura del Progetto

Di seguito viene illustrata l'organizzazione dei file e delle cartelle del progetto. L'architettura segue il pattern di separazione delle responsabilità (Separation of Concerns), distinguendo chiaramente tra logica di business, interfaccia utente e gestione dei dati.

### 📁 Root Directory (Livello Principale)

| File / Cartella | Descrizione |
| :--- | :--- |
| **`.env`** | File di configurazione contenente le variabili d'ambiente sensibili (es. `HUBSPOT_API_KEY`, `REDIS_URL`). **Non viene tracciato da Git.** |
| **`.gitignore`** | Elenco dei file e cartelle da escludere dal version control (es. `venv/`, `__pycache__/`, `data/`). |
| **`docker-compose.yml`** | File di orchestrazione dei container. Definisce i servizi: **Redis** (broker), **API** (backend) e **Worker** (Celery). Configurazione nativa dei "Volumi" per garantire che i file contenuti nella cartella data/ non vengano persi al riavvio del server. |
| **`README.md`** | Documentazione generale del progetto, istruzioni di installazione e avvio. |
| **`pyproject.toml / poetry.lock`** | Costituiscono il moderno sistema di gestione delle dipendenze in Python. Garantiscono che l'applicazione giri con le versioni esatte dichiarate, prevenendo crash da aggiornamenti imprevisti di pacchetti. |

---

### 📁 data/ (Storage Locale)
Questa cartella funge da "Data Lake" locale. È esclusa dal repository remoto per evitare conflitti e problemi di peso.

*   **`raw/`**: Contiene i file JSON grezzi scaricati direttamente dalle API di HubSpot (backup dei dati originali).
*   **`processed/`**: Contiene i file `.parquet` ottimizzati dopo la fase di pulizia (ETL).
*   **`process_mining.duckdb`**: Il database OLAP locale (DuckDB) dove risiedono le tabelle pronte per le query SQL veloci.
* **`raw/dummy_deals.json`**: File di dati "Mock" utilizzato per testare l'intera architettura e il frontend senza consumare le chiamate API (Rate Limits) di HubSpot.
* **`process_mining.db`**: Il file fisico gestito da DuckDB che agisce come Data Warehouse colonnare, offrendo letture ultra-rapide sui log di sistema.

---

### 📁 notebooks/ (Laboratorio)
Area di sviluppo e prototipazione rapida.

*   **`*.ipynb`**: Jupyter Notebooks utilizzati per testare le chiamate API, sperimentare con gli algoritmi di PM4Py e visualizzare i dati in anteprima prima di implementare la logica definitiva nel codice sorgente.

---

### 📁 app/ (Codice Sorgente)
Il cuore dell'applicazione, suddiviso in moduli logici.

#### 🔹 app/core/ (Configurazione)
*   **`config.py`**: Gestisce il caricamento delle impostazioni (es. legge il file `.env` tramite Pydantic).
*   **`database.py`**: Gestisce la connessione singleton al database DuckDB.

#### 🔹 app/connectors/ (Integrazioni Esterne)
*   **`hubspot.py`**: Modulo dedicato alla comunicazione con HubSpot. Gestisce l'autenticazione, la paginazione delle richieste e il rispetto dei *Rate Limits* delle API.

#### 🔹 app/services/ (Logica di Business)
Qui risiede l'intelligenza del software. Questi file sono puri e non dipendono né dal web server né dall'interfaccia grafica.
*   **`etl_service.py`**: Contiene le funzioni di pulizia dati (Data Cleaning) utilizzando **Polars**. Trasforma i JSON grezzi in *Event Logs* standardizzati. Dopodiché applica anche logica di Hashing per Login
*   **`mining_service.py`**: Wrappa la libreria **PM4Py**. Contiene le funzioni per calcolare il *Directly-Follows Graph (DFG)* e le statistiche di processo.

#### 🔹 app/tasks/ (Gestione Asincrona)
Il livello che gestisce i lavori pesanti in background per non bloccare l'interfaccia.
*   **`worker.py`**: Inizializza l'istanza di **Celery** e la connette a **Redis**.
*   **`jobs.py`**: Definisce i task asincroni (decorati con `@celery.task`) che orchestrano i servizi di ETL e Mining.

#### 🔹 app/api/ (Backend REST)
L'interfaccia tra il mondo esterno (UI) e la logica interna.
*   **`main.py`**: Punto di ingresso dell'applicazione **FastAPI**.
*   **`routes.py`**: Definisce gli endpoint HTTP (es. `POST /start-analysis`, `GET /status/{task_id}`).
*   **`schemas.py`**: Definisce i modelli Pydantic per la validazione dei dati in ingresso e uscita.

#### 🔹 app/ui/ (Frontend)
L'interfaccia utente sviluppata in Python puro.
*   **`main.py`**: Punto di ingresso dell'applicazione **Taipy**.
*   **`pages/`**: Contiene il layout delle diverse schermate (Dashboard, Settings).
*   **`assets/`**: File statici (CSS, loghi, immagini).

---

### 📁 tests/
Contiene i file di collaudo sviluppati con approccio "Mock-Driven". Permettono di isolare e testare singolarmente l'estrazione (test_etl.py), il calcolo algoritmico (test_mining.py) e il rendering della pagina (test_ui.py).

## 🛠️ Requisiti e Installazione locale
### Passaggi Fondamentali
Il progetto utilizza Poetry e richiede Python 3.12.
1. Clona il repository:
```bash
git clone https://github.com/tuo-repo/process-mining-thesis.git
cd process-mining-thesis
```
2. Attiva l'ambiente e installa le librerie:
```bash
poetry env use py -3.12  # Forza l'uso di Python 3.12
poetry install           # Installa tutto dal file poetry.lock
```

3. Configura le variabili d'ambiente:
Crea un file `.env` copiando il template (se disponibile) e inserendo le chiavi segrete.

4. Esegui l'applicazione visiva:
```bash
poetry run python -m app.ui.main
```
### 🔬 Dettagli Metodologici per la Tesi
Estrazione Dati (The "History" Challenge): Il sistema non si limita a leggere lo stato "attuale" del CRM. Estrae la cronologia esatta dei cambi di fase di vendita, ricostruendo l'Event Log necessario per gli algoritmi di Process Discovery.
Gestione GDPR (Pseudonimizzazione): I dati reali provenienti dal CRM contengono PII (Personal Identifiable Information). Il modulo ETL implementa un processo di Hash unidirezionale prima che il dato venga storicizzato, permettendo al Process Mining di valutare le performance di un operatore senza mai salvarne l'identità in chiaro.