# Process Mining System - Sistema Completamente Plastico

## Panoramica

Questo è un sistema Process Mining completamente **plastico** e **auto-adattivo** che si configura automaticamente senza bisogno di setup iniziale. Il sistema è progettato per essere:

- **Auto-Discovery**: Scopre automaticamente la configurazione HubSpot
- **Bootstrap Automatico**: Configura directory e impostazioni al primo avvio
- **ETL Reattivo**: Si attiva automaticamente quando ci sono nuovi dati
- **UI Dinamica**: Si adatta ai dati disponibili invece di mostrare dati hardcoded
- **Integrazione Completa**: Tutti i componenti lavorano insieme in modo coordinato

## Caratteristiche Principali

### 🚀 Auto-Discovery HubSpot
- **Scoperta Automatica**: Analizza automaticamente la struttura dei deal HubSpot
- **Pipeline Stages**: Identifica automaticamente le fasi delle pipeline
- **Proprietà Disponibili**: Scopre le proprietà disponibili per l'estrazione
- **Configurazione Dinamica**: Genera una configurazione basata sui dati reali

### 📁 Bootstrap Automatico
- **Setup Directory**: Crea automaticamente tutte le directory necessarie
- **Configurazione Base**: Genera una configurazione base se non disponibile
- **Validazione Sistema**: Verifica la correttezza del setup
- **Gestione Errori**: Gestisce automaticamente gli errori di configurazione

### 🔄 ETL Reattivo
- **Monitoraggio Continuo**: Controlla automaticamente la presenza di nuovi dati
- **Attivazione Automatica**: Si attiva quando vengono rilevati nuovi file
- **Gestione Job**: Coordinamento intelligente dei job ETL
- **Retry Automatico**: Gestione automatica dei fallimenti e retry

### 🖥️ UI Dinamica
- **Adattamento Dati**: Si adatta automaticamente ai dati disponibili
- **Metriche Reali**: Mostra metriche basate sui dati effettivi
- **Visualizzazioni Dinamiche**: Grafici e diagrammi basati sui dati reali
- **Stato Sistema**: Monitoraggio in tempo reale dello stato del sistema

### 🔗 Integrazione Completa
- **Test Sistema**: Verifica completa dell'integrazione tra tutti i componenti
- **Coordinamento**: Tutti i servizi lavorano in modo coordinato
- **Monitoraggio**: Controllo continuo dello stato del sistema
- **Logging**: Logging integrato per il debug e il monitoraggio

## Installazione

### Prerequisiti
- Python 3.10+
- HubSpot API Key (opzionale, il sistema funziona anche senza)

### Setup Rapido

1. **Clona il repository**
   ```bash
   git clone <repository-url>
   cd Process-Mining
   ```

2. **Installa le dipendenze**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configura l'ambiente**
   ```bash
   # Crea il file .env nella root del progetto
   echo "HUBSPOT_API_KEY=tua_api_key" > .env
   ```

4. **Avvia il sistema**
   ```bash
   python main.py
   ```

## Modalità di Esecuzione

### Modalità Completa (Default)
Avvia tutto il sistema con bootstrap automatico:
```bash
python main.py --mode full
```

### Solo Bootstrap
Esegue solo il bootstrap del sistema:
```bash
python main.py --mode bootstrap
```

### Solo ETL
Avvia solo il sistema ETL reattivo:
```bash
python main.py --mode etl
```

### Solo UI
Avvia solo l'interfaccia utente:
```bash
python main.py --mode ui
```

### Test Sistema
Esegue il test completo del sistema:
```bash
python main.py --mode test
```

### Test Solo (Esci dopo il test)
Esegue il test e poi esce:
```bash
python main.py --test-only
```

## Opzioni Avanzate

### Disabilitare Componenti
```bash
# Avvia senza bootstrap automatico
python main.py --no-bootstrap

# Avvia senza ETL automatico
python main.py --no-etl

# Avvia senza UI automatica
python main.py --no-ui
```

### Esempi di Combinazioni
```bash
# Avvia UI senza bootstrap (se già configurato)
python main.py --mode ui --no-bootstrap

# Avvia ETL con bootstrap ma senza UI
python main.py --mode etl --no-ui

# Test completo senza bootstrap (se già configurato)
python main.py --mode test --no-bootstrap
```

## Struttura del Sistema

```
Process-Mining/
├── main.py                          # Script principale di avvio
├── app/
│   ├── core/
│   │   ├── bootstrap.py            # Sistema di bootstrap e auto-discovery
│   │   ├── config.py               # Configurazione aggiornata con bootstrap
│   │   └── integration.py          # Sistema di integrazione e testing
│   ├── services/
│   │   └── etl/
│   │       └── reactive_etl.py     # Sistema ETL reattivo
│   └── ui/
│       └── main.py                 # UI completamente dinamica
├── data/                           # Directory dati (creata automaticamente)
│   ├── raw/                        # Dati grezzi
│   ├── processed/                  # Dati processati
│   └── warehouse/                  # Data warehouse
└── logs/                           # Log sistema (creata automaticamente)
```

## Flusso di Lavoro

### 1. Avvio Sistema
```
python main.py
├── Bootstrap Automatico
│   ├── Creazione Directory
│   ├── Scoperta HubSpot (se API key disponibile)
│   ├── Configurazione Dinamica
│   └── Validazione Sistema
├── Avvio ETL Reattivo
├── Avvio UI Dinamica
└── Monitoraggio Continuo
```

### 2. Auto-Discovery HubSpot
```
Bootstrap Manager
├── Analisi Struttura Deal
├── Scoperta Pipeline Stages
├── Identificazione Proprietà
├── Generazione Configurazione
└── Validazione Finale
```

### 3. ETL Reattivo
```
Reactive ETL Manager
├── Monitoraggio File (ogni 5 minuti)
├── Rilevamento Nuovi Dati
├── Attivazione Pipeline ETL
├── Elaborazione Dati
├── Controllo Qualità
└── Governance Privacy
```

### 4. UI Dinamica
```
Dynamic UI Manager
├── Rilevamento Dati Disponibili
├── Caricamento Dati Reali
├── Calcolo Metriche Dinamiche
├── Generazione Visualizzazioni
└── Aggiornamento in Tempo Reale
```

## Output del Sistema

### Bootstrap Output
```
🚀 Avvio bootstrap sistema Process Mining
📁 Setup directory sistema
✅ Setup directory completato: 7 directory create/verificate
🔍 Verifica configurazione esistente
📊 Analisi struttura deal HubSpot
🔄 Scoperta pipeline stages
📋 Scoperta proprietà disponibili
⚙️ Generazione configurazione raccomandata
💾 Applicazione configurazione scoperta
✅ Bootstrap sistema completato con successo
```

### ETL Output
```
🔄 Avvio sistema ETL reattivo
📁 Trovati 3 file recenti, attivazione ETL
📥 Inizio estrazione dati
🔄 Inizio trasformazione dati
🔍 Inizio controllo qualità dati
🔒 Inizio governance privacy
💾 Salvataggio risultati
✅ Pipeline ETL completata con successo
```

### UI Output
```
🖥️ Avvio interfaccia utente
📊 Process Mining Dashboard
📈 Panoramica Generale
📊 150 eventi | 🔄 25 casi | 📋 8 attività
🔄 Mappa Processo
📊 Key Performance Indicators
⚠️ Rilevamento Anomalie
```

## Risoluzione dei Problemi

### Problemi Comuni

#### 1. HubSpot API Key Non Disponibile
```
⚠️ HubSpot API key non disponibile, creazione configurazione base
✅ Configurazione base creata
```
**Soluzione**: Aggiungi la tua API key al file `.env`

#### 2. Directory Non Create
```
❌ Errore nella creazione directory: Permission denied
```
**Soluzione**: Verifica i permessi della directory di esecuzione

#### 3. Errori di Connessione
```
❌ Errore API HubSpot durante discovery: Connection timeout
```
**Soluzione**: Controlla la connessione internet e la validità dell'API key

#### 4. Errori UI
```
❌ Errore applicazione UI: ModuleNotFoundError
```
**Soluzione**: Installa le dipendenze mancanti con `pip install -r requirements.txt`

### Log di Sistema
Tutti i log sono salvati nella directory `logs/`:
- `app.log`: Log principale dell'applicazione
- `bootstrap_results/`: Risultati dei bootstrap
- `integration_tests/`: Risultati dei test di integrazione

### Debug
Per abilitare il debug dettagliato:
```bash
export LOG_LEVEL=DEBUG
python main.py
```

## Personalizzazione

### Configurazione ETL
Modifica `app/core/config.py` per personalizzare:
- Intervalli di estrazione
- Soglie di qualità dati
- Configurazioni privacy

### Configurazione UI
Modifica `app/ui/main.py` per personalizzare:
- Layout dashboard
- Metriche visualizzate
- Stili e temi

### Configurazione Bootstrap
Modifica `app/core/bootstrap.py` per personalizzare:
- Logica di discovery
- Criteri di configurazione
- Validazione sistema

## Contribuire

1. Fork del repository
2. Crea un branch per la tua feature: `git checkout -b feature/nome-feature`
3. Commit delle modifiche: `git commit -m 'Aggiunta feature X'`
4. Push sul branch: `git push origin feature/nome-feature`
5. Apri una Pull Request

## Licenza

Questo progetto è rilasciato sotto licenza MIT. Vedi il file `LICENSE` per i dettagli.

## Supporto

Per supporto e domande:
- Crea una Issue su GitHub
- Controlla i log di sistema
- Verifica la configurazione HubSpot
- Esegui il test di sistema: `python main.py --test-only`