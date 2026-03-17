# Process Mining React UI

Interfaccia utente React per il sistema Process Mining, progettata per essere integrata in HubSpot tramite External Cards.

## Panoramica

Questa UI fornisce un'interfaccia web professionale per:

- **Visualizzazione Processi**: Lista di tutti i processi/workflow disponibili
- **Analisi Processi**: Avvio e monitoraggio dell'analisi dei processi
- **Dashboard Interattive**: Grafici e visualizzazioni dei risultati
- **Gestione Varianti**: Visualizzazione delle diverse varianti di ogni processo

## Struttura del Progetto

```
frontend/
├── src/
│   ├── components/          # Componenti React
│   │   ├── ProcessList.tsx  # Lista dei processi
│   │   ├── ProcessDetail.tsx # Dettagli processo
│   │   └── ProcessAnalysis.tsx # Analisi processo
│   ├── App.tsx               # Componente principale
│   ├── main.tsx              # Entry point
│   └── index.css             # Stili globali
├── package.json              # Dipendenze e script
├── vite.config.ts            # Configurazione Vite
├── tsconfig.json             # Configurazione TypeScript
└── README.md                 # Questo file
```

## Installazione

### Prerequisiti
- Node.js 18+ 
- npm o yarn

### Setup
```bash
# Nella directory frontend/
npm install

# Oppure con yarn
yarn install
```

## Avvio Sviluppo

```bash
# Avvia server di sviluppo
npm run dev

# Oppure
yarn dev
```

L'app sarà disponibile su `http://localhost:3000`

## Build per Produzione

```bash
# Crea build ottimizzata
npm run build

# Oppure
yarn build
```

I file saranno generati nella directory `dist/`

## Configurazione API

### Backend URL
Configura l'URL del backend nel file `.env`:

```bash
# Crea file .env nella root di frontend/
echo "VITE_API_URL=http://localhost:8000/api/v1" > .env
```

### Variabili d'ambiente
- `VITE_API_URL`: URL del backend API
- `VITE_HUBSPOT_APP_ID`: ID app HubSpot (se necessario)

## Integrazione HubSpot

### External Cards Setup
1. **Crea External Card in HubSpot**:
   - Vai in HubSpot → Settings → Apps → External Cards
   - Crea una nuova External Card
   - Imposta l'URL dell'iframe al tuo frontend

2. **Configurazione Iframe**:
   - URL: `https://tuodominio.com/process-mining-ui`
   - Dimensioni: 1200x800px (consigliato)
   - Abilita CORS se necessario

3. **Sicurezza**:
   - HTTPS obbligatorio per HubSpot
   - Configura CORS nel backend
   - Considera autenticazione OAuth 2.0

### Esempio Configurazione HubSpot
```json
{
  "name": "Process Mining Dashboard",
  "description": "Dashboard per l'analisi dei processi aziendali",
  "iframe_url": "https://tuodominio.com/process-mining-ui",
  "width": 1200,
  "height": 800,
  "permissions": ["read_deals", "read_contacts"]
}
```

## Componenti Principali

### ProcessList
- **Funzione**: Mostra lista processi disponibili
- **Features**: 
  - Ricerca e filtro
  - Statistiche riassuntive
  - Avvio analisi processi
  - Stato processi in tempo reale

### ProcessDetail  
- **Funzione**: Dettagli processo specifico
- **Features**:
  - Informazioni processo
  - Varianti processo
  - Metriche KPI
  - Storico analisi

### ProcessAnalysis
- **Funzione**: Monitoraggio analisi processo
- **Features**:
  - Stato analisi in tempo reale
  - Risultati analisi
  - Visualizzazioni grafiche
  - Download report

## API Endpoints Utilizzati

### Process Management
- `GET /api/v1/processes` - Lista processi
- `GET /api/v1/processes/{id}` - Dettagli processo
- `POST /api/v1/processes/{id}/analyze` - Avvia analisi
- `GET /api/v1/processes/{id}/analysis/status` - Stato analisi
- `GET /api/v1/processes/{id}/analysis/results` - Risultati analisi
- `GET /api/v1/processes/{id}/variants` - Varianti processo

## Stili e Design

### Material-UI
L'interfaccia utilizza Material-UI per un design professionale e responsive.

### Personalizzazione
Modifica i temi in `src/theme.ts` per personalizzare colori e stili:

```typescript
const theme = createTheme({
  palette: {
    primary: {
      main: '#your-color',
    },
  },
});
```

## Deploy

### Render.com (Consigliato)
1. **Crea account su Render.com**
2. **Connetti repository GitHub**
3. **Configura Web Service**
   - Build Command: `npm run build`
   - Start Command: `npm run preview`
   - Environment: `Production`

4. **Configura variabili d'ambiente**
   - `VITE_API_URL`: URL backend

### Railway
1. **Crea account su Railway.app**
2. **Importa repository**
3. **Configura variabili d'ambiente**
4. **Deploy automatico**

## Monitoraggio e Debug

### Logging
- Errori API: Console browser
- Stato analisi: Real-time updates
- Performance: Vite dev server

### Strumenti
- **React DevTools**: Debug componenti
- **Network Tab**: Monitoraggio API calls
- **Console**: Errori e log

## Sicurezza

### Best Practices
- HTTPS obbligatorio
- CORS configurato correttamente
- Validazione input
- Sanitizzazione dati

### HubSpot Requirements
- iframe sandboxing
- Content Security Policy
- OAuth 2.0 authentication

## Contribuire

1. **Fork del repository**
2. **Crea branch feature**: `git checkout -b feature/nome-feature`
3. **Commit modifiche**: `git commit -m 'Aggiunta feature X'`
4. **Push branch**: `git push origin feature/nome-feature`
5. **Pull Request**

## Supporto

Per supporto e domande:
- Crea issue su GitHub
- Controlla la documentazione HubSpot
- Verifica configurazione API