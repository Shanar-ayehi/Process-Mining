# Process Mining React UI - Dashboard Interattiva

Interfaccia utente React per il sistema Process Mining con **React Flow** per visualizzazione interattiva dei processi e funzionalità **What-If Analysis**.

## Panoramica

Questa UI fornisce un'interfaccia web professionale per:

- **Dashboard Interattiva**: Canvas full-screen con grafo processo usando React Flow
- **Visualizzazione Processi**: Nodi custom con badge automazioni HubSpot ⚡️
- **Filtro Rumore**: Slider continuo per filtrare archi per frequenza
- **What-If Analysis**: Sidebar per modificare tempi e automazioni
- **Simulazione**: Progress indicator per simulazioni asincrone

## Struttura del Progetto

```
frontend/
├── src/
│   ├── components/              # Componenti React
│   │   ├── ProcessList.tsx      # Lista processi con statistiche
│   │   ├── ProcessAnalysis.tsx  # Canvas React Flow full-screen
│   │   ├── CustomNode.tsx       # Nodo custom con badge automazioni
│   │   ├── WhatIfSidebar.tsx    # Sidebar What-If Analysis
│   │   └── ProcessDetail.tsx    # Dettaglio processo
│   ├── services/
│   │   └── auth.ts              # Servizio autenticazione
│   ├── App.tsx                  # Routing principale con ReactFlowProvider
│   ├── main.tsx                 # Entry point
│   └── index.css                # Stili (inclusi React Flow)
├── package.json                 # Dipendenze (incluso @xyflow/react)
├── vite.config.ts               # Configurazione Vite
├── tsconfig.json                # Configurazione TypeScript
└── README.md                    # Questo file
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

L'app sarà disponibile su `http://localhost:5173`

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
echo "VITE_API_BASE_URL=http://localhost:8000/api/v1" > .env
echo "VITE_API_URL=http://localhost:8000/api/v1" >> .env
```

### Variabili d'ambiente
| Variabile | Descrizione | Default |
|-----------|-------------|---------|
| `VITE_API_BASE_URL` | URL base backend per Axios | `http://localhost:8000/api/v1` |
| `VITE_API_URL` | URL completo per redirect auth | `http://localhost:8000/api/v1` |
| `VITE_HUBSPOT_CLIENT_ID` | Client ID app HubSpot | - |
| `VITE_HUBSPOT_REDIRECT_URI` | URI redirect OAuth | Auto-generato |

## Autenticazione

### Flusso OAuth 2.0 con HubSpot

Il frontend gestisce l'autenticazione tramite OAuth 2.0 con HubSpot:

1. **Login**: L'utente clicca "Accedi" e viene reindirizzato a HubSpot
2. **Autorizzazione**: HubSpot chiede i permessi all'utente
3. **Callback**: HubSpot reindirizza a `/auth/success?token=<JWT>`
4. **Salvataggio**: Il componente `AuthCallback` salva il JWT in `localStorage` come `'token'`
5. **Accesso**: `ProtectedRoute` controlla il token e consente l'accesso immediato

### Axios Request Interceptor

Ogni chiamata API invia automaticamente il token JWT tramite un interceptor configurato in `src/services/auth.ts`:

```typescript
axios.interceptors.request.use((config) => {
  const token = localStorage.getItem('token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});
```

### ProtectedRoute

Il componente `ProtectedRoute` in `App.tsx` implementa la strategia **"trust local token"**:

```typescript
const ProtectedRoute: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const token = localStorage.getItem('token');

  if (!token) {
    // Niente token → redirect al login
    window.location.href = `${API_URL}/auth/hubspot/login`;
    return null;
  }

  // Token presente → accesso immediato
  return <>{children}</>;
}
```

**Vantaggi:**
- Nessuna chiamata API all'avvio (istantaneo)
- L'autorizzazione reale avviene quando le API restituiscono 401 se il token è scaduto
- Elimina loop di redirect causati da errori di rete o configurazione

### Servizio Auth

Le funzioni di autenticazione sono in `src/services/auth.ts`:

| Funzione | Descrizione |
|----------|-------------|
| `authenticateWithHubSpot()` | Redirect a HubSpot OAuth |
| `handleAuthCallback(code)` | Gestisce callback OAuth |
| `checkAuthStatus()` | Verifica stato autenticazione |
| `logout()` | Effettua logout |
| `getAccessToken()` | Ottiene token corrente |
| `isTokenExpired()` | Verifica scadenza token |
| `getAuthHeaders()` | Genera header Authorization |

## Componenti Principali

### ProcessList
- **Funzione**: Mostra lista processi disponibili
- **Features**:
  - Ricerca e filtro per nome/descrizione
  - Statistiche riassuntive (processi totali, attivi, varianti)
  - Cards con informazioni processo (varianti, casi, attività, tempo medio)
  - Quality score con progress bar
  - Avvio analisi processi (apre nuova tab)

### ProcessAnalysis ← **NUOVO**
- **Funzione**: Canvas interattivo full-screen con React Flow
- **Features**:
  - **React Flow Canvas**: Visualizzazione grafo processo a schermo intero
  - **Nodi Custom**: Con badge automazioni ⚡️ e tempo medio
  - **Filtro Frequenza**: Slider continuo 0-100% per filtrare archi
  - **Stats Chips**: Conteggio nodi Start, End, Automazioni
  - **MiniMap**: Navigazione stile Google Maps
  - **Controls**: Zoom, pan, fit view
  - **Background**: Griglia per riferimento visivo
  - **Click Nodo**: Apre sidebar What-If

### CustomNode ← **NUOVO**
- **Funzione**: Nodo custom per React Flow
- **Features**:
  - Badge ⚡️ se nodo ha automazioni HubSpot
  - Visualizzazione tempo medio attività
  - Colori diversi per tipo nodo (verde=start, rosso=blu=normal)
  - Chip con tipo nodo (START, END, NORMAL)
  - Contatore automazioni attive
  - Click apre sidebar What-If

### WhatIfSidebar ← **NUOVO**
- **Funzione**: Pannello laterale per analisi What-If
- **Features**:
  - **Informazioni Nodo**: Nome, tipo, tempo medio
  - **Automazioni HubSpot**: Lista workflow con toggle on/off
  - **Controlli What-If**:
    - Slider moltiplicatore tempo (10%-200%)
    - Override delay automazioni (0-30 giorni)
  - **Simulazione**:
    - Bottone "Simula Scenario"
    - Progress bar con percentuale
    - Alert risultato (successo/errore)
  - **Riepilogo Modifiche**: Riepilogo delle modifiche applicate
  - **Chiusura**: Bottone X o click su background canvas

### ProcessDetail
- **Funzione**: Dettagli processo specifico
- **Features**:
  - Informazioni processo
  - Varianti processo
  - Metriche KPI
  - Storico analisi

## API Endpoints Utilizzati

### Process Management
- `GET /api/v1/processes` - Lista processi
- `GET /api/v1/processes/{id}` - Dettagli processo
- `POST /api/v1/processes/{id}/analyze` - Avvia analisi
- `GET /api/v1/processes/{id}/analysis/status` - Stato analisi
- `GET /api/v1/processes/{id}/analysis/results` - Risultati analisi
- `GET /api/v1/processes/{id}/variants` - Varianti processo

### Mining (Nuovi)
- `GET /api/v1/mining/discover/dfg-with-automations/{id}` - DFG con automazioni
- `POST /api/v1/analytics/simulate` - Simulazione What-If
- `POST /api/v1/analytics/simulate/compare` - Confronto scenari

## Dipendenze Principali

### UI Framework
- **@mui/material**: Componenti Material-UI
- **@mui/icons-material**: Icone Material-UI

### Grafi Interattivi ← **NUOVO**
- **@xyflow/react**: React Flow per visualizzazione grafi
  - Canvas interattivo con zoom/pan
  - Nodi e archi customizzabili
  - MiniMap e Controls
  - Auto-layout

### HTTP Client
- **axios**: Chiamate API REST

### Routing
- **react-router-dom**: Navigazione SPA

## Stili e Design

### Material-UI
L'interfaccia utilizza Material-UI per un design professionale e responsive.

### React Flow Styles
Gli stili di React Flow sono in `src/index.css`:
```css
.react-flow {
  background-color: #fafafa;
}

.react-flow__node-custom {
  border-radius: 8px;
  padding: 10px;
}

.react-flow__controls {
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
  border-radius: 8px;
}
```

### Personalizzazione
Modifica i temi in `src/App.tsx` per personalizzare colori e stili.

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
   - OAuth 2.0 con scope `automation`

### Esempio Configurazione HubSpot
```json
{
  "name": "Process Mining Dashboard",
  "description": "Dashboard per l'analisi dei processi aziendali con What-If Analysis",
  "iframe_url": "https://tuodominio.com/process-mining-ui",
  "width": 1200,
  "height": 800,
  "permissions": ["read_deals", "read_contacts", "automation"]
}
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

### Docker
```bash
# Build immagine
docker build -t process-mining-frontend .

# Run container
docker run -p 5173:5173 process-mining-frontend
```

## Monitoraggio e Debug

### Logging
- Errori API: Console browser
- Stato analisi: Real-time updates
- Performance: Vite dev server

### Strumenti
- **React DevTools**: Debug componenti
- **React Flow DevTools**: Debug grafi
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
- OAuth 2.0 authentication con scope `automation`

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
- Consulta docs/HUBSPOT_INTEGRATION.md per dettagli integrazione