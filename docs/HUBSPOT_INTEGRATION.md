# Integrazione HubSpot External Cards

Guida completa per integrare il Process Mining Dashboard in HubSpot tramite External Cards.

## Panoramica

Questa integrazione permette di mostrare il tuo Process Mining Dashboard direttamente all'interno dell'interfaccia HubSpot, senza che gli utenti debbano uscire dalla piattaforma.

## Architettura dell'Integrazione

```
HubSpot Dashboard
    ↓ (External Card)
    Iframe
    ↓ (HTTPS)
    React Frontend (Port 3000)
    ↓ (API Calls)
    FastAPI Backend (Port 8000)
    ↓ (Data Processing)
    Process Mining System
```

## Prerequisiti

### Tecnici
- [x] Backend FastAPI in esecuzione
- [x] Frontend React buildato e deployato
- [x] HTTPS per il frontend (obbligatorio per HubSpot)
- [x] CORS configurato nel backend

### HubSpot
- [x] Account HubSpot con permessi admin
- [x] Accesso a Settings → Apps → External Cards
- [x] API Key HubSpot (se necessaria per l'autenticazione)

## Passo 1: Configurazione Backend

### 1.1 Avvia il Backend
```bash
# Nella root del progetto
python main.py --mode full
```

### 1.2 Configura CORS
Assicurati che il backend permetta le chiamate dal tuo dominio frontend:

```python
# In app/api/main.py
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://tuodominio.com"],  # URL del frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### 1.3 Verifica API
Testa che le API siano raggiungibili:
```bash
curl http://localhost:8000/api/v1/processes
```

## Passo 2: Configurazione Frontend

### 2.1 Installa Dipendenze
```bash
cd frontend
npm install
```

### 2.2 Configura URL Backend
Crea un file `.env` nella directory frontend:

```bash
# frontend/.env
VITE_API_URL=https://backend-tuodominio.com/api/v1
```

### 2.3 Build per Produzione
```bash
npm run build
```

### 2.4 Deploy Frontend
Opzioni di deploy:

#### Render.com (Consigliato)
1. Crea account su [Render.com](https://render.com)
2. Connetti il repository GitHub
3. Crea Web Service:
   - Build Command: `npm run build`
   - Start Command: `npm run preview`
   - Environment: Production

#### Railway
1. Crea account su [Railway.app](https://railway.app)
2. Importa repository
3. Configura variabili d'ambiente

## Passo 3: Configurazione HubSpot

### 3.1 Crea External Card

1. **Accedi a HubSpot**
   - Vai su `settings.hubspot.com`
   - Naviga a **Apps** → **External Cards**

2. **Crea Nuova External Card**
   - Clicca su **Create External Card**
   - Compila i campi:

```json
{
  "name": "Process Mining Dashboard",
  "description": "Dashboard per l'analisi dei processi aziendali",
  "iframe_url": "https://frontend-tuodominio.com",
  "width": 1200,
  "height": 800,
  "permissions": ["read_deals", "read_contacts", "read_companies"],
  "supported_objects": ["deal", "contact", "company"]
}
```

3. **Configura Dimensioni**
   - **Width**: 1200px (consigliato)
   - **Height**: 800px (consigliato)
   - **Responsive**: Abilita se possibile

### 3.2 Configurazione Sicurezza

#### Content Security Policy
Aggiungi CSP header nel backend:

```python
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware

# Aggiungi header CSP
@app.middleware("http")
async def add_csp_header(request: Request, call_next):
    response = await call_next(request)
    response.headers["Content-Security-Policy"] = "frame-ancestors 'self' https://*.hubspot.com;"
    return response
```

#### CORS Headers
```python
# Backend CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://*.hubspot.com"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)
```

### 3.3 Test Integrazione

1. **Verifica Iframe**
   - Apri HubSpot
   - Vai alla pagina dove hai aggiunto l'External Card
   - Verifica che il dashboard carichi correttamente

2. **Test Funzionalità**
   - Verifica che la lista processi carichi
   - Testa l'avvio analisi
   - Controlla la visualizzazione risultati

## Passo 4: Configurazione Avanzata

### 4.1 Autenticazione OAuth 2.0

#### Setup OAuth in HubSpot
1. **Crea App in HubSpot Developer Portal**
   - Vai su [developers.hubspot.com](https://developers.hubspot.com)
   - Crea una nuova app
   - Configura OAuth 2.0

2. **Configura Redirect URI**
   ```
   https://frontend-tuodominio.com/auth/callback
   ```

3. **Ottieni Credenziali**
   - Client ID
   - Client Secret
   - Scopes: `crm.objects.deals.read`, `crm.objects.contacts.read`

#### Implementa Auth nel Frontend
```typescript
// frontend/src/services/auth.ts
export const authenticateWithHubSpot = async () => {
  const clientId = process.env.VITE_HUBSPOT_CLIENT_ID;
  const redirectUri = 'https://frontend-tuodominio.com/auth/callback';
  
  const authUrl = `https://app.hubspot.com/oauth/authorize?client_id=${clientId}&redirect_uri=${redirectUri}&scope=crm.objects.deals.read`;
  
  window.location.href = authUrl;
};
```

### 4.2 Passaggio Parametri da HubSpot

#### Ricevi Parametri nel Frontend
```typescript
// frontend/src/hooks/useHubSpotParams.ts
import { useEffect, useState } from 'react';

export const useHubSpotParams = () => {
  const [hubSpotParams, setHubSpotParams] = useState<any>({});
  
  useEffect(() => {
    // HubSpot passa parametri via URL
    const urlParams = new URLSearchParams(window.location.search);
    const dealId = urlParams.get('dealId');
    const contactId = urlParams.get('contactId');
    
    setHubSpotParams({
      dealId,
      contactId,
      // Altri parametri HubSpot
    });
  }, []);
  
  return hubSpotParams;
};
```

#### Configura External Card con Parametri
```json
{
  "iframe_url": "https://frontend-tuodominio.com?dealId={{deal.id}}&contactId={{contact.id}}",
  "supported_objects": ["deal", "contact"]
}
```

### 4.3 Comunicazione tra Iframe e HubSpot

#### Messaggistica PostMessage
```typescript
// frontend/src/services/hubspotBridge.ts

// Invia messaggi a HubSpot
export const sendMessageToHubSpot = (message: any) => {
  window.parent.postMessage({
    source: 'process-mining-dashboard',
    data: message
  }, 'https://app.hubspot.com');
};

// Ricevi messaggi da HubSpot
export const listenToHubSpotMessages = (callback: (data: any) => void) => {
  window.addEventListener('message', (event) => {
    if (event.origin !== 'https://app.hubspot.com') return;
    
    if (event.data.source === 'hubspot') {
      callback(event.data.data);
    }
  });
};
```

## Passo 5: Testing e Debug

### 5.1 Testing Locale

#### Setup Reverse Proxy per HTTPS
```bash
# Installa ngrok per testing HTTPS locale
npm install -g ngrok

# Esegui backend
python main.py --mode full

# Esegui frontend
cd frontend && npm run dev

# Crea tunnel HTTPS
ngrok http 3000  # Frontend
ngrok http 8000  # Backend
```

#### Configura HubSpot per Testing
```json
{
  "iframe_url": "https://tuo-ngrok-id.ngrok.io",
  "width": 1200,
  "height": 800
}
```

### 5.2 Debug Tools

#### Browser Console
- Controlla errori CORS
- Verifica chiamate API
- Monitora messaggi iframe

#### Network Tab
- Verifica richieste HTTPS
- Controlla headers
- Monitora tempi di risposta

#### HubSpot Developer Tools
- Console HubSpot
- Network tab HubSpot
- Errori iframe

### 5.3 Common Issues

#### Problema: Iframe non carica
**Soluzione:**
- Verifica HTTPS
- Controlla CSP headers
- Verifica CORS configuration

#### Problema: API calls falliscono
**Soluzione:**
- Controlla CORS
- Verifica URL backend
- Controlla autenticazione

#### Problema: Dimensioni iframe sbagliate
**Soluzione:**
- Aggiorna dimensioni in HubSpot
- Usa CSS responsive
- Testa su diversi browser

## Passo 6: Deploy Produzione

### 6.1 Checklist Deploy

#### Backend
- [ ] HTTPS configurato
- [ ] CORS abilitato per dominio frontend
- [ ] CSP headers corretti
- [ ] Database configurato
- [ ] Logging attivo

#### Frontend
- [ ] Build ottimizzato
- [ ] HTTPS attivo
- [ ] Variabili d'ambiente configurate
- [ ] Error handling implementato

#### HubSpot
- [ ] External Card configurata
- [ ] Dimensioni corrette
- [ ] Permessi verificati
- [ ] Test completi

### 6.2 Monitoraggio

#### Backend Monitoring
```python
# Aggiungi logging nel backend
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Request: {request.method} {request.url}")
    response = await call_next(request)
    logger.info(f"Response: {response.status_code}")
    return response
```

#### Frontend Monitoring
```typescript
// Aggiungi error boundary
import { ErrorBoundary } from 'react-error-boundary';

function ErrorFallback({ error, resetErrorBoundary }) {
  return (
    <div>
      <p>Si è verificato un errore:</p>
      <pre>{error.message}</pre>
      <button onClick={resetErrorBoundary}>Riprova</button>
    </div>
  );
}

// Usa error boundary
<ErrorBoundary FallbackComponent={ErrorFallback}>
  <App />
</ErrorBoundary>
```

## Documentazione di Riferimento

### HubSpot Documentation
- [External Cards Guide](https://developers.hubspot.com/docs/api/crm/external-cards)
- [OAuth 2.0 Integration](https://developers.hubspot.com/docs/api/working-with-oauth)
- [CORS Configuration](https://developers.hubspot.com/docs/api/working-with-cors)

### React & FastAPI
- [React Documentation](https://react.dev)
- [FastAPI Documentation](https://fastapi.tiangolo.com)
- [Vite Documentation](https://vitejs.dev)

## Supporto

### Contatti
- **Email**: support@processmining.com
- **GitHub Issues**: [link al repository]
- **Documentation**: [link alla documentazione]

### Troubleshooting
1. Controlla la console browser per errori
2. Verifica la connettività API
3. Controlla la configurazione CORS
4. Testa con ngrok per ambiente locale
5. Controlla i log backend/frontend