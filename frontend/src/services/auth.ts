import axios from 'axios';

// Configurazione base API
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000/api/v1';

// Interfacce per la gestione auth
export interface AuthTokens {
  access_token: string;
  refresh_token: string;
  expires_in: number;
  token_type: string;
}

export interface User {
  user_id: string;
  email: string;
  hub_id: string;
  scopes: string[];
}

// Funzione per ottenere il redirect URI dinamico
function getRedirectUri(): string {
  // Prova a leggere da variabile d'ambiente Vite
  const envRedirectUri = import.meta.env.VITE_HUBSPOT_REDIRECT_URI;
  if (envRedirectUri) {
    return envRedirectUri;
  }
  
  // Fallback dinamico: usa l'origin corrente + /auth/callback
  return `${window.location.origin}/auth/callback`;
}

// Funzione per iniziare il flusso OAuth con HubSpot
export async function authenticateWithHubSpot(): Promise<void> {
  try {
    const redirectUri = getRedirectUri();
    const scopes = [
      'crm.objects.deals.read',
      'crm.objects.deals.write',
      'crm.objects.contacts.read',
      'crm.objects.contacts.write',
      'crm.objects.companies.read',
      'timeline.events.read',
      'timeline.events.write',
      'engagements.read',
      'settings.user.read'
    ].join(' ');
    
    // Costruisci URL di autorizzazione
    const authUrl = `https://app.hubspot.com/oauth/authorize?client_id=${import.meta.env.VITE_HUBSPOT_CLIENT_ID}&redirect_uri=${encodeURIComponent(redirectUri)}&scope=${encodeURIComponent(scopes)}&response_type=code`;
    
    // Redirect all'autorizzazione HubSpot
    window.location.href = authUrl;
  } catch (error) {
    console.error('Errore durante l\'autenticazione con HubSpot:', error);
    throw new Error('Impossibile avviare l\'autenticazione con HubSpot');
  }
}

// Funzione per gestire il callback OAuth
export async function handleAuthCallback(code: string): Promise<AuthTokens> {
  try {
    const response = await axios.get(`${API_BASE_URL}/auth/callback`, {
      params: { code }
    });
    
    // Salva token nel localStorage
    if (response.data.access_token) {
      localStorage.setItem('access_token', response.data.access_token);
      localStorage.setItem('token_expires_at', new Date(Date.now() + response.data.expires_in * 1000).toISOString());
    }
    
    return response.data;
  } catch (error) {
    console.error('Errore durante il callback OAuth:', error);
    throw new Error('Autenticazione fallita');
  }
}

// Funzione per verificare lo stato dell'autenticazione
export async function checkAuthStatus(): Promise<{ authenticated: boolean; user?: User }> {
  try {
    const response = await axios.get(`${API_BASE_URL}/auth/status`);
    return response.data;
  } catch (error) {
    console.error('Errore durante il controllo autenticazione:', error);
    return { authenticated: false };
  }
}

// Funzione per effettuare il logout
export async function logout(): Promise<void> {
  try {
    await axios.post(`${API_BASE_URL}/auth/logout`);
    localStorage.removeItem('access_token');
    localStorage.removeItem('token_expires_at');
  } catch (error) {
    console.error('Errore durante il logout:', error);
    throw new Error('Logout fallito');
  }
}

// Funzione per rinnovare il token
export async function refreshToken(): Promise<AuthTokens> {
  try {
    const refreshToken = localStorage.getItem('refresh_token');
    if (!refreshToken) {
      throw new Error('Nessun refresh token disponibile');
    }
    
    const response = await axios.post(`${API_BASE_URL}/auth/refresh`, {
      refresh_token: refreshToken
    });
    
    // Salva nuovo token
    if (response.data.access_token) {
      localStorage.setItem('access_token', response.data.access_token);
      localStorage.setItem('token_expires_at', new Date(Date.now() + response.data.expires_in * 1000).toISOString());
    }
    
    return response.data;
  } catch (error) {
    console.error('Errore durante il refresh token:', error);
    throw new Error('Token refresh fallito');
  }
}

// Funzione per ottenere il token corrente
export function getAccessToken(): string | null {
  return localStorage.getItem('access_token');
}

// Funzione per verificare se il token è scaduto
export function isTokenExpired(): boolean {
  const expiresAt = localStorage.getItem('token_expires_at');
  if (!expiresAt) return true;
  
  return new Date() >= new Date(expiresAt);
}

// Funzione per ottenere header di autorizzazione
export function getAuthHeaders(): { Authorization: string } {
  const token = getAccessToken();
  if (!token) {
    throw new Error('Nessun token disponibile');
  }
  return { Authorization: `Bearer ${token}` };
}

// Hook per gestire l'autenticazione (da usare nei componenti)
export const useAuth = () => {
  const login = async () => {
    await authenticateWithHubSpot();
  };

  const logoutUser = async () => {
    await logout();
  };

  const checkStatus = async () => {
    return await checkAuthStatus();
  };

  return {
    login,
    logout: logoutUser,
    checkStatus,
    getAccessToken,
    isTokenExpired,
    getAuthHeaders
  };
};