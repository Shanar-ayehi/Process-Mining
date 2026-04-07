import axios from 'axios';

// Configurazione base API
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000/api/v1';
axios.defaults.baseURL = API_BASE_URL;

// Axios Request Interceptor: aggiunge automaticamente il token JWT a ogni richiesta
axios.interceptors.request.use((config) => {
  const token = localStorage.getItem('token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

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
    const response = await axios.get('/auth/callback', {
      params: { code }
    });
    
    // Salva token nel localStorage
    if (response.data.access_token) {
      localStorage.setItem('token', response.data.access_token);
      localStorage.setItem('token_expires_at', new Date(Date.now() + response.data.expires_in * 1000).toISOString());
    }
    
    return response.data;
  } catch (error) {
    console.error('🔴 ERRORE IN HANDLE AUTH CALLBACK:', error);
    throw new Error('Autenticazione fallita');
  }
}

// Funzione per verificare lo stato dell'autenticazione
export async function checkAuthStatus(): Promise<{ authenticated: boolean; user?: User }> {
  try {
    const response = await axios.get('/auth/status');
    return response.data;
  } catch (error: any) {
    console.error('🔴 ERRORE REALE IN CHECK AUTH STATUS:', {
      message: error?.message,
      status: error?.response?.status,
      data: error?.response?.data,
      url: error?.config?.url
    });
    return { authenticated: false };
  }
}

// Funzione per effettuare il logout
export async function logout(): Promise<void> {
  try {
    await axios.post('/auth/logout');
    localStorage.removeItem('token');
    localStorage.removeItem('token_expires_at');
  } catch (error) {
    console.error('🔴 ERRORE IN LOGOUT:', error);
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
    
    const response = await axios.post('/auth/refresh', {
      refresh_token: refreshToken
    });
    
    // Salva nuovo token
    if (response.data.access_token) {
      localStorage.setItem('token', response.data.access_token);
      localStorage.setItem('token_expires_at', new Date(Date.now() + response.data.expires_in * 1000).toISOString());
    }
    
    return response.data;
  } catch (error) {
    console.error('🔴 ERRORE IN REFRESH TOKEN:', error);
    throw new Error('Token refresh fallito');
  }
}

// Funzione per ottenere il token corrente
export function getAccessToken(): string | null {
  return localStorage.getItem('token');
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