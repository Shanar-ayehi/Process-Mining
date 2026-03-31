import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import './index.css'
import axios from 'axios';

// Interceptor per aggiungere il token JWT a tutte le richieste
axios.interceptors.request.use((config) => {
  const token = localStorage.getItem('access_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
)
axios.defaults.headers.common['bypass-tunnel-reminder'] = 'true'
