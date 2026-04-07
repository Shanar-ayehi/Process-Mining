import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import './index.css'
import axios from 'axios';

// L'interceptor per il token JWT è configurato in services/auth.ts

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
)
axios.defaults.headers.common['bypass-tunnel-reminder'] = 'true'
