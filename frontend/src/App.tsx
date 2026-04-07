import React from 'react'
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom'
import { CssBaseline, Container, Box, AppBar, Toolbar, Typography, Button } from '@mui/material'
import { Dashboard as DashboardIcon, Analytics as AnalyticsIcon, Settings as SettingsIcon } from '@mui/icons-material'
import { ReactFlowProvider } from '@xyflow/react'

// Importiamo i componenti
import ProcessList from './components/ProcessList'
import ProcessDetail from './components/ProcessDetail'
import ProcessAnalysis from './components/ProcessAnalysis'
import GlobalAnalysis from './components/GlobalAnalysis'
import AuthCallback from './components/AuthCallback'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api/v1'

const ProtectedRoute: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  // Controllo immediato: c'è un token in localStorage?
  const token = localStorage.getItem('token');

  if (!token) {
    // Niente token, vai a fare il login
    window.location.href = `${API_URL}/auth/hubspot/login`;
    return null;
  }

  // Se il token c'è, entra direttamente.
  // Le chiamate API useranno questo token e restituiranno 401 se non valido.
  return <>{children}</>;
}

function App() {
  return (
    <Router>
      <CssBaseline />
      <div className="process-mining-app">
        <AppBar position="static" color="primary">
          <Toolbar>
            <Box display="flex" alignItems="center" justifyContent="space-between" width="100%">
              <Box display="flex" alignItems="center">
                <AnalyticsIcon sx={{ mr: 1 }} />
                <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
                  Process Mining Dashboard
                </Typography>
              </Box>
              <Box>
                <Button color="inherit" component={Link} to="/" startIcon={<DashboardIcon />}>Processi</Button>
                <Button color="inherit" component={Link} to="/analysis" startIcon={<AnalyticsIcon />}>Analisi</Button>
                <Button color="inherit" component={Link} to="/settings" startIcon={<SettingsIcon />}>Impostazioni</Button>
              </Box>
            </Box>
          </Toolbar>
        </AppBar>
        <Container maxWidth="xl" className="app-container">
          <ReactFlowProvider>
            <Routes>
              {/* <-- AGGIUNTA LA ROTTA DI CALLBACK QUI (Fuori da ProtectedRoute) --> */}
              <Route path="/auth/success" element={<AuthCallback />} />
              
              <Route path="/" element={<ProtectedRoute><ProcessList /></ProtectedRoute>} />
              <Route path="/process/:processId" element={<ProtectedRoute><ProcessDetail /></ProtectedRoute>} />
              <Route path="/analysis/:id" element={<ProtectedRoute><ProcessAnalysis /></ProtectedRoute>} />
              <Route path="/analysis" element={<ProtectedRoute><GlobalAnalysis /></ProtectedRoute>} />
              <Route path="/settings" element={<ProtectedRoute><div>Impostazioni</div></ProtectedRoute>} />
            </Routes>
          </ReactFlowProvider>
        </Container>
      </div>
    </Router>
  )
}
export default App