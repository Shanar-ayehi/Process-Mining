import React, { useState, useEffect } from 'react'
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom'
import { CssBaseline, Container, Box, AppBar, Toolbar, Typography, Button } from '@mui/material'
import { Dashboard as DashboardIcon, Analytics as AnalyticsIcon, Settings as SettingsIcon } from '@mui/icons-material'
import { ReactFlowProvider } from '@xyflow/react'

// Importiamo i componenti
import ProcessList from './components/ProcessList'
import ProcessDetail from './components/ProcessDetail'
import ProcessAnalysis from './components/ProcessAnalysis'
import GlobalAnalysis from './components/GlobalAnalysis'
import AuthCallback from './components/AuthCallback' // <-- AGGIUNTO QUESTO
import { checkAuthStatus } from './services/auth'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api/v1'

const ProtectedRoute: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [isAuthenticated, setIsAuthenticated] = useState<boolean | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const checkAuth = async () => {
      // Controllo istantaneo sul localStorage
      const localToken = localStorage.getItem('access_token');
      if (!localToken) {
        setIsAuthenticated(false);
        setLoading(false);
        return;
      }

      try {
        const status = await checkAuthStatus();
        setIsAuthenticated(status.authenticated);
      } catch (error) {
        console.error("Auth check failed", error);
        setIsAuthenticated(false);
      } finally {
        setLoading(false);
      }
    };
    checkAuth();
  }, []);

  if (loading) return <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px"><Typography>Verifica autenticazione...</Typography></Box>

  if (isAuthenticated === false) {
    window.location.href = `${API_URL}/auth/hubspot/login`
    return null
  }
  return <>{children}</>
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