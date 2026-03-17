import React, { useState, useEffect } from 'react'
import { BrowserRouter as Router, Routes, Route, Link, Navigate, useLocation } from 'react-router-dom'
import { CssBaseline, Container, Box, AppBar, Toolbar, Typography, Button, Snackbar, Alert } from '@mui/material'
import { Dashboard as DashboardIcon, Analytics as AnalyticsIcon, Settings as SettingsIcon, Login as LoginIcon, Logout as LogoutIcon } from '@mui/icons-material'

// Importiamo i componenti delle pagine
import ProcessList from './components/ProcessList'
import ProcessDetail from './components/ProcessDetail'
import ProcessAnalysis from './components/ProcessAnalysis'

// Importiamo il servizio auth
import { useAuth, checkAuthStatus } from './services/auth'

function App() {
  return (
    <Router>
      <CssBaseline />
      <div className="process-mining-app">
        {/* Header */}
        <AppBar position="static" color="primary">
          <Toolbar>
            <Box display="flex" alignItems="center" justifyContent="space-between" width="100%">
              <Box display="flex" alignItems="center">
                <AnalyticsIcon sx={{ mr: 1 }} />
                <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
                  Process Mining Dashboard
                </Typography>
              </Box>
              
              {/* Navigation */}
              <Box>
                <Button color="inherit" component={Link} to="/" startIcon={<DashboardIcon />}>
                  Processi
                </Button>
                <Button color="inherit" component={Link} to="/analysis" startIcon={<AnalyticsIcon />}>
                  Analisi
                </Button>
                <Button color="inherit" component={Link} to="/settings" startIcon={<SettingsIcon />}>
                  Impostazioni
                </Button>
              </Box>
            </Box>
          </Toolbar>
        </AppBar>

        {/* Main Content */}
        <Container maxWidth="xl" className="app-container">
          <Routes>
            <Route path="/" element={<ProcessList />} />
            <Route path="/process/:processId" element={<ProcessDetail />} />
            <Route path="/process/:processId/analysis" element={<ProcessAnalysis />} />
            <Route path="/analysis" element={<div>Analisi Generale</div>} />
            <Route path="/settings" element={<div>Impostazioni</div>} />
          </Routes>
        </Container>
      </div>
    </Router>
  )
}

export default App