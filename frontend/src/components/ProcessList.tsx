import React, { useState, useEffect } from 'react'
import { 
  Box, 
  Grid, 
  Card, 
  CardContent, 
  Typography, 
  Button, 
  Chip, 
  LinearProgress, 
  Alert,
  TextField,
  InputAdornment
} from '@mui/material'
import { 
  PlayArrow as PlayArrowIcon, 
  Refresh as RefreshIcon,
  Search as SearchIcon,
  TrendingUp as TrendingUpIcon
} from '@mui/icons-material'
import axios from 'axios'

interface ProcessInfo {
  process_id: string
  name: string
  description: string
  status: string
  created_at: string
  last_analyzed?: string
  variants_count: number
  cases_count: number
  activities_count: number
  avg_processing_time?: number
  quality_score?: number
}

const ProcessList: React.FC = () => {
  const [processes, setProcesses] = useState<ProcessInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [searchTerm, setSearchTerm] = useState('')
  const [refreshing, setRefreshing] = useState(false)

  // URL del backend (da configurare per Vite)
  const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000/api/v1'

  const fetchProcesses = async () => {
    try {
      setLoading(true)
      setError(null)
      
      const response = await axios.get(`${API_BASE_URL}/processes`)
      setProcesses(response.data.processes || [])
    } catch (err: any) {
      console.error('Errore nel recupero processi:', err)
      setError(err.message || 'Errore nel recupero processi')
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }

  useEffect(() => {
    fetchProcesses()
  }, [])

  const handleRefresh = () => {
    setRefreshing(true)
    fetchProcesses()
  }

  const handleAnalyzeProcess = async (processId: string) => {
    try {
      await axios.post(`${API_BASE_URL}/processes/${processId}/analyze`)
      // Ricarica la lista per aggiornare gli stati
      fetchProcesses()
    } catch (err: any) {
      console.error('Errore nell\'avvio analisi:', err)
      setError('Errore nell\'avvio analisi')
    }
  }

  const filteredProcesses = processes.filter(process =>
    process.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
    process.description.toLowerCase().includes(searchTerm.toLowerCase())
  )

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'active': return 'success'
      case 'analyzing': return 'warning'
      case 'completed': return 'info'
      default: return 'default'
    }
  }

  if (loading && processes.length === 0) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
        <Box textAlign="center">
          <LinearProgress sx={{ mb: 2 }} />
          <Typography>Caricamento processi...</Typography>
        </Box>
      </Box>
    )
  }

  return (
    <Box>
      {/* Header */}
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Box>
          <Typography variant="h4" component="h1" gutterBottom>
            Processi Disponibili
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Gestisci e analizza i tuoi processi aziendali
          </Typography>
        </Box>
        
        <Box display="flex" gap={2}>
          <Button
            variant="outlined"
            startIcon={<RefreshIcon />}
            onClick={handleRefresh}
            disabled={refreshing}
          >
            {refreshing ? 'Aggiornamento...' : 'Aggiorna'}
          </Button>
        </Box>
      </Box>

      {/* Search Bar */}
      <Box mb={3}>
        <TextField
          fullWidth
          placeholder="Cerca processi per nome o descrizione..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <SearchIcon color="action" />
              </InputAdornment>
            ),
          }}
        />
      </Box>

      {/* Error Alert */}
      {error && (
        <Alert severity="error" sx={{ mb: 3 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      )}

      {/* Stats Summary */}
      <Box mb={3}>
        <Grid container spacing={2}>
          <Grid size={{ xs: 12, sm: 4 }}>
            <Card>
              <CardContent>
                <Box display="flex" alignItems="center" justifyContent="space-between">
                  <Box>
                    <Typography color="text.secondary">Processi Totali</Typography>
                    <Typography variant="h6">{processes.length}</Typography>
                  </Box>
                  <TrendingUpIcon color="primary" sx={{ fontSize: 40 }} />
                </Box>
              </CardContent>
            </Card>
          </Grid>
          <Grid size={{ xs: 12, sm: 4 }}>
            <Card>
              <CardContent>
                <Box display="flex" alignItems="center" justifyContent="space-between">
                  <Box>
                    <Typography color="text.secondary">Processi Attivi</Typography>
                    <Typography variant="h6">{processes.filter(p => p.status === 'active').length}</Typography>
                  </Box>
                  <TrendingUpIcon color="success" sx={{ fontSize: 40 }} />
                </Box>
              </CardContent>
            </Card>
          </Grid>
          <Grid size={{ xs: 12, sm: 4 }}>
            <Card>
              <CardContent>
                <Box display="flex" alignItems="center" justifyContent="space-between">
                  <Box>
                    <Typography color="text.secondary">Varianti Totali</Typography>
                    <Typography variant="h6">{processes.reduce((sum, p) => sum + p.variants_count, 0)}</Typography>
                  </Box>
                  <TrendingUpIcon color="info" sx={{ fontSize: 40 }} />
                </Box>
              </CardContent>
            </Card>
          </Grid>
        </Grid>
      </Box>

      {/* Process Cards */}
      <Grid container spacing={3}>
        {filteredProcesses.map((process) => (
          <Grid size={{ xs: 12, md: 6, lg: 4 }} key={process.process_id}>
            <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
              <CardContent sx={{ flexGrow: 1 }}>
                <Box display="flex" justifyContent="space-between" alignItems="start" mb={2}>
                  <Box>
                    <Typography variant="h6" component="h3" gutterBottom>
                      {process.name}
                    </Typography>
                    <Typography variant="body2" color="text.secondary" paragraph>
                      {process.description}
                    </Typography>
                  </Box>
                  <Chip 
                    label={process.status.toUpperCase()} 
                    color={getStatusColor(process.status) as any}
                    size="small"
                  />
                </Box>

                {/* Process Stats */}
                <Grid container spacing={2} sx={{ mb: 2 }}>
                  <Grid size={6}>
                    <Typography variant="caption" color="text.secondary">Varianti</Typography>
                    <Typography variant="body1">{process.variants_count}</Typography>
                  </Grid>
                  <Grid size={6}>
                    <Typography variant="caption" color="text.secondary">Casi</Typography>
                    <Typography variant="body1">{process.cases_count}</Typography>
                  </Grid>
                  <Grid size={6}>
                    <Typography variant="caption" color="text.secondary">Attività</Typography>
                    <Typography variant="body1">{process.activities_count}</Typography>
                  </Grid>
                  {process.avg_processing_time && (
                    <Grid size={6}>
                      <Typography variant="caption" color="text.secondary">Tempo Medio</Typography>
                      <Typography variant="body1">{process.avg_processing_time.toFixed(1)} giorni</Typography>
                    </Grid>
                  )}
                </Grid>

                {/* Quality Score */}
                {process.quality_score && (
                  <Box mb={2}>
                    <Typography variant="caption" color="text.secondary" display="block" mb={1}>
                      Qualità Dati: {Math.round(process.quality_score * 100)}%
                    </Typography>
                    <LinearProgress 
                      variant="determinate" 
                      value={process.quality_score * 100} 
                      color={process.quality_score > 0.8 ? "success" : "warning"}
                    />
                  </Box>
                )}

                {/* Last Analyzed */}
                {process.last_analyzed && (
                  <Typography variant="caption" color="text.secondary">
                    Ultima analisi: {new Date(process.last_analyzed).toLocaleDateString()}
                  </Typography>
                )}
              </CardContent>

              {/* Actions */}
              <Box sx={{ p: 2, borderTop: 1, borderColor: 'divider' }}>
                <Button
                  fullWidth
                  variant="contained"
                  startIcon={<PlayArrowIcon />}
                  onClick={() => handleAnalyzeProcess(process.process_id)}
                  disabled={process.status === 'analyzing'}
                >
                  {process.status === 'analyzing' ? 'In Analisi...' : 'Analizza Processo'}
                </Button>
              </Box>
            </Card>
          </Grid>
        ))}
      </Grid>

      {/* No Results */}
      {filteredProcesses.length === 0 && !loading && (
        <Box textAlign="center" py={5}>
          <Typography variant="h6" color="text.secondary">
            Nessun processo trovato
          </Typography>
          {searchTerm && (
            <Typography variant="body2" color="text.secondary">
              Prova a rimuovere o modificare il termine di ricerca
            </Typography>
          )}
        </Box>
      )}
    </Box>
  )
}

export default ProcessList