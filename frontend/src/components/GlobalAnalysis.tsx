import React, { useState, useEffect } from 'react'
import { 
  Box, 
  Grid, 
  Card, 
  CardContent, 
  Typography, 
  LinearProgress,
  Alert
} from '@mui/material'
import { 
  TrendingUp as TrendingUpIcon,
  Assessment as AssessmentIcon,
  Timeline as TimelineIcon,
  Speed as SpeedIcon
} from '@mui/icons-material'
import axios from 'axios'

interface GlobalStats {
  total_processes: number
  active_processes: number
  total_cases: number
  total_variants: number
  avg_processing_time: number
  overall_quality_score: number
  top_bottlenecks: Array<{
    activity: string
    avg_duration: number
    frequency: number
  }>
  trend_analysis: {
    cases_trend: string
    efficiency_trend: string
  }
}

const GlobalAnalysis: React.FC = () => {
  const [stats, setStats] = useState<GlobalStats | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000/api/v1'

  useEffect(() => {
    fetchGlobalStats()
  }, [])

  const fetchGlobalStats = async () => {
    try {
      setLoading(true)
      setError(null)
      
      const response = await axios.get(`${API_BASE_URL}/analytics/global`)
      setStats(response.data)
    } catch (err: any) {
      console.error('Errore nel recupero statistiche globali:', err)
      setError(err.message || 'Errore nel recupero statistiche globali')
    } finally {
      setLoading(false)
    }
  }

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
        <Box textAlign="center">
          <LinearProgress sx={{ mb: 2 }} />
          <Typography>Caricamento analisi globale...</Typography>
        </Box>
      </Box>
    )
  }

  if (error) {
    return (
      <Box>
        <Alert severity="error">{error}</Alert>
      </Box>
    )
  }

  if (!stats) {
    return (
      <Box>
        <Alert severity="info">Nessun dato disponibile per l'analisi globale</Alert>
      </Box>
    )
  }

  return (
    <Box>
      {/* Header */}
      <Box mb={3}>
        <Typography variant="h4" component="h1" gutterBottom>
          Analisi Globale dei Processi
        </Typography>
        <Typography variant="subtitle1" color="text.secondary">
          Panoramica delle performance di tutti i processi aziendali
        </Typography>
      </Box>

      {/* Global Stats Cards */}
      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box display="flex" alignItems="center" justifyContent="space-between">
                <Box>
                  <Typography color="text.secondary">Processi Totali</Typography>
                  <Typography variant="h4">{stats.total_processes}</Typography>
                </Box>
                <AssessmentIcon color="primary" sx={{ fontSize: 40 }} />
              </Box>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box display="flex" alignItems="center" justifyContent="space-between">
                <Box>
                  <Typography color="text.secondary">Processi Attivi</Typography>
                  <Typography variant="h4">{stats.active_processes}</Typography>
                </Box>
                <TrendingUpIcon color="success" sx={{ fontSize: 40 }} />
              </Box>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box display="flex" alignItems="center" justifyContent="space-between">
                <Box>
                  <Typography color="text.secondary">Casi Totali</Typography>
                  <Typography variant="h4">{stats.total_cases}</Typography>
                </Box>
                <TimelineIcon color="info" sx={{ fontSize: 40 }} />
              </Box>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Box display="flex" alignItems="center" justifyContent="space-between">
                <Box>
                  <Typography color="text.secondary">Tempo Medio</Typography>
                  <Typography variant="h4">{stats.avg_processing_time?.toFixed(1) ?? 'N/A'} giorni</Typography>
                </Box>
                <SpeedIcon color="warning" sx={{ fontSize: 40 }} />
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Quality Score */}
      <Card sx={{ mb: 4 }}>
        <CardContent>
          <Typography variant="h6" gutterBottom>
            Qualità Complessiva dei Dati
          </Typography>
          <Box display="flex" alignItems="center" gap={2}>
            <Box flexGrow={1}>
              <LinearProgress 
                variant="determinate" 
                value={stats.overall_quality_score * 100} 
                color={stats.overall_quality_score > 0.8 ? "success" : "warning"}
                sx={{ height: 10, borderRadius: 5 }}
              />
            </Box>
            <Typography variant="h6">
              {Math.round(stats.overall_quality_score * 100)}%
            </Typography>
          </Box>
        </CardContent>
      </Card>

      {/* Top Bottlenecks */}
      {stats.top_bottlenecks && stats.top_bottlenecks.length > 0 && (
        <Card sx={{ mb: 4 }}>
          <CardContent>
            <Typography variant="h6" gutterBottom>
              Principali Colli di Bottiglia
            </Typography>
            {stats.top_bottlenecks.map((bottleneck, index) => (
              <Box key={index} sx={{ mb: 2 }}>
                <Box display="flex" justifyContent="space-between" alignItems="center" mb={1}>
                  <Typography variant="body1">{bottleneck.activity}</Typography>
                  <Typography variant="body2" color="text.secondary">
                    {bottleneck.avg_duration.toFixed(1)} giorni ({bottleneck.frequency} occorrenze)
                  </Typography>
                </Box>
                <LinearProgress 
                  variant="determinate" 
                  value={Math.min((bottleneck.avg_duration / 30) * 100, 100)} 
                  color="error"
                />
              </Box>
            ))}
          </CardContent>
        </Card>
      )}

      {/* Trend Analysis */}
      <Card>
        <CardContent>
          <Typography variant="h6" gutterBottom>
            Analisi dei Trend
          </Typography>
          <Grid container spacing={2}>
            <Grid item xs={12} sm={6}>
              <Box>
                <Typography color="text.secondary" gutterBottom>
                  Trend Casi
                </Typography>
                <Typography variant="body1">
                  {stats.trend_analysis?.cases_trend ?? 'N/A'}
                </Typography>
              </Box>
            </Grid>
            <Grid item xs={12} sm={6}>
              <Box>
                <Typography color="text.secondary" gutterBottom>
                  Trend Efficienza
                </Typography>
                <Typography variant="body1">
                  {stats.trend_analysis?.efficiency_trend ?? 'N/A'}
                </Typography>
              </Box>
            </Grid>
          </Grid>
        </CardContent>
      </Card>
    </Box>
  )
}

export default GlobalAnalysis