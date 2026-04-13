import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Grid,
  List,
  ListItem,
  ListItemText,
  Chip,
  CircularProgress,
  Alert,
  Divider
} from '@mui/material';
import axios from 'axios';
import { useParams } from 'react-router-dom';

const ProcessAnalyticsDashboard: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000/api/v1';

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [analyticsData, setAnalyticsData] = useState<any>(null);

  const fetchAnalyticsData = useCallback(async () => {
    if (!id) return;

    try {
      setLoading(true);
      setError(null);

      const response = await axios.get(`${API_BASE_URL}/analytics/features/${id}`);
      setAnalyticsData(response.data);

    } catch (err: any) {
      console.error('Errore caricamento dati analytics:', err);
      setError(err.response?.data?.detail || 'Errore nel caricamento delle statistiche');
    } finally {
      setLoading(false);
    }
  }, [id, API_BASE_URL]);

  useEffect(() => {
    fetchAnalyticsData();
  }, [fetchAnalyticsData]);

  if (loading) {
    return (
      <Box
        display="flex"
        justifyContent="center"
        alignItems="center"
        height="100%"
        flexDirection="column"
        gap={2}
      >
        <CircularProgress size={60} />
        <Typography variant="h6" color="text.secondary">
          Calcolo statistiche processo...
        </Typography>
      </Box>
    );
  }

  if (error) {
    return (
      <Box p={3}>
        <Alert severity="error">{error}</Alert>
      </Box>
    );
  }

  if (!analyticsData) {
    return null;
  }

  return (
    <Box p={3} sx={{ height: '100%', overflow: 'auto' }}>
      <Typography variant="h5" gutterBottom>
        Dashboard Analytics Processo
      </Typography>

      <Grid container spacing={3} sx={{ mb: 4 }}>
        {/* KPI Principali */}
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent sx={{ textAlign: 'center', py: 3 }}>
              <Typography variant="caption" color="text.secondary">
                Durata Media Caso
              </Typography>
              <Typography variant="h4">
                {analyticsData.avg_case_duration_seconds ? (analyticsData.avg_case_duration_seconds / 86400).toFixed(1) : '-'}
              </Typography>
              <Typography variant="caption">giorni</Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent sx={{ textAlign: 'center', py: 3 }}>
              <Typography variant="caption" color="text.secondary">
                Tempo Medio Tra Attività
              </Typography>
              <Typography variant="h4">
                {analyticsData.avg_time_between_activities ? (analyticsData.avg_time_between_activities / 3600).toFixed(1) : '-'}
              </Typography>
              <Typography variant="caption">ore</Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent sx={{ textAlign: 'center', py: 3 }}>
              <Typography variant="caption" color="text.secondary">
                Tasso di Conversione
              </Typography>
              <Typography variant="h4">
                {analyticsData.patterns?.rework_rate ? (analyticsData.patterns.rework_rate * 100).toFixed(1) : '-'}
              </Typography>
              <Typography variant="caption">% Rework</Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent sx={{ textAlign: 'center', py: 3 }}>
              <Typography variant="caption" color="text.secondary">
                Numero Varianti
              </Typography>
              <Typography variant="h4">
                {analyticsData.process_variants?.total_variants || '-'}
              </Typography>
              <Typography variant="caption">percorsi unici</Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      <Grid container spacing={3}>
        {/* Attività più frequenti */}
        <Grid item xs={12} md={6}>
          <Card sx={{ height: '100%' }}>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                🔝 Attività più frequenti
              </Typography>
              <List dense>
                {analyticsData.top_activities && analyticsData.top_activities_frequency && 
                  analyticsData.top_activities.map((activity: string, index: number) => (
                    <ListItem key={index} divider>
                      <ListItemText
                        primary={activity}
                        secondary={`${analyticsData.top_activities_frequency[index]} occorrenze`}
                      />
                      <Chip
                        label={`#${index + 1}`}
                        size="small"
                        color={index === 0 ? 'primary' : 'default'}
                      />
                    </ListItem>
                  ))
                }
              </List>
            </CardContent>
          </Card>
        </Grid>

        {/* Colli di Bottiglia */}
        <Grid item xs={12} md={6}>
          <Card sx={{ height: '100%' }}>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                🚩 Colli di Bottiglia (Attività più lente)
              </Typography>
              <List dense>
                {analyticsData.activity_durations && 
                  analyticsData.activity_durations.slice(0, 5).map((item: any, index: number) => (
                    <ListItem key={index} divider>
                      <ListItemText
                        primary={item.activity}
                        secondary={`Tempo medio: ${(item.avg_duration / 86400).toFixed(1)} giorni`}
                      />
                      <Chip
                        label="Slow"
                        size="small"
                        color="error"
                      />
                    </ListItem>
                  ))
                }
              </List>
            </CardContent>
          </Card>
        </Grid>

        {/* Social Network Risorse */}
        <Grid item xs={12} md={6}>
          <Card sx={{ height: '100%' }}>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                👥 Risorse più attive
              </Typography>
              <List dense>
                {analyticsData.social_network?.most_connected_resources && 
                  analyticsData.social_network.most_connected_resources.slice(0, 8).map(([pair, score]: any, index: number) => (
                    <ListItem key={index} divider>
                      <ListItemText
                        primary={pair}
                        secondary={`Similarità: ${(score * 100).toFixed(0)}%`}
                      />
                    </ListItem>
                  ))
                }
              </List>
            </CardContent>
          </Card>
        </Grid>

        {/* Rework Patterns */}
        <Grid item xs={12} md={6}>
          <Card sx={{ height: '100%' }}>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                🔄 Pattern di Rework
              </Typography>
              <List dense>
                {analyticsData.patterns && (
                  <>
                    <ListItem divider>
                      <ListItemText
                        primary="Casi con Rework"
                        secondary={`${analyticsData.patterns.rework_cases} casi`}
                      />
                      <Chip
                        label={`${(analyticsData.patterns.rework_rate * 100).toFixed(1)}%`}
                        size="small"
                        color="warning"
                      />
                    </ListItem>
                  </>
                )}
              </List>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      <Box mt={4}>
        <Divider sx={{ mb: 2 }} />
        <Typography variant="caption" color="text.secondary">
          Ultimo aggiornamento: {new Date(analyticsData.timestamp).toLocaleString('it-IT')}
        </Typography>
      </Box>
    </Box>
  );
};

export default ProcessAnalyticsDashboard;