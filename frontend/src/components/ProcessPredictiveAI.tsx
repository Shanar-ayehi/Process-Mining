import React, { useState } from 'react';
import {
  Box,
  Typography,
  Button,
  Card,
  CardContent,
  Grid,
  LinearProgress,
  Chip,
  Alert,
  List,
  ListItem,
  ListItemText,
  CircularProgress
} from '@mui/material';
import axios from 'axios';
import { useParams } from 'react-router-dom';

const ProcessPredictiveAI: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000/api/v1';

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [modelResult, setModelResult] = useState<any>(null);

  const trainModel = async () => {
    if (!id) return;

    try {
      setLoading(true);
      setError(null);
      setModelResult(null);

      const response = await axios.post(`${API_BASE_URL}/analytics/predictive/train/${id}`);
      setModelResult(response.data);

    } catch (err: any) {
      console.error('Errore training modello:', err);
      if (err.response?.status === 400) {
        setError("Servono più dati storici per addestrare l'IA con precisione. Sono necessari almeno 10 casi completati.");
      } else {
        setError(err.response?.data?.detail || 'Errore durante l\'addestramento del modello');
      }
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <Box
        display="flex"
        justifyContent="center"
        alignItems="center"
        height="100%"
        flexDirection="column"
        gap={3}
        p={5}
      >
        <CircularProgress size={80} />
        <Typography variant="h6" color="text.secondary">
          🤖 Addestramento Intelligenza Artificiale in corso...
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Analisi pattern storici | Calcolo feature | Training Random Forest
        </Typography>
        <Box width={300}>
          <LinearProgress />
        </Box>
      </Box>
    );
  }

  if (error) {
    return (
      <Box p={5} display="flex" flexDirection="column" gap={3} alignItems="center">
        <Alert severity="warning" sx={{ maxWidth: 500 }}>
          {error}
        </Alert>
        <Button
          variant="contained"
          onClick={trainModel}
          disabled={loading}
        >
          🔄 Riprova
        </Button>
      </Box>
    );
  }

  if (modelResult) {
    return (
      <Box p={3} sx={{ height: '100%', overflow: 'auto' }}>
        <Grid container spacing={3} sx={{ mb: 4 }}>
          <Grid item xs={12} md={6}>
            <Card>
              <CardContent>
                <Typography variant="caption" color="text.secondary">
                  Accuratezza Modello
                </Typography>
                <Typography variant="h2" color="primary" sx={{ my: 2 }}>
                  {modelResult.evaluation?.accuracy ? Math.round(modelResult.evaluation.accuracy * 100) : '-'}%
                </Typography>
                <Chip
                  label="Random Forest Classifier"
                  size="small"
                  color="success"
                />
              </CardContent>
            </Card>
          </Grid>

          <Grid item xs={12} md={6}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Performance Modello
                </Typography>
                <List dense>
                  {modelResult.evaluation && Object.entries(modelResult.evaluation).map(([metric, value]: any, index: number) => {
                    // Salta feature_importance: è un array e viene renderizzato separatamente dopo
                    if (metric === 'feature_importance' || Array.isArray(value)) return null;
                    
                    return (
                      <ListItem key={index} divider>
                        <ListItemText
                          primary={metric.replace('_', ' ').toUpperCase()}
                          secondary={typeof value === 'number' ? value.toFixed(3) : String(value)}
                        />
                      </ListItem>
                    )
                  })}
                </List>
              </CardContent>
            </Card>
          </Grid>
        </Grid>

        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                🎯 Fattori di Successo (Feature Importance)
              </Typography>
              <Typography variant="body2" color="text.secondary" paragraph>
                Queste sono le variabili che influenzano di più la probabilità di successo del deal
              </Typography>

              <List>
                {modelResult.evaluation?.feature_importance && modelResult.evaluation.feature_importance.map((item: any, index: number) => (
                  <ListItem key={index} divider>
                    <ListItemText
                      primary={item.feature.replace('_', ' ')}
                      secondary={
                        <Box sx={{ mt: 1 }}>
                          <LinearProgress
                            variant="determinate"
                            value={item.importance * 100}
                            sx={{ height: 8, borderRadius: 4 }}
                          />
                        </Box>
                      }
                    />
                    <Chip
                      label={`${Math.round(item.importance * 100)}%`}
                      size="small"
                      color={index < 2 ? 'primary' : 'default'}
                    />
                  </ListItem>
                ))}
              </List>
            </CardContent>
          </Card>
        </Grid>

        <Box mt={4} display="flex" justifyContent="center">
          <Button
            variant="outlined"
            onClick={trainModel}
            disabled={loading}
          >
            🔄 Riavvio Addestramento
          </Button>
        </Box>
      </Box>
    );
  }

  return (
    <Box
      p={5}
      display="flex"
      flexDirection="column"
      alignItems="center"
      justifyContent="center"
      height="100%"
      gap={4}
    >
      <Typography variant="h4" align="center">
        🤖 Previsioni Intelligenza Artificiale
      </Typography>

      <Typography variant="body1" color="text.secondary" align="center" sx={{ maxWidth: 600 }}>
        Addestra un modello di Machine Learning su tutti i dati storici del processo.
        L'AI analizzerà automaticamente i pattern e identificherà quali fattori influenzano di più il successo del deal.
      </Typography>

      <Button
        variant="contained"
        size="large"
        onClick={trainModel}
        disabled={loading}
        sx={{ py: 2, px: 6, fontSize: '1.1rem' }}
      >
        Avvia Addestramento Modello
      </Button>

      <Box mt={2}>
        <Chip label="Random Forest" size="small" sx={{ mx: 0.5 }} />
        <Chip label="Feature Engineering" size="small" sx={{ mx: 0.5 }} />
        <Chip label="Cross Validation 5-Fold" size="small" sx={{ mx: 0.5 }} />
      </Box>
    </Box>
  );
};

export default ProcessPredictiveAI;