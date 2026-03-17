import React from 'react';
import { Box, Typography, Button, Paper, Grid } from '@mui/material';
import { ArrowBack as ArrowBackIcon } from '@mui/icons-material';
import { useParams, useNavigate } from 'react-router-dom';

const ProcessDetail: React.FC = () => {
  const { processId } = useParams();
  const navigate = useNavigate();

  const handleBack = () => {
    navigate('/');
  };

  return (
    <Box sx={{ p: 3 }}>
      <Button
        startIcon={<ArrowBackIcon />}
        onClick={handleBack}
        sx={{ mb: 2 }}
      >
        Torna ai Processi
      </Button>
      
      <Paper elevation={3} sx={{ p: 3, mb: 3 }}>
        <Typography variant="h4" gutterBottom>
          Dettagli Processo: {processId}
        </Typography>
        <Typography variant="body1" color="text.secondary">
          Questa pagina mostrerà i dettagli specifici del processo selezionato.
        </Typography>
      </Paper>

      <Box sx={{ display: 'flex', gap: 3, flexDirection: { xs: 'column', md: 'row' } }}>
        <Paper elevation={2} sx={{ p: 2, flex: 1, height: '200px' }}>
          <Typography variant="h6">Metriche Processo</Typography>
          <Typography variant="body2" color="text.secondary">
            Informazioni sulle performance del processo
          </Typography>
        </Paper>
        <Paper elevation={2} sx={{ p: 2, flex: 1, height: '200px' }}>
          <Typography variant="h6">Analisi Varianti</Typography>
          <Typography variant="body2" color="text.secondary">
            Distribuzione delle diverse varianti del processo
          </Typography>
        </Paper>
      </Box>
    </Box>
  );
};

export default ProcessDetail;