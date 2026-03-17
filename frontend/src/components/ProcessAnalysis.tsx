import React from 'react';
import { Box, Typography, Paper, Grid, Chip } from '@mui/material';
import { useParams } from 'react-router-dom';

const ProcessAnalysis: React.FC = () => {
  const { processId } = useParams();

  return (
    <Box sx={{ p: 3 }}>
      <Paper elevation={3} sx={{ p: 3, mb: 3 }}>
        <Typography variant="h4" gutterBottom>
          Analisi Processo: {processId}
        </Typography>
        <Typography variant="body1" color="text.secondary">
          Questa pagina mostrerà l'analisi approfondita del processo selezionato.
        </Typography>
      </Paper>

      <Box sx={{ display: 'flex', gap: 3, flexDirection: { xs: 'column', md: 'row' } }}>
        <Paper elevation={2} sx={{ p: 2, flex: 2, height: '300px' }}>
          <Typography variant="h6" gutterBottom>
            Mappa Processo
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Visualizzazione grafica del flusso del processo
          </Typography>
        </Paper>
        <Paper elevation={2} sx={{ p: 2, flex: 1, height: '300px' }}>
          <Typography variant="h6" gutterBottom>
            KPI Principali
          </Typography>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
            <Chip label="Tempo medio: 5.2 giorni" color="primary" size="small" />
            <Chip label="Varianti: 15" color="secondary" size="small" />
            <Chip label="Conformità: 94%" color="success" size="small" />
          </Box>
        </Paper>
      </Box>

      <Box sx={{ display: 'flex', gap: 3, flexDirection: { xs: 'column', md: 'row' }, mt: 3 }}>
        <Paper elevation={2} sx={{ p: 2, flex: 1, height: '200px' }}>
          <Typography variant="h6">Analisi Bottleneck</Typography>
          <Typography variant="body2" color="text.secondary">
            Identificazione dei colli di bottiglia nel processo
          </Typography>
        </Paper>
        <Paper elevation={2} sx={{ p: 2, flex: 1, height: '200px' }}>
          <Typography variant="h6">Raccomandazioni</Typography>
          <Typography variant="body2" color="text.secondary">
            Suggerimenti per l'ottimizzazione del processo
          </Typography>
        </Paper>
      </Box>
    </Box>
  );
};

export default ProcessAnalysis;