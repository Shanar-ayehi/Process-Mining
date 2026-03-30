import React, { useState, useEffect } from 'react';
import {
  Box,
  Drawer,
  Typography,
  IconButton,
  Divider,
  Slider,
  Switch,
  Button,
  CircularProgress,
  LinearProgress,
  Alert,
  Chip,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
} from '@mui/material';
import {
  Close as CloseIcon,
  Bolt as BoltIcon,
  Schedule as ScheduleIcon,
  PlayArrow as PlayArrowIcon,
  CheckCircle as CheckCircleIcon,
} from '@mui/icons-material';

interface AutomationRule {
  workflow_id: string;
  workflow_name: string;
  trigger_type: string;
  trigger_property?: string;
  trigger_value?: string;
  actions: Array<{
    type: string;
    delay_days: number;
    email_id?: string;
    property?: string;
    value?: string;
  }>;
}

interface NodeInfo {
  id: string;
  label: string;
  type: 'start' | 'end' | 'normal';
  avgTime?: number;
  automationRules?: AutomationRule[];
}

interface WhatIfSidebarProps {
  open: boolean;
  node: NodeInfo | null;
  onClose: () => void;
  onSimulate: (modifications: Record<string, any>) => Promise<void>;
}

const WhatIfSidebar: React.FC<WhatIfSidebarProps> = ({
  open,
  node,
  onClose,
  onSimulate,
}) => {
  const [timeMultiplier, setTimeMultiplier] = useState(1.0);
  const [disabledAutomations, setDisabledAutomations] = useState<Set<string>>(new Set());
  const [overrideDelay, setOverrideDelay] = useState<number | null>(null);
  const [simulating, setSimulating] = useState(false);
  const [simulationProgress, setSimulationProgress] = useState(0);
  const [simulationResult, setSimulationResult] = useState<any>(null);

  // Reset state when node changes
  useEffect(() => {
    if (node) {
      setTimeMultiplier(1.0);
      setDisabledAutomations(new Set());
      setOverrideDelay(null);
      setSimulationResult(null);
    }
  }, [node?.id]);

  const handleToggleAutomation = (workflowId: string) => {
    setDisabledAutomations((prev) => {
      const newSet = new Set(prev);
      if (newSet.has(workflowId)) {
        newSet.delete(workflowId);
      } else {
        newSet.add(workflowId);
      }
      return newSet;
    });
  };

  const handleSimulate = async () => {
    if (!node) return;

    setSimulating(true);
    setSimulationProgress(0);

    // Simulate progress
    const progressInterval = setInterval(() => {
      setSimulationProgress((prev) => {
        if (prev >= 90) {
          clearInterval(progressInterval);
          return 90;
        }
        return prev + 10;
      });
    }, 200);

    try {
      const modifications: Record<string, any> = {
        [node.label]: {
          time_multiplier: timeMultiplier,
        },
      };

      if (disabledAutomations.size > 0) {
        modifications[node.label].disable_automation = true;
      }

      if (overrideDelay !== null) {
        modifications[node.label].override_automation_delay = overrideDelay;
      }

      await onSimulate(modifications);

      setSimulationProgress(100);
      setSimulationResult({
        success: true,
        message: 'Simulazione completata con successo!',
      });
    } catch (error) {
      setSimulationResult({
        success: false,
        message: 'Errore durante la simulazione',
      });
    } finally {
      clearInterval(progressInterval);
      setSimulating(false);
    }
  };

  if (!node) return null;

  const hasAutomations = node.automationRules && node.automationRules.length > 0;

  return (
    <Drawer
      anchor="right"
      open={open}
      onClose={onClose}
      PaperProps={{
        sx: { width: 400, maxWidth: '90vw' },
      }}
    >
      <Box sx={{ p: 3 }}>
        {/* Header */}
        <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
          <Typography variant="h6">What-If Analysis</Typography>
          <IconButton onClick={onClose} size="small">
            <CloseIcon />
          </IconButton>
        </Box>

        <Divider sx={{ mb: 3 }} />

        {/* Node Info */}
        <Box mb={3}>
          <Typography variant="subtitle2" color="text.secondary" gutterBottom>
            Informazioni Nodo
          </Typography>
          <Box display="flex" alignItems="center" gap={1} mb={1}>
            <Chip
              label={node.type.toUpperCase()}
              size="small"
              color={
                node.type === 'start'
                  ? 'success'
                  : node.type === 'end'
                  ? 'error'
                  : 'primary'
              }
            />
            <Typography variant="body1" fontWeight="bold">
              {node.label}
            </Typography>
          </Box>
          {node.avgTime !== undefined && (
            <Box display="flex" alignItems="center" gap={1}>
              <ScheduleIcon fontSize="small" color="action" />
              <Typography variant="body2" color="text.secondary">
                Tempo medio: {node.avgTime.toFixed(1)} giorni
              </Typography>
            </Box>
          )}
        </Box>

        <Divider sx={{ mb: 3 }} />

        {/* Automazioni */}
        {hasAutomations && (
          <Box mb={3}>
            <Typography variant="subtitle2" color="text.secondary" gutterBottom>
              <BoltIcon sx={{ fontSize: 16, mr: 0.5, verticalAlign: 'middle' }} />
              Automazioni HubSpot
            </Typography>
            <List dense>
              {node.automationRules!.map((rule) => (
                <ListItem
                  key={rule.workflow_id}
                  sx={{
                    bgcolor: disabledAutomations.has(rule.workflow_id)
                      ? 'action.disabledBackground'
                      : 'background.paper',
                    borderRadius: 1,
                    mb: 1,
                    border: '1px solid',
                    borderColor: 'divider',
                  }}
                >
                  <ListItemIcon>
                    <Switch
                      edge="start"
                      checked={!disabledAutomations.has(rule.workflow_id)}
                      onChange={() => handleToggleAutomation(rule.workflow_id)}
                      size="small"
                    />
                  </ListItemIcon>
                  <ListItemText
                    primary={rule.workflow_name}
                    secondary={
                      <Box>
                        <Typography variant="caption" display="block">
                          Trigger: {rule.trigger_type}
                        </Typography>
                        {rule.actions.map((action, idx) => (
                          <Typography key={idx} variant="caption" display="block">
                            • {action.type} (dopo {action.delay_days.toFixed(1)} giorni)
                          </Typography>
                        ))}
                      </Box>
                    }
                  />
                </ListItem>
              ))}
            </List>
          </Box>
        )}

        <Divider sx={{ mb: 3 }} />

        {/* Controlli What-If */}
        <Box mb={3}>
          <Typography variant="subtitle2" color="text.secondary" gutterBottom>
            Controlli What-If
          </Typography>

          {/* Time Multiplier Slider */}
          <Box mb={3}>
            <Typography variant="body2" gutterBottom>
              Moltiplicatore Tempo: {(timeMultiplier * 100).toFixed(0)}%
            </Typography>
            <Slider
              value={timeMultiplier * 100}
              onChange={(_, value) => setTimeMultiplier((value as number) / 100)}
              min={10}
              max={200}
              step={5}
              marks={[
                { value: 50, label: '50%' },
                { value: 100, label: '100%' },
                { value: 150, label: '150%' },
              ]}
              valueLabelDisplay="auto"
              valueLabelFormat={(value) => `${value}%`}
            />
            <Typography variant="caption" color="text.secondary">
              {timeMultiplier < 1
                ? `Riduzione del ${((1 - timeMultiplier) * 100).toFixed(0)}%`
                : timeMultiplier > 1
                ? `Aumento del ${((timeMultiplier - 1) * 100).toFixed(0)}%`
                : 'Nessuna modifica'}
            </Typography>
          </Box>

          {/* Override Automation Delay */}
          {hasAutomations && (
            <Box mb={3}>
              <Typography variant="body2" gutterBottom>
                Override Delay Automazioni (giorni)
              </Typography>
              <Slider
                value={overrideDelay ?? 0}
                onChange={(_, value) => setOverrideDelay(value as number)}
                min={0}
                max={30}
                step={0.5}
                marks={[
                  { value: 0, label: '0' },
                  { value: 7, label: '7' },
                  { value: 14, label: '14' },
                  { value: 30, label: '30' },
                ]}
                valueLabelDisplay="auto"
                disabled={disabledAutomations.size === node.automationRules!.length}
              />
              <Typography variant="caption" color="text.secondary">
                {overrideDelay === null
                  ? 'Nessun override (usa delay originale)'
                  : `Delay impostato a ${overrideDelay} giorni`}
              </Typography>
            </Box>
          )}
        </Box>

        <Divider sx={{ mb: 3 }} />

        {/* Simulazione */}
        <Box mb={3}>
          <Button
            variant="contained"
            fullWidth
            size="large"
            startIcon={
              simulating ? <CircularProgress size={20} color="inherit" /> : <PlayArrowIcon />
            }
            onClick={handleSimulate}
            disabled={simulating}
          >
            {simulating ? 'Simulazione in corso...' : 'Simula Scenario'}
          </Button>

          {/* Progress Bar */}
          {simulating && (
            <Box mt={2}>
              <LinearProgress variant="determinate" value={simulationProgress} />
              <Typography variant="caption" color="text.secondary" align="center" display="block" mt={1}>
                {simulationProgress}% completato
              </Typography>
            </Box>
          )}

          {/* Result */}
          {simulationResult && (
            <Alert
              severity={simulationResult.success ? 'success' : 'error'}
              icon={simulationResult.success ? <CheckCircleIcon /> : undefined}
              sx={{ mt: 2 }}
            >
              {simulationResult.message}
            </Alert>
          )}
        </Box>

        {/* Riepilogo Modifiche */}
        <Box
          sx={{
            bgcolor: 'grey.100',
            borderRadius: 1,
            p: 2,
          }}
        >
          <Typography variant="subtitle2" gutterBottom>
            Riepilogo Modifiche
          </Typography>
          <Typography variant="body2" color="text.secondary">
            • Tempo attività: {(timeMultiplier * 100).toFixed(0)}%
          </Typography>
          {hasAutomations && (
            <>
              <Typography variant="body2" color="text.secondary">
                • Automazioni disabilitate: {disabledAutomations.size}/
                {node.automationRules!.length}
              </Typography>
              {overrideDelay !== null && (
                <Typography variant="body2" color="text.secondary">
                  • Override delay: {overrideDelay} giorni
                </Typography>
              )}
            </>
          )}
        </Box>
      </Box>
    </Drawer>
  );
};

export default WhatIfSidebar;