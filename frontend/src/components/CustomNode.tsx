import React, { memo } from 'react';
import { Handle, Position, NodeProps } from '@xyflow/react';
import { Box, Typography, Chip, Paper } from '@mui/material';
import { Bolt as BoltIcon } from '@mui/icons-material';

interface CustomNodeData {
  label: string;
  type: 'start' | 'end' | 'normal';
  avgTime?: number;
  is_automated?: boolean;
  automationRules?: Array<{
    workflow_id: string;
    workflow_name: string;
    trigger_type: string;
    actions: Array<{ type: string; delay_days: number }>;
  }>;
  isIsolated?: boolean;
  onClick?: () => void;
}

const CustomNode: React.FC<NodeProps> = ({ data, selected }) => {
  const nodeData = data as unknown as CustomNodeData;
  const hasAutomations = nodeData.automationRules && nodeData.automationRules.length > 0;
  const isAutomated = nodeData.is_automated || hasAutomations;

  const formatDuration = (seconds: number) => {
    if (!seconds) return "0 sec";
    if (seconds >= 86400) return `${(seconds / 86400).toFixed(1)} giorni`;
    if (seconds >= 3600) return `${(seconds / 3600).toFixed(1)} ore`;
    if (seconds >= 60) return `${(seconds / 60).toFixed(1)} min`;
    return `${seconds.toFixed(1)} sec`;
  };

  const getNodeColor = () => {
    if (isAutomated) return '#9c27b0'; // Priorità massima: Purple per automazioni
    switch (nodeData.type) {
      case 'start': return '#4caf50';
      case 'end': return '#f44336';
      default: return '#2196f3';
    }
  };

  const getNodeBorder = () => {
    if (selected) return '3px solid #ff9800';
    if (isAutomated) return '2px solid #9c27b0';
    return '1px solid #e0e0e0';
  };

  return (
    <>
      <Handle type="target" position={Position.Top} style={{ background: '#555' }} />
      
      <Paper
        elevation={selected ? 8 : 2}
        onClick={nodeData.onClick}
        sx={{
          padding: '12px 16px',
          minWidth: 150,
          maxWidth: 250,
          borderRadius: 2,
          border: getNodeBorder(),
          backgroundColor: 'white',
          cursor: 'pointer',
          transition: 'all 0.2s ease',
          '&:hover': {
            elevation: 6,
            transform: 'scale(1.02)',
          },
        }}
      >
        {/* Badge Automazione */}
        {hasAutomations && (
          <Box
            sx={{
              position: 'absolute',
              top: -8,
              right: -8,
              backgroundColor: '#9c27b0',
              borderRadius: '50%',
              width: 24,
              height: 24,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <BoltIcon sx={{ fontSize: 16, color: 'white' }} />
          </Box>
        )}

        {/* Header con tipo nodo */}
        <Box display="flex" alignItems="center" gap={1} mb={1}>
          <Box
            sx={{
              width: 12,
              height: 12,
              borderRadius: '50%',
              backgroundColor: getNodeColor(),
            }}
          />
          <Chip
            label={isAutomated ? 'AUTOMATION' : nodeData.type.toUpperCase()}
            size="small"
            sx={{
              height: 18,
              fontSize: '0.65rem',
              backgroundColor: getNodeColor(),
              color: 'white',
            }}
          />
        </Box>

        {/* Nome Fase */}
        <Typography variant="subtitle2" fontWeight="bold" noWrap>
          {nodeData.label}
        </Typography>

        {/* Tempo Medio */}
        {nodeData.avgTime !== undefined && (
          <Typography variant="caption" color="text.secondary">
            ⏱ {formatDuration(nodeData.avgTime)} (media)
          </Typography>
        )}

        {/* Contatore Automazioni */}
        {hasAutomations && (
          <Typography variant="caption" color="secondary" display="block" mt={0.5}>
            ⚡ {nodeData.automationRules!.length} automazion{nodeData.automationRules!.length > 1 ? 'i' : 'e'}
          </Typography>
        )}
      </Paper>

      <Handle type="source" position={Position.Bottom} style={{ background: '#555' }} />
    </>
  );
};

export default memo(CustomNode);