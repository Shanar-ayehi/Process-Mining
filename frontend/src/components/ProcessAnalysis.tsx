import React, { useState, useEffect, useCallback, useMemo } from 'react';
import {
  ReactFlow,
  MiniMap,
  Controls,
  Background,
  Node,
  Edge,
  ConnectionMode,
  MarkerType,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import {
  Box,
  Typography,
  Slider,
  Paper,
  CircularProgress,
  Alert,
  IconButton,
  Tooltip,
  Chip,
} from '@mui/material';
import {
  FilterList as FilterIcon,
  Refresh as RefreshIcon,
} from '@mui/icons-material';
import { useParams } from 'react-router-dom';
import axios from 'axios';

import CustomNode from './CustomNode';
import WhatIfSidebar from './WhatIfSidebar';

interface GraphNode {
  id: string;
  label: string;
  type: 'start' | 'end' | 'normal';
  automation_rules?: Array<{
    workflow_id: string;
    workflow_name: string;
    trigger_type: string;
    actions: Array<{ type: string; delay_days: number }>;
  }>;
}

interface GraphEdge {
  id: string;
  source: string;
  target: string;
  type: string;
  weight: number;
  label?: string;
}

interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

interface NodePerformance {
  [nodeId: string]: number; // average time in days
}

const ProcessAnalysis: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000/api/v1';

  // State
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const [nodePerformance, setNodePerformance] = useState<NodePerformance>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [frequencyFilter, setFrequencyFilter] = useState(0);
  const [maxFrequency, setMaxFrequency] = useState(100);
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);

  // Fetch graph data
  const fetchGraphData = useCallback(async () => {
    if (!id) return;

    try {
      setLoading(true);
      setError(null);

      const response = await axios.get(
        `${API_BASE_URL}/mining/discover/dfg-with-automations/${id}?include_performance=true`
      );

      const data = response.data;
      setGraphData(data.graph_data);

      // Extract node performance from edges
      const performance: NodePerformance = {};
      if (data.graph_data?.edges) {
        data.graph_data.edges.forEach((edge: GraphEdge) => {
          if (edge.type === 'performance' && edge.weight) {
            performance[edge.source] = edge.weight;
          }
        });
      }
      setNodePerformance(performance);

      // Calculate max frequency for filter
      if (data.graph_data?.edges) {
        const frequencies = data.graph_data.edges
          .filter((e: GraphEdge) => e.type === 'frequency')
          .map((e: GraphEdge) => e.weight);
        setMaxFrequency(Math.max(...frequencies, 100));
      }
    } catch (err: any) {
      console.error('Errore nel caricamento del grafo:', err);
      setError(err.response?.data?.detail || 'Errore nel caricamento dei dati');
    } finally {
      setLoading(false);
    }
  }, [id, API_BASE_URL]);

  useEffect(() => {
    fetchGraphData();
  }, [fetchGraphData]);

  // Filter edges by frequency
  const filteredEdges = useMemo(() => {
    if (!graphData?.edges) return [];
    return graphData.edges.filter((edge) => edge.weight >= frequencyFilter);
  }, [graphData?.edges, frequencyFilter]);

  // Convert to React Flow nodes
  const rfNodes: Node[] = useMemo(() => {
    if (!graphData?.nodes) return [];

    return graphData.nodes.map((node) => ({
      id: node.id,
      type: 'custom',
      position: { x: 0, y: 0 }, // Will be auto-layouted
      data: {
        label: node.label,
        type: node.type,
        avgTime: nodePerformance[node.id],
        automationRules: node.automation_rules || [],
        onClick: () => {
          setSelectedNode(node);
          setSidebarOpen(true);
        },
      },
    }));
  }, [graphData?.nodes, nodePerformance]);

  // Convert to React Flow edges
  const rfEdges: Edge[] = useMemo(() => {
    return filteredEdges.map((edge) => ({
      id: edge.id,
      source: edge.source,
      target: edge.target,
      label: edge.label,
      animated: edge.type === 'performance',
      style: {
        strokeWidth: Math.max(1, Math.min(edge.weight / 10, 5)),
        stroke: edge.type === 'performance' ? '#9c27b0' : '#2196f3',
      },
      markerEnd: {
        type: MarkerType.ArrowClosed,
      },
    }));
  }, [filteredEdges]);

  // Node types for React Flow
  const nodeTypes = useMemo(
    () => ({
      custom: CustomNode,
    }),
    []
  );

  // Handle simulation
  const handleSimulate = async (modifications: Record<string, any>) => {
    try {
      const response = await axios.post(`${API_BASE_URL}/analytics/simulate`, {
        portal_id: id,
        num_cases: 100,
        modifications,
        seed: 42,
      });

      console.log('Simulazione avviata:', response.data);
      return response.data;
    } catch (err: any) {
      console.error('Errore simulazione:', err);
      throw new Error(err.response?.data?.detail || 'Errore durante la simulazione');
    }
  };

  // Handle node click on canvas background (close sidebar)
  const handlePaneClick = useCallback(() => {
    setSidebarOpen(false);
    setSelectedNode(null);
  }, []);

  if (loading) {
    return (
      <Box
        display="flex"
        justifyContent="center"
        alignItems="center"
        height="100vh"
        flexDirection="column"
        gap={2}
      >
        <CircularProgress size={60} />
        <Typography variant="h6" color="text.secondary">
          Caricamento grafo processo...
        </Typography>
      </Box>
    );
  }

  if (error) {
    return (
      <Box p={3}>
        <Alert
          severity="error"
          action={
            <IconButton color="inherit" size="small" onClick={fetchGraphData}>
              <RefreshIcon />
            </IconButton>
          }
        >
          {error}
        </Alert>
      </Box>
    );
  }

  return (
    <Box sx={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
      {/* Top Bar */}
      <Paper elevation={2} sx={{ p: 2, borderRadius: 0 }}>
        <Box display="flex" alignItems="center" justifyContent="space-between" flexWrap="wrap" gap={2}>
          <Box>
            <Typography variant="h6">
              Analisi Processo: {id}
            </Typography>
            <Typography variant="caption" color="text.secondary">
              {graphData?.nodes.length || 0} nodi • {filteredEdges.length} archi visibili
            </Typography>
          </Box>

          {/* Frequency Filter */}
          <Box display="flex" alignItems="center" gap={2} minWidth={300}>
            <FilterIcon color="action" />
            <Box flex={1}>
              <Typography variant="caption" color="text.secondary" gutterBottom>
                Filtro Frequenza Minima: {frequencyFilter}
              </Typography>
              <Slider
                value={frequencyFilter}
                onChange={(_, value) => setFrequencyFilter(value as number)}
                min={0}
                max={maxFrequency}
                step={1}
                valueLabelDisplay="auto"
                size="small"
              />
            </Box>
          </Box>

          {/* Stats Chips */}
          <Box display="flex" gap={1}>
            <Chip
              label={`${graphData?.nodes.filter(n => n.type === 'start').length || 0} Start`}
              color="success"
              size="small"
            />
            <Chip
              label={`${graphData?.nodes.filter(n => n.type === 'end').length || 0} End`}
              color="error"
              size="small"
            />
            <Chip
              label={`${graphData?.nodes.filter(n => n.automation_rules && n.automation_rules.length > 0).length || 0} Automazioni`}
              color="secondary"
              size="small"
            />
          </Box>

          <Tooltip title="Aggiorna">
            <IconButton onClick={fetchGraphData}>
              <RefreshIcon />
            </IconButton>
          </Tooltip>
        </Box>
      </Paper>

      {/* React Flow Canvas */}
      <Box flex={1} position="relative">
        <ReactFlow
          nodes={rfNodes}
          edges={rfEdges}
          nodeTypes={nodeTypes}
          connectionMode={ConnectionMode.Loose}
          fitView
          fitViewOptions={{ padding: 0.2 }}
          onPaneClick={handlePaneClick}
          minZoom={0.1}
          maxZoom={2}
          defaultViewport={{ x: 0, y: 0, zoom: 0.8 }}
        >
          <MiniMap
            nodeStrokeWidth={3}
            zoomable
            pannable
            style={{
              height: 120,
              width: 150,
            }}
          />
          <Controls showInteractive={false} />
          <Background color="#f0f0f0" gap={16} />

          {/* Empty state */}
          {rfNodes.length === 0 && (
            <Box
              position="absolute"
              top="50%"
              left="50%"
              sx={{ transform: 'translate(-50%, -50%)', textAlign: 'center' }}
            >
              <Typography variant="h6" color="text.secondary">
                Nessun dato disponibile
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Verifica che il processo abbia dati sufficienti per generare il grafo
              </Typography>
            </Box>
          )}
        </ReactFlow>
      </Box>

      {/* What-If Sidebar */}
      <WhatIfSidebar
        open={sidebarOpen}
        node={selectedNode}
        onClose={() => {
          setSidebarOpen(false);
          setSelectedNode(null);
        }}
        onSimulate={handleSimulate}
      />
    </Box>
  );
};

export default ProcessAnalysis;