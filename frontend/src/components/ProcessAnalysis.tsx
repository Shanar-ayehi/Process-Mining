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
  useNodesState,
  useEdgesState,
  Position,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import dagre from 'dagre';
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
  Button,
} from '@mui/material';
import {
  FilterList as FilterIcon,
  Refresh as RefreshIcon,
  List as ListIcon,
} from '@mui/icons-material';
import { useParams } from 'react-router-dom';
import axios from 'axios';

import CustomNode from './CustomNode';
import CustomEdge from './CustomEdge';
import WhatIfSidebar from './WhatIfSidebar';
import Drawer from '@mui/material/Drawer';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemText from '@mui/material/ListItemText';
import CloseIcon from '@mui/icons-material/Close';
import Tabs from '@mui/material/Tabs';
import Tab from '@mui/material/Tab';
import ProcessAnalyticsDashboard from './ProcessAnalyticsDashboard';
import ProcessPredictiveAI from './ProcessPredictiveAI';

interface GraphNode {
  id: string;
  label: string;
  type: 'start' | 'end' | 'normal';
  is_automated?: boolean;
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
  absolute_frequency: number;
  is_bottleneck: boolean;
}

interface CustomEdgeData extends Record<string, unknown> {
  absoluteFrequency: number;
  isBottleneck: boolean;
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

// Auto Layout con Dagre
const dagreGraph = new dagre.graphlib.Graph();
dagreGraph.setDefaultEdgeLabel(() => ({}));

const getLayoutedElements = (nodes: Node[], edges: Edge[], direction = 'TB') => {
  dagreGraph.setGraph({ 
    rankdir: direction,
    nodesep: 60,
    ranksep: 80,
    marginx: 40,
    marginy: 40
  });

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, { width: 172, height: 52 });
  });

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(dagreGraph);

  const layoutedNodes = nodes.map((node) => {
    const nodeWithPosition = dagreGraph.node(node.id);
    return {
      ...node,
      targetPosition: Position.Top,
      sourcePosition: Position.Bottom,
      position: {
        x: nodeWithPosition.x - 172 / 2,
        y: nodeWithPosition.y - 52 / 2,
      },
    };
  });

  return { nodes: layoutedNodes, edges };
};

/**
 * Formatta durata in secondi in unità di misura leggibile
 */
const formatDuration = (seconds: number): string => {
  if (!seconds || isNaN(seconds) || seconds <= 0) return '0 sec';

  if (seconds < 60) {
    return `${Math.round(seconds)} sec`;
  }

  const minutes = seconds / 60;
  if (minutes < 60) {
    return `${minutes.toFixed(1)} min`;
  }

  const hours = minutes / 60;
  if (hours < 24) {
    return `${hours.toFixed(1)} ore`;
  }

  const days = hours / 24;
  return `${days.toFixed(1)} giorni`;
};

const ProcessAnalysis: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000/api/v1';

  // State
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const [simulatedGraphData, setSimulatedGraphData] = useState<GraphData | null>(null);
  const [nodePerformance, setNodePerformance] = useState<NodePerformance>({});
  const [simulatedNodePerformance, setSimulatedNodePerformance] = useState<NodePerformance>({});
  const [activeViewMode, setActiveViewMode] = useState<'original' | 'simulated'>('original');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showIsolatedNodes, setShowIsolatedNodes] = useState(false);
  const [selectedNode, setSelectedNode] = useState<any>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [viewType, setViewType] = useState<'performance' | 'frequency'>('performance');
  const [algorithm, setAlgorithm] = useState<'dfg_performance' | 'alpha' | 'heuristic' | 'inductive'>('dfg_performance');
  const [variantsOpen, setVariantsOpen] = useState(false);
  const [variantsData, setVariantsData] = useState<any>(null);
  const [loadingVariants, setLoadingVariants] = useState(false);
  const [activeTab, setActiveTab] = useState(0);

  // ✅ React Flow State per Drag & Drop
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);

  // Fetch graph data
  const fetchGraphData = useCallback(async () => {
    if (!id) return;

    try {
      setLoading(true);
      setError(null);
      
      // ✅ Reset stato grafo per feedback visivo durante caricamento
      setNodes([]);
      setEdges([]);
      setGraphData(null);

      let endpoint = '';
      switch(algorithm) {
        case 'dfg_performance':
          endpoint = `${API_BASE_URL}/mining/discover/dfg-with-automations/${id}?include_performance=true`;
          break;
        case 'alpha':
          endpoint = `${API_BASE_URL}/mining/discover/alpha/${id}`;
          break;
        case 'heuristic':
          endpoint = `${API_BASE_URL}/mining/discover/heuristic/${id}`;
          break;
        case 'inductive':
          endpoint = `${API_BASE_URL}/mining/discover/inductive/${id}`;
          break;
      }

      const response = await axios.get(endpoint);

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

    } catch (err: any) {
      console.error('Errore nel caricamento del grafo:', err);
      setError(err.response?.data?.detail || 'Errore nel caricamento dei dati');
    } finally {
      setLoading(false);
    }
  }, [id, API_BASE_URL, algorithm]);

  useEffect(() => {
    fetchGraphData();
  }, [id, algorithm, fetchGraphData]);

  // ✅ Dati attivi in base alla modalità di visualizzazione
  const activeGraphData = useMemo(() => {
    return activeViewMode === 'simulated' && simulatedGraphData ? simulatedGraphData : graphData;
  }, [activeViewMode, simulatedGraphData, graphData]);

  const activeNodePerformance = useMemo(() => {
    return activeViewMode === 'simulated' ? simulatedNodePerformance : nodePerformance;
  }, [activeViewMode, simulatedNodePerformance, nodePerformance]);

  // Nessun filtro: tutti gli archi sono sempre visibili
  const filteredEdges = useMemo(() => {
    return activeGraphData?.edges || [];
  }, [activeGraphData?.edges]);

  // Calcola nodi connessi dopo filtro archi
  const connectedNodeIds = useMemo(() => {
    const ids = new Set<string>();
    filteredEdges.forEach(edge => {
      ids.add(edge.source);
      ids.add(edge.target);
    });
    return ids;
  }, [filteredEdges]);

  // Convert to React Flow nodes
  const rfNodes: Node[] = useMemo(() => {
    if (!activeGraphData?.nodes) return [];

    return activeGraphData.nodes.map((node) => ({
      id: node.id,
      type: 'custom',
      position: { x: 0, y: 0 },
      data: {
        label: node.label,
        type: node.type,
        avgTime: activeNodePerformance[node.id],
        is_automated: node.is_automated,
        automationRules: node.automation_rules || [],
        isIsolated: !connectedNodeIds.has(node.id),
        onClick: () => { 
            setSelectedNode({
              ...node,
              avgTime: activeNodePerformance[node.id],
              automationRules: node.automation_rules || []
            }); 
            setSidebarOpen(true); 
          },
      },
      // Opacità ridotta per nodi isolati
      style: {
        opacity: connectedNodeIds.has(node.id) ? 1 : (showIsolatedNodes ? 0.2 : 0),
        pointerEvents: connectedNodeIds.has(node.id) ? 'auto' : 'none',
        transition: 'opacity 0.3s ease'
      } as React.CSSProperties
    })).filter(node => showIsolatedNodes || connectedNodeIds.has(node.id));
  }, [activeGraphData?.nodes, activeNodePerformance, connectedNodeIds, showIsolatedNodes]);

  // Convert to React Flow edges
  const rfEdges: Edge[] = useMemo(() => {
    return filteredEdges.map((edge) => ({
      id: edge.id,
      source: edge.source,
      target: edge.target,
      label: edge.label,
      animated: edge.type === 'performance',
      type: 'custom',
      data: {
        absoluteFrequency: edge.absolute_frequency,
        isBottleneck: edge.is_bottleneck,
        weight: edge.weight,
        label: edge.label
      } as CustomEdgeData,
      style: {
        strokeWidth: Math.max(1, Math.min(edge.absolute_frequency / 10, 5)),
        stroke: edge.is_bottleneck ? '#d32f2f' : (edge.type === 'performance' ? '#1976d2' : '#2196f3'),
      },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color: edge.is_bottleneck ? '#d32f2f' : '#1976d2'
      },
    }));
  }, [filteredEdges]);

  // Applica Auto Layout automatico
  useEffect(() => {
    if (rfNodes.length > 0) {
      const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(rfNodes, rfEdges);
      setNodes(layoutedNodes);
      setEdges(layoutedEdges);
    }
  }, [rfNodes, rfEdges, setNodes, setEdges]);

// Edge types for React Flow
const edgeTypes = {
  custom: CustomEdge,
};

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

      console.log('✅ Simulazione completata:', response.data);
      return response.data;
    } catch (err: any) {
      console.error('Errore simulazione:', err);
      throw new Error(err.response?.data?.detail || 'Errore durante la simulazione');
    }
  };

  // ✅ Callback completamento simulazione What-If
  const handleSimulationComplete = useCallback((result: any) => {
    console.log('✅ RISULTATO API SIMULAZIONE:', result);
    
    // ✅ ✅ ✅ SOLUZIONE DEFINITIVA: il backend restituisce ARRAY
    const graph = Array.isArray(result) ? result[0] : result.graph?.graph_data || result.graph || result;

    console.log('✅ GRAFO ESTRATTO:', graph);
    
    if (graph && graph.nodes && graph.edges) {
      console.log('✅ GRAFO VALIDO:', graph.nodes.length, 'nodi', graph.edges.length, 'archi');
      
      // Salva con nuovo riferimento per forzare re-render
      setSimulatedGraphData({
        nodes: [...graph.nodes],
        edges: [...graph.edges]
      });
      
      // Switch automatico sulla vista simulata
      setActiveViewMode('simulated');
      
      console.log('✅ STATO AGGIORNATO CON SUCCESSO');
    } else {
      console.log('❌ STRUTTURA GRAFO NON VALIDA', result);
    }
  }, []);

  // ✅ Reset della modalità What-If
  const handleResetSimulation = useCallback(() => {
    setSimulatedGraphData(null);
    setSimulatedNodePerformance({});
    setActiveViewMode('original');
  }, []);

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
      {/* Tabs Navigation */}
      <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
        <Tabs
          value={activeTab}
          onChange={(_, newValue) => setActiveTab(newValue)}
          sx={{ px: 2 }}
        >
          <Tab label="Mappa Processo" />
          <Tab label="Statistiche" />
          <Tab label="🤖 Previsioni AI" />
        </Tabs>
      </Box>

      {/* Top Bar (solo se tab Mappa è attivo) */}
      {activeTab === 0 && (
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
              label={`${graphData?.nodes.filter(n => n.is_automated || (n.automation_rules && n.automation_rules.length > 0)).length || 0} Automazioni`}
              color="secondary"
              size="small"
            />
          </Box>

          {/* Selettore Algoritmo */}
          <Box minWidth={180}>
            <Typography variant="caption" color="text.secondary" gutterBottom display="block">
              Algoritmo Discovery
            </Typography>
            <select
              aria-label="Seleziona Algoritmo"
              title="Seleziona Algoritmo"
              value={algorithm}
              onChange={(e) => setAlgorithm(e.target.value as any)}
              style={{
                padding: '6px 12px',
                borderRadius: 4,
                border: '1px solid rgba(0,0,0,0.12)',
                backgroundColor: 'white',
                fontSize: '0.875rem',
                width: '100%',
                cursor: 'pointer'
              }}
            >
              <option value="dfg_performance">DFG (Performance)</option>
              <option value="alpha">Alpha Miner</option>
              <option value="heuristic">Heuristic Miner</option>
              <option value="inductive">Inductive Miner</option>
            </select>
          </Box>

          <Tooltip title="Visualizza Varianti">
            <IconButton 
              onClick={async () => {
                setLoadingVariants(true);
                try {
                  const response = await axios.get(`${API_BASE_URL}/mining/discover/variants/${id}`);
                  setVariantsData(response.data);
                  setVariantsOpen(true);
                } catch (err) {
                  console.error('Errore caricamento varianti:', err);
                } finally {
                  setLoadingVariants(false);
                }
              }}
            >
              <ListIcon />
            </IconButton>
          </Tooltip>

          <Tooltip title="Aggiorna">
            <IconButton onClick={fetchGraphData}>
              <RefreshIcon />
            </IconButton>
          </Tooltip>

          {/* ✅ Toggle What-If Comparison View */}
          {simulatedGraphData && (
            <Box display="flex" alignItems="center" gap={1} ml={2}>
              <Chip 
                label={activeViewMode === 'original' ? '🔵 Processo Originale' : '🟢 Simulazione What-If'}
                color={activeViewMode === 'original' ? 'primary' : 'success'}
                sx={{ fontWeight: 'bold' }}
              />
              <Button
                variant={activeViewMode === 'original' ? 'contained' : 'outlined'}
                size="small"
                onClick={() => setActiveViewMode('original')}
              >
                Originale
              </Button>
              <Button
                variant={activeViewMode === 'simulated' ? 'contained' : 'outlined'}
                size="small"
                color="success"
                onClick={() => setActiveViewMode('simulated')}
              >
                Simulato
              </Button>
              <Tooltip title="Chiudi simulazione e torna allo stato originale">
                <IconButton onClick={handleResetSimulation} color="error" size="small">
                  <CloseIcon fontSize="small" />
                </IconButton>
              </Tooltip>
            </Box>
          )}

        </Box>
      </Paper>
      )}

      {/* Tab Content */}
      <Box flex={1} position="relative">
        {activeTab === 0 ? (
          <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          connectionMode={ConnectionMode.Loose}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
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
        ) : activeTab === 1 ? (
          <ProcessAnalyticsDashboard />
        ) : (
          <ProcessPredictiveAI />
        )}
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
        onSimulationComplete={handleSimulationComplete}
      />

      {/* Varianti Drawer */}
      <Drawer anchor="right" open={variantsOpen} onClose={() => setVariantsOpen(false)}>
        <Box sx={{ width: 480, p: 3 }}>
          <Box display="flex" alignItems="center" justifyContent="space-between" mb={2}>
            <Typography variant="h6">Varianti di Processo</Typography>
            <IconButton onClick={() => setVariantsOpen(false)}>
              <CloseIcon />
            </IconButton>
          </Box>

          {loadingVariants ? (
            <Box display="flex" justifyContent="center" p={4}>
              <CircularProgress />
            </Box>
          ) : variantsData ? (
            <>
              <Box mb={2}>
                <Typography variant="body2" color="text.secondary">
                  <strong>Totale varianti:</strong> {variantsData.statistics.total_variants}<br/>
                  <strong>Visualizzate:</strong> {variantsData.statistics.filtered_variants}<br/>
                  <strong>Copertura:</strong> {variantsData.statistics.coverage_percentage.toFixed(1)}%
                </Typography>
              </Box>

              <List sx={{ maxHeight: 'calc(100vh - 180px)', overflow: 'auto' }}>
                {Object.entries(variantsData.variants).map(([path, count]: any, index) => (
                  <ListItem key={index} divider alignItems="flex-start">
                    <ListItemText
                      primary={path}
                      primaryTypographyProps={{
                        fontSize: '0.875rem',
                        lineHeight: 1.5
                      }}
                      secondary={`${count} casi • ${((count / variantsData.statistics.covered_cases) * 100).toFixed(1)}%`}
                    />
                  </ListItem>
                ))}
              </List>
            </>
          ) : null}
        </Box>
      </Drawer>
    </Box>
  );
};

export default ProcessAnalysis;