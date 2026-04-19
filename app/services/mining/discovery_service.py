import os
import math
# Monkey-patch per il bug del PID 0 di pm4py in Docker
if hasattr(os, 'getppid') and os.getppid() == 0:
    os.getppid = lambda: os.getpid()

import pm4py
import polars as pl
import pandas as pd
from typing import Any, Optional, Tuple
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict
from app.core.logger import get_logger
from app.core.config import settings

logger = get_logger()

# Tipi per chiavi DFG (tuple di attività)
DFGKey = Tuple[str, str]


class DFGEdge(BaseModel):
    """Modello Pydantic per validazione archi DFG"""
    model_config = ConfigDict(frozen=True, extra='forbid')
    
    id: str
    source: str
    target: str
    type: str
    weight: float | int
    frequency: int | None = None
    absolute_frequency: int
    label: str | None = None
    value_seconds: float | None = None
    is_bottleneck: bool = False
    details: dict[str, Any] | None = None


class DFGNode(BaseModel):
    """Modello Pydantic per validazione nodi DFG"""
    model_config = ConfigDict(frozen=True, extra='allow')
    
    id: str
    label: str
    type: str
    is_automated: bool = False
    automation_rules: list[dict[str, Any]] | None = None


class DFGGraph(BaseModel):
    """Modello Pydantic per l'intero grafo DFG"""
    model_config = ConfigDict(frozen=True)
    
    nodes: list[DFGNode]
    edges: list[DFGEdge]


class DiscoveryService:
    """Servizio per il Process Discovery con PM4Py."""
    
    def __init__(self):
        self.output_dir = settings.data_dir / "processed"
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def _sanitize_dict_robust(self, d: Any) -> Any:
        """
        Sanifica ricorsivamente TUTTO l'oggetto per renderlo 100% JSON compliant.
        Gestisce NaN, Inf, tipi numpy, pandas, tuple, set e ogni struttura annidata.
        Questa funzione è BULLETPROOF e non dovrebbe mai fallire.
        """
        import numpy as np
        import pandas as pd
        
        # Caso base: None
        if d is None:
            return None
        
        # Gestisci Pandas NA/NaN prima di tutto
        try:
            if pd.isna(d):
                return 0.0
        except:
            pass
        
        # Gestisci numpy NaN e Inf
        try:
            if np.isnan(d) or np.isinf(d):
                return 0.0
        except:
            pass
        
        # Tipi numpy numerici
        if isinstance(d, np.floating):
            return float(d)
        if isinstance(d, np.integer):
            return int(d)
        if isinstance(d, np.bool_):
            return bool(d)
        
        # Dizionari
        if isinstance(d, dict):
            return {
                self._sanitize_dict_robust(key): self._sanitize_dict_robust(value)
                for key, value in d.items()
            }
        
        # Liste
        if isinstance(d, list):
            return [self._sanitize_dict_robust(item) for item in d]
        
        # Tuple
        if isinstance(d, tuple):
            return tuple(self._sanitize_dict_robust(item) for item in d)
        
        # Set
        if isinstance(d, set):
            return [self._sanitize_dict_robust(item) for item in d]
        
        # Pandas Series
        if isinstance(d, pd.Series):
            return self._sanitize_dict_robust(d.fillna(0).to_list())
        
        # Pandas DataFrame
        if isinstance(d, pd.DataFrame):
            return self._sanitize_dict_robust(d.fillna(0).to_dict(orient='records'))
        
        # Float standard Python
        if isinstance(d, float):
            if math.isnan(d) or math.isinf(d):
                return 0.0
            return d
        
        # Tutti gli altri tipi vengono lasciati come sono
        return d
    
    def discover_dfg(self, df: pl.DataFrame, 
                    output_image_path: Optional[str] = None,
                    workflows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        """
        Scopre il Directly-Follows Graph (DFG) dal DataFrame.
        
        Args:
            df: DataFrame Polars con event log
            output_image_path: Path per salvare l'immagine (opzionale)
            workflows: Lista opzionale di workflow HubSpot da mappare sui nodi
            
        Returns:
            Dizionario con DFG e statistiche
        """
        logger.info("Avvio Process Discovery (DFG)...")
        
        try:
            # Converte Polars a Pandas e formatta per PM4Py
            log = self._prepare_event_log(df)
            
            # Calcola DFG
            dfg, start_activities, end_activities = pm4py.discover_dfg(log)
            
            # Salva immagine se richiesto
            if output_image_path is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_image_path = str(self.output_dir / f"process_map_{timestamp}.png")
            
            pm4py.save_vis_dfg(dfg, start_activities, end_activities, output_image_path)
            logger.info(f"Mappa del processo salvata in: {output_image_path}")
            
            # Calcola statistiche
            stats = self._calculate_dfg_statistics(dfg, start_activities, end_activities)
            
            # Converte DFG in formato JSON per il frontend (con mapping workflow)
            graph_data = self._dfg_to_graph_format(dfg, start_activities, end_activities, is_performance=False, workflows=workflows)
            
            result = {
                'dfg': dfg,
                'start_activities': start_activities,
                'end_activities': end_activities,
                'image_path': output_image_path,
                'graph_data': graph_data,
                'statistics': stats,
                'discovery_timestamp': datetime.now().isoformat()
            }
            
            # ✅ SANIFICA TUTTO PRIMA DI RESTITUIRE
            result = self._sanitize_dict_robust(result)
            
            logger.info("Process Discovery (DFG) completato con successo")
            return result
            
        except Exception as e:
            logger.error(f"Errore nel Process Discovery: {e}")
            raise
    
    def discover_alpha_miner(self, df: pl.DataFrame) -> dict[str, Any]:
        """
        Scopre il modello di processo con Alpha Miner.
        
        Args:
            df: DataFrame Polars con event log
            
        Returns:
            Dizionario con modello e statistiche
        """
        logger.info("Avvio Alpha Miner...")
        
        try:
            log = self._prepare_event_log(df)
            
            # Calcola modello con Alpha Miner
            net, initial_marking, final_marking = pm4py.discover_petri_net_alpha(log)
            
            # Calcola statistiche
            stats = self._calculate_petri_net_statistics(net, initial_marking, final_marking)
            
            # Converte Petri Net in formato JSON per il frontend
            graph_data = self._petri_net_to_graph_format(net, initial_marking, final_marking)
            
            result = {
                'petri_net': net,
                'initial_marking': initial_marking,
                'final_marking': final_marking,
                'graph_data': graph_data,
                'statistics': stats,
                'discovery_timestamp': datetime.now().isoformat()
            }
            
            logger.info("Alpha Miner completato con successo")
            return result
            
        except Exception as e:
            logger.error(f"Errore in Alpha Miner: {e}")
            raise
    
    def discover_heuristic_miner(self, df: pl.DataFrame, 
                               dependency_threshold: float = 0.5) -> dict[str, Any]:
        """
        Scopre il modello di processo con Heuristic Miner.
        
        Args:
            df: DataFrame Polars con event log
            dependency_threshold: Soglia di dipendenza per Heuristic Miner
            
        Returns:
            Dizionario con modello e statistiche
        """
        logger.info(f"Avvio Heuristic Miner (threshold: {dependency_threshold})...")
        
        try:
            log = self._prepare_event_log(df)
            
            # Calcola modello con Heuristic Miner
            net, initial_marking, final_marking = pm4py.discover_petri_net_heuristics(
                log, dependency_threshold=dependency_threshold
            )
            
            # Calcola statistiche
            stats = self._calculate_petri_net_statistics(net, initial_marking, final_marking)
            
            # Converte Petri Net in formato JSON per il frontend
            graph_data = self._petri_net_to_graph_format(net, initial_marking, final_marking)
            
            result = {
                'petri_net': net,
                'initial_marking': initial_marking,
                'final_marking': final_marking,
                'dependency_threshold': dependency_threshold,
                'graph_data': graph_data,
                'statistics': stats,
                'discovery_timestamp': datetime.now().isoformat()
            }
            
            logger.info("Heuristic Miner completato con successo")
            return result
            
        except Exception as e:
            logger.error(f"Errore in Heuristic Miner: {e}")
            raise
    
    def discover_inductive_miner(self, df: pl.DataFrame) -> dict[str, Any]:
        """
        Scopre il modello di processo con Inductive Miner.
        
        Args:
            df: DataFrame Polars con event log
            
        Returns:
            Dizionario con modello e statistiche
        """
        logger.info("Avvio Inductive Miner...")
        
        try:
            log = self._prepare_event_log(df)
            
            # Calcola modello con Inductive Miner
            tree = pm4py.discover_process_tree_inductive(log)
            
            # Converte albero in rete di Petri
            net, initial_marking, final_marking = pm4py.convert_to_petri_net(tree)
            
            # Calcola statistiche
            stats = self._calculate_petri_net_statistics(net, initial_marking, final_marking)
            
            # Converte Petri Net in formato JSON per il frontend
            # Nota: il Process Tree non viene serializzato, ma la Petri Net derivata è sufficiente
            graph_data = self._petri_net_to_graph_format(net, initial_marking, final_marking)
            
            result = {
                'process_tree': tree,
                'petri_net': net,
                'initial_marking': initial_marking,
                'final_marking': final_marking,
                'graph_data': graph_data,
                'statistics': stats,
                'discovery_timestamp': datetime.now().isoformat()
            }
            
            logger.info("Inductive Miner completato con successo")
            return result
            
        except Exception as e:
            logger.error(f"Errore in Inductive Miner: {e}")
            raise
    
    def discover_variants(self, df: pl.DataFrame, 
                         min_frequency_threshold: float = 0.05) -> dict[str, Any]:
        """
        Scopre le varianti del processo.
        
        Args:
            df: DataFrame Polars con event log
            min_frequency_threshold: Soglia minima di frequenza per includere varianti
            
        Returns:
            Dizionario con varianti e statistiche
        """
        logger.info(f"Avvio scoperta varianti (threshold: {min_frequency_threshold})...")
        
        try:
            log = self._prepare_event_log(df)
            
            # Calcola varianti
            variants = pm4py.get_variants(log)
            
            # Filtra varianti per frequenza
            total_cases = len(log['case:concept:name'].unique())
            filtered_variants = {}
            
            for variant, count in variants.items():
                frequency = count / total_cases
                if frequency >= min_frequency_threshold:
                    # Converte la tupla/lista in una stringa leggibile per JSON e Frontend
                    if isinstance(variant, tuple) or isinstance(variant, list):
                        # Se gli elementi sono stringhe pulite
                        variant_str = " ➔ ".join(str(v) for v in variant)
                    else:
                        variant_str = str(variant)
                        
                    filtered_variants[variant_str] = count
            
            # Ordina varianti per frequenza
            sorted_variants = sorted(filtered_variants.items(), key=lambda x: x[1], reverse=True)
            
            # Calcola statistiche
            stats = {
                'total_variants': len(variants),
                'filtered_variants': len(filtered_variants),
                'covered_cases': sum(count for _, count in filtered_variants.items()),
                'coverage_percentage': (sum(count for _, count in filtered_variants.items()) / total_cases) * 100,
                'top_variant': sorted_variants[0] if sorted_variants else None
            }
            
            result = {
                'variants': dict(sorted_variants),
                'min_frequency_threshold': min_frequency_threshold,
                'statistics': stats,
                'discovery_timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Scoperta varianti completata: {len(filtered_variants)} varianti trovate")
            return result
            
        except Exception as e:
            logger.error(f"Errore nella scoperta varianti: {e}")
            raise
    
    def discover_performance_dfg(self, df: pl.DataFrame,
                                workflows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        """
        Scopre il DFG con informazioni di performance.
        
        Args:
            df: DataFrame Polars con event log
            workflows: Lista opzionale di workflow HubSpot da mappare sui nodi
            
        Returns:
            Dizionario con DFG performance e statistiche
        """
        logger.info("Avvio Performance DFG (Pandas Nativo)...")
        
        try:
            import pandas as pd
            import pm4py
            
            pdf = df.to_pandas()
            
            # 1. Parsing Date e Ordinamento ASSOLUTO
            pdf['timestamp'] = pd.to_datetime(pdf['timestamp'], utc=True, errors='coerce')
            pdf = pdf.dropna(subset=['case_id', 'timestamp'])
            pdf = pdf.sort_values(by=['case_id', 'timestamp'], ascending=[True, True])
            
            # 2. Calcolo Matematico Nativo (Infallibile, bypassa PM4Py)
            pdf['next_activity'] = pdf.groupby('case_id')['activity'].shift(-1)
            pdf['next_timestamp'] = pdf.groupby('case_id')['timestamp'].shift(-1)
            pdf['duration_sec'] = (pdf['next_timestamp'] - pdf['timestamp']).dt.total_seconds()
            
            edges_df = pdf.dropna(subset=['next_activity', 'duration_sec'])
            perf_dfg = edges_df.groupby(['activity', 'next_activity'])['duration_sec'].mean().to_dict()
            
            # 3. Individuazione Automazioni (con Fallback)
            automated_activities = []
            if 'resource' in pdf.columns:
                mask = pdf['resource'].astype(str).str.upper().str.contains('AUTOMATION', na=False)
                automated_activities = pdf[mask]['activity'].unique().tolist()
                logger.info(f"Trovate {len(automated_activities)} attività automatizzate dalla colonna resource")

            # Fallback se le risorse sono ancora hashate da vecchi volumi Docker:
            if not automated_activities:
                # Identifica automaticamente attività tipiche dei bot nel CRM
                typical_bot_tasks = ['Qualified', 'Contract Sent', 'Email Scheduled', 'Lead Assigned', 'Stage Updated']
                automated_activities = [
                    act for act in pdf['activity'].unique() 
                    if any(bot_task.upper() in str(act).upper() for bot_task in typical_bot_tasks)
                ]
                if automated_activities:
                    logger.info(f"Trovate {len(automated_activities)} attività automatizzate tramite fallback intelligente")
                
            # 4. Estrazione Frequenze base tramite PM4Py (solo per consistenza archi)
            pdf_formatted = pm4py.format_dataframe(pdf, case_id='case_id', activity_key='activity', timestamp_key='timestamp')
            freq_result = pm4py.discover_dfg(pdf_formatted)
            freq_dfg = freq_result[0] if isinstance(freq_result, tuple) else freq_result
            
            # ✅ ALGORITMO COLLI DI BOTTIGLIA
            # Calcola soglia: 75° percentile + 1 deviazione standard
            all_durations = list(perf_dfg.values())
            bottleneck_threshold = 0.0
            is_bottleneck_map = {}
            
            if all_durations:
                import numpy as np
                p75 = np.percentile(all_durations, 75)
                std_dev = np.std(all_durations)
                bottleneck_threshold = p75 + std_dev
                
                logger.info(f"✅ ALGORITMO COLLI DI BOTTIGLIA ATTIVO! Soglia calcolata: {bottleneck_threshold:.2f} secondi")
                
                for (source, target), duration in perf_dfg.items():
                    is_bottleneck_map[(source, target)] = duration > bottleneck_threshold
                
                bottleneck_count = sum(1 for v in is_bottleneck_map.values() if v)
                logger.info(f"✅ Trovati {bottleneck_count} colli di bottiglia su {len(is_bottleneck_map)} archi totali")
            
            nodes = []
            edges = []
            added_nodes = set()
            
            # Nodi di inizio e fine VERI calcolati direttamente
            start_nodes = set(pdf.groupby('case_id')['activity'].first().unique())
            end_nodes = set(pdf.groupby('case_id')['activity'].last().unique())
            
            for (source, target), freq in freq_dfg.items():
                if source not in added_nodes:
                    nodes.append({
                        "id": source, "label": source,
                        "type": "start" if source in start_nodes else ("end" if source in end_nodes else "normal"),
                        "is_automated": source in automated_activities
                    })
                    added_nodes.add(source)
                if target not in added_nodes:
                    nodes.append({
                        "id": target, "label": target,
                        "type": "end" if target in end_nodes else ("start" if target in start_nodes else "normal"),
                        "is_automated": target in automated_activities
                    })
                    added_nodes.add(target)
                    
                # Assegna i secondi calcolati direttamente da Pandas
                avg_seconds = perf_dfg.get((source, target), 0.0)
                
                # Formatta label leggibile
                if avg_seconds >= 86400:
                    label = f"{avg_seconds / 86400:.1f} giorni"
                elif avg_seconds >= 3600:
                    label = f"{avg_seconds / 3600:.1f} ore"
                elif avg_seconds >= 60:
                    label = f"{avg_seconds / 60:.1f} min"
                else:
                    label = f"{avg_seconds:.1f} sec"
                    
                edges.append({
                    "id": f"{source}-{target}", "source": source, "target": target,
                    "type": "performance", "weight": float(avg_seconds), "label": label,
                    "value_seconds": avg_seconds,
                    "absolute_frequency": int(freq),
                    "is_bottleneck": is_bottleneck_map.get((source, target), False),
                    "details": {
                        "average_seconds": avg_seconds
                    }
                })
            
            # Applica mapping workflows se presenti
            if workflows:
                nodes = self._map_workflows_to_nodes(nodes, workflows)
            
            graph_data = {"nodes": nodes, "edges": edges}
            
            logger.info(f"✅ ABSOLUTE FREQUENCY AGGIUNTA SU TUTTI GLI {len(edges)} ARCHI DEL GRAFO")
            
            result = {
                'frequency_dfg': freq_dfg,
                'performance_dfg': perf_dfg,
                'start_activities': list(start_nodes),
                'end_activities': list(end_nodes),
                'graph_data': graph_data,
                'discovery_timestamp': datetime.now().isoformat()
            }
            
            # ✅ SANIFICA TUTTO PRIMA DI RESTITUIRE
            result = self._sanitize_dict_robust(result)
            
            logger.info("Performance DFG completato con successo (calcolo nativo Pandas)")
            return result
            
        except Exception as e:
            logger.error(f"Errore in Performance DFG: {e}")
            raise
    
    def _map_workflows_to_nodes(
        self,
        nodes: list[dict[str, Any]],
        workflows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Mappa i workflow HubSpot sui nodi del grafo.
        
        Analizza i trigger dei workflow e li associa ai nodi corrispondenti
        basandosi sulle proprietà che attivano il workflow (es. dealstage).
        
        Args:
            nodes: Lista di nodi del grafo
            workflows: Lista di workflow estratti da HubSpot
            
        Returns:
            Lista di nodi con attributo automation_rules aggiunto
        """
        if not workflows:
            return nodes
        
        # Crea mappa nodi per lookup veloce
        nodes_map = {node["label"]: node for node in nodes}
        
        for workflow in workflows:
            workflow_id = workflow.get("id")
            workflow_name = workflow.get("name", "Unnamed Workflow")
            trigger = workflow.get("trigger", {})
            
            # Analizza il tipo di trigger
            trigger_type = trigger.get("type")
            
            if trigger_type == "PROPERTY_CHANGE":
                # Trigger basato su cambio proprietà (es. dealstage)
                trigger_property = trigger.get("propertyName", "")
                trigger_value = trigger.get("value", "")
                
                # Cerca nodo corrispondente
                if trigger_value in nodes_map:
                    node = nodes_map[trigger_value]
                    
                    # Inizializza automation_rules se non presente
                    if "automation_rules" not in node:
                        node["automation_rules"] = []
                    
                    # Estrai azioni dal workflow
                    actions = self._extract_workflow_actions(workflow)
                    
                    # Aggiungi regola
                    node["automation_rules"].append({
                        "workflow_id": workflow_id,
                        "workflow_name": workflow_name,
                        "trigger_type": trigger_type,
                        "trigger_property": trigger_property,
                        "trigger_value": trigger_value,
                        "actions": actions
                    })
        
        return nodes
    
    def _extract_workflow_actions(self, workflow: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Estrae le azioni da un workflow HubSpot.
        
        Args:
            workflow: Dati workflow da HubSpot
            
        Returns:
            Lista di azioni con tipo e delay (in giorni)
        """
        actions = []
        
        # Le azioni sono nella struttura workflow["actions"]
        workflow_actions = workflow.get("actions", [])
        
        for action in workflow_actions:
            action_type = action.get("type", "")
            action_config = action.get("config", {})
            
            # Converti delay da millisecondi a secondi
            delay_ms = action_config.get("delayMillis", 0)
            delay_seconds = delay_ms / 1000  # ms -> secondi
            
            if action_type == "SEND_EMAIL":
                actions.append({
                "type": "SEND_EMAIL",
                "delay_seconds": delay_seconds,
                "email_id": action_config.get("emailId")
                })
            
            elif action_type == "SET_PROPERTY":
                actions.append({
                "type": "SET_PROPERTY",
                "delay_seconds": delay_seconds,
                "property": action_config.get("propertyName"),
                "value": action_config.get("value")
                })
            
            elif action_type == "CREATE_TASK":
                actions.append({
                "type": "CREATE_TASK",
                "delay_seconds": delay_seconds,
                "task_type": action_config.get("taskType")
                })
            
            elif action_type == "WEBHOOK":
                actions.append({
                "type": "WEBHOOK",
                "delay_seconds": delay_seconds,
                "url": action_config.get("url")
                })
            
            else:
                # Azione generica
                actions.append({
                "type": action_type,
                "delay_seconds": delay_seconds
                })
        
        return actions

    def _petri_net_to_graph_format(
        self,
        net,
        initial_marking,
        final_marking
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Converte una Rete di Petri raw di PM4Py in un formato JSON-serializable
        comprensibile per il frontend (lista di Nodi e Archi).

        Args:
            net: Rete di Petri di PM4Py
            initial_marking: Marcatura iniziale
            final_marking: Marcatura finale

        Returns:
            Dizionario con chiavi "nodes" e "edges" in formato JSON-safe
        """
        nodes: list[dict[str, Any]] = []
        edges: list[dict[str, Any]] = []

        # Determina quali places sono start/end basandosi sulle marcature
        initial_places = set(initial_marking.keys()) if initial_marking else set()
        final_places = set(final_marking.keys()) if final_marking else set()

        # --- Nodi: Places ---
        for place in net.places:
            place_name = str(place.name)
            is_start = place in initial_places
            is_end = place in final_places
            
            nodes.append({
                "id": place_name,
                "label": place_name,
                "type": "place",
                "is_start": is_start,
                "is_end": is_end
            })

        # --- Nodi: Transitions ---
        for transition in net.transitions:
            transition_name = str(transition.name)
            # Se la transizione ha label None, è una transizione invisibile (tau/silent)
            label = transition.label if transition.label is not None else "tau"
            
            nodes.append({
                "id": transition_name,
                "label": label,
                "type": "transition",
                "is_invisible": transition.label is None
            })

        # --- Archi ---
        for i, arc in enumerate(net.arcs):
            # Forza conversione a stringa per evitare problemi di serializzazione oggetti
            source_name = str(arc.source.name)
            target_name = str(arc.target.name)
            
            # ID univoco garantito
            edge_id = f"e_{source_name}_{target_name}_{i}"
            
            edges.append({
                "id": edge_id,
                "source": source_name,
                "target": target_name,
                "type": "default", # Tipo standard di React Flow invece di "arc"
                "weight": 1,       # Peso fittizio per bypassare il filtro frequenza del frontend
                "label": ""        # Nessuna label per gli archi della rete di Petri
            })

        return {"nodes": nodes, "edges": edges}

    def _dfg_to_graph_format(
        self,
        dfg: dict[DFGKey, Any],
        start_activities: dict[str, int],
        end_activities: dict[str, int],
        is_performance: bool = False,
        workflows: list[dict[str, Any]] | None = None,
        automated_activities: list[str] | None = None
    ) -> dict[str, list[dict[str, Any]]]:
        automated_activities = automated_activities or []
        """
        Converte un DFG raw di PM4Py in un formato JSON-serializable
        comprensibile per il frontend (lista di Nodi e Archi).

        Args:
            dfg: Dizionario DFG di PM4Py (chiavi: tuple (source, target))
            start_activities: Attività iniziali con frequenze
            end_activities: Attività finali con frequenze
            is_performance: Se True, il DFG contiene metriche di performance
            workflows: Lista opzionale di workflow HubSpot da mappare sui nodi

        Returns:
            Dizionario con chiavi "nodes" e "edges" in formato JSON-safe
        """
        nodes_dict: dict[str, dict[str, Any]] = {}
        edges: list[dict[str, Any]] = []

        # Determina i tipi di nodo basandosi su start/end activities
        start_set = set(start_activities.keys())
        end_set = set(end_activities.keys())

        # Itera su ogni arco del DFG
        for (source, target), value in dfg.items():
            # --- Nodi ---
            # Aggiungi nodo source se non presente
            if source not in nodes_dict:
                node_type = "start" if source in start_set else ("end" if source in end_set else "normal")
                nodes_dict[source] = {
                    "id": source,
                    "label": source,
                    "type": node_type,
                    "is_automated": source in automated_activities
                }

            # Aggiungi nodo target se non presente
            if target not in nodes_dict:
                node_type = "start" if target in start_set else ("end" if target in end_set else "normal")
                nodes_dict[target] = {
                    "id": target,
                    "label": target,
                    "type": node_type,
                    "is_automated": target in automated_activities
                }

            # --- Archi ---
            edge_id = f"e_{source}_{target}"

            if is_performance:
                # Per Performance DFG, value è un dizionario con 'average', 'minimum', 'maximum'
                if isinstance(value, dict):
                    # ✅ PM4Py RESTITUISCE TEMPI IN SECONDI, NON GIORNI
                    avg_seconds = value.get('average', 0)
                    min_seconds = value.get('minimum', 0)
                    max_seconds = value.get('maximum', 0)
                    
                    # ✅ Formatta correttamente l'etichetta di tempo
                    hours = avg_seconds / 3600
                    if hours > 24:
                        time_label = f"{round(hours / 24, 1)} giorni"
                    elif hours > 1:
                        time_label = f"{round(hours, 1)} ore"
                    else:
                        minutes = avg_seconds / 60
                        time_label = f"{round(minutes, 1)} min"
                    
                    edges.append({
                        "id": edge_id,
                        "source": source,
                        "target": target,
                        "type": "performance",
                        "weight": avg_seconds,
                        "value_seconds": avg_seconds,
                        "label": time_label,
                        "details": {
                            "average_seconds": avg_seconds,
                            "minimum_seconds": min_seconds,
                            "maximum_seconds": max_seconds
                        }
                    })
                else:
                    # Fallback se il valore non è un dizionario
                    edges.append({
                        "id": edge_id,
                        "source": source,
                        "target": target,
                        "type": "performance",
                        "weight": float(value) if value else 0,
                        "label": f"{value} giorni"
                    })
            else:
                # Per DFG standard, value è la frequenza (intero)
                frequency = int(value) if value else 0
                edges.append({
                    "id": edge_id,
                    "source": source,
                    "target": target,
                    "type": "frequency",
                    "weight": frequency,
                    "absolute_frequency": frequency,
                    "label": f"{frequency} occorrenze"
                })

        # Converti dizionario nodi in lista
        nodes = list(nodes_dict.values())
        
        # Mappa workflow sui nodi se forniti
        if workflows:
            # 🔴 IMPORTANTE: Converti i nodi Pydantic FROZEN in dizionari mutabili
            nodes = [n.model_dump() if hasattr(n, 'model_dump') else dict(n) for n in nodes]
            nodes = self._map_workflows_to_nodes(nodes, workflows)

        return {"nodes": nodes, "edges": edges}

    def _prepare_event_log(self, df: pl.DataFrame) -> pd.DataFrame:
        """Converte DataFrame Polars in Pandas formattato per PM4Py."""
        # Converte Polars in Pandas
        pdf = df.to_pandas()
        
        # Forza la colonna a datetime per permettere a PM4Py di calcolare le durate
        if 'timestamp' in pdf.columns:
            pdf['timestamp'] = pd.to_datetime(pdf['timestamp'], utc=True, errors='coerce')
        
        # Formattiamo indicando a PM4Py le 3 colonne obbligatorie
        formatted_log = pm4py.format_dataframe(
            pdf,
            case_id='case_id',
            activity_key='activity',
            timestamp_key='timestamp'
        )
        return formatted_log
    
    def _calculate_dfg_statistics(self, dfg: dict, start_activities: dict, end_activities: dict) -> dict[str, Any]:
        """Calcola statistiche per il DFG."""
        return {
            'total_edges': len(dfg),
            'unique_activities': len(set(list(dfg.keys()) + [edge[1] for edge in dfg.keys()])),
            'start_activities_count': len(start_activities),
            'end_activities_count': len(end_activities),
            'most_frequent_transition': max(dfg.items(), key=lambda x: x[1]) if dfg else None
        }
    
    def _calculate_petri_net_statistics(self, net, initial_marking, final_marking) -> dict[str, Any]:
        """Calcola statistiche per la rete di Petri."""
        return {
            'places_count': len(net.places),
            'transitions_count': len(net.transitions),
            'arcs_count': len(net.arcs),
            'initial_marking_places': len(initial_marking),
            'final_marking_places': len(final_marking)
        }
    
    def _calculate_performance_statistics(self, performance_dfg: dict) -> dict[str, Any]:
        """Calcola statistiche per il DFG performance."""
        if not performance_dfg:
            return {}
        
        avg_durations = [info.get('average', info.get('mean', 0)) for info in performance_dfg.values()]
        min_durations = [info.get('minimum', info.get('min', 0)) for info in performance_dfg.values()]
        max_durations = [info.get('maximum', info.get('max', 0)) for info in performance_dfg.values()]
        
        return {
            'total_transitions': len(performance_dfg),
            'avg_duration_mean': sum(avg_durations) / len(avg_durations) if avg_durations else 0,
            'avg_duration_min': min(avg_durations) if avg_durations else 0,
            'avg_duration_max': max(avg_durations) if avg_durations else 0,
            'fastest_transition': min(performance_dfg.items(), key=lambda x: x[1].get('average', x[1].get('mean', 0))) if avg_durations else None,
            'slowest_transition': max(performance_dfg.items(), key=lambda x: x[1].get('average', x[1].get('mean', 0))) if avg_durations else None
        }

# Creazione istanza globale
discovery_service = DiscoveryService()