"""
Servizio per What-If Analysis e Simulazione dei Processi

Questo modulo utilizza SimPy per simulare il passaggio di casi attraverso un processo,
permettendo di testare modifiche ai tempi e alle probabilità di transizione.
Supporta anche la simulazione di automazioni HubSpot (workflow).
"""

import simpy
import random
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
from datetime import datetime
from app.core.logger import get_logger

logger = get_logger()


class SimulationService:
    """Servizio per What-If Analysis e simulazione dei processi con SimPy."""
    
    def __init__(self, seed: int = 42):
        """
        Inizializza il servizio di simulazione.
        
        Args:
            seed: Seed per riproducibilità della simulazione
        """
        self.seed = seed
    
    def _normalize_dfg_to_probabilities(
        self, 
        dfg: Dict[Tuple[str, str], int]
    ) -> Dict[str, List[Tuple[str, float]]]:
        """
        Converte il DFG con frequenze in probabilità di transizione.
        
        Args:
            dfg: Dizionario DFG con chiavi (source, target) e valori frequenze
            
        Returns:
            Dizionario con chiavi source e valori lista di (target, probability)
        """
        # ✅ FIX PM4Py: gestisci anche il caso in cui è restituito come tupla (primo elemento)
        if isinstance(dfg, tuple):
            dfg = dfg[0]
            
        # Raggruppa per source
        source_totals = defaultdict(int)
        transitions = defaultdict(list)
        
        for (source, target), frequency in dfg.items():
            source_totals[source] += frequency
            transitions[source].append((target, frequency))
        
        # Normalizza in probabilità
        probabilities = {}
        for source, targets in transitions.items():
            total = source_totals[source]
            probabilities[source] = [
                (target, freq / total) for target, freq in targets
            ]
        
        return probabilities
    
    def _apply_modifications_to_performance(
        self,
        performance_dfg: Dict[Tuple[str, str], Any],
        modifications: Optional[Dict[str, Dict[str, float]]]
    ) -> Dict[Tuple[str, str], Any]:
        """
        Applica le modifiche dell'utente alle performance.
        
        ✅ VERSIONE ROBUSTA: gestisce automaticamente sia i vecchi dizionari PM4Py
        che il nuovo formato nativo Pandas con valori float diretti.
        
        Args:
            performance_dfg: Performance originali (dict o float)
            modifications: Modifiche dell'utente (es. {"Activity": {"time_multiplier": 0.8}})
            
        Returns:
            Performance modificate nello stesso formato di input
        """
        if not modifications:
            return performance_dfg
        
        modified_performance = {}
        for (source, target), perf in performance_dfg.items():
            
            # ✅ FIX: Gestisci ENTRAMBI i formati
            if isinstance(perf, dict):
                # Formato vecchio PM4Py: dizionario con average/minimum/maximum
                modified_perf = perf.copy()
                avg_value = perf.get("average", 0)
                
                if source in modifications:
                    mod = modifications[source]
                    if "time_multiplier" in mod:
                        modified_perf["average"] = avg_value * mod["time_multiplier"]
                        modified_perf["minimum"] = perf.get("minimum", 0) * mod["time_multiplier"]
                        modified_perf["maximum"] = perf.get("maximum", 0) * mod["time_multiplier"]
                
                if target in modifications:
                    mod = modifications[target]
                    if "time_multiplier" in mod:
                        modified_perf["average"] = modified_perf.get("average", 0) * mod["time_multiplier"]
                        modified_perf["minimum"] = modified_perf.get("minimum", 0) * mod["time_multiplier"]
                        modified_perf["maximum"] = modified_perf.get("maximum", 0) * mod["time_multiplier"]
            else:
                # ✅ Formato NUOVO Pandas: valore float diretto
                avg_value = float(perf) if perf is not None else 0.0
                
                if source in modifications:
                    mod = modifications[source]
                    if "time_multiplier" in mod:
                        avg_value *= mod["time_multiplier"]
                
                if target in modifications:
                    mod = modifications[target]
                    if "time_multiplier" in mod:
                        avg_value *= mod["time_multiplier"]
                
                modified_perf = avg_value
            
            modified_performance[(source, target)] = modified_perf
        
        return modified_performance
    
    def _apply_modifications_to_probabilities(
        self,
        transition_probs: Dict[str, List[Tuple[str, float]]],
        modifications: Optional[Dict[str, Dict[str, float]]]
    ) -> Dict[str, List[Tuple[str, float]]]:
        """
        Applica le modifiche dell'utente alle probabilità di transizione.
        
        Args:
            transition_probs: Probabilità originali
            modifications: Modifiche (es. {"Activity": {"target_activity": 0.5}})
            
        Returns:
            Probabilità modificate
        """
        if not modifications:
            return transition_probs
        
        modified_probs = {}
        for source, targets in transition_probs.items():
            if source in modifications:
                mod = modifications[source]
                # Cerca modifiche specifiche ai target
                new_targets = []
                for target, prob in targets:
                    if target in mod:
                        new_targets.append((target, mod[target]))
                    else:
                        new_targets.append((target, prob))
                
                # Rinormalizza
                total = sum(p for _, p in new_targets)
                if total > 0:
                    modified_probs[source] = [(t, p / total) for t, p in new_targets]
                else:
                    modified_probs[source] = new_targets
            else:
                modified_probs[source] = targets
        
        return modified_probs
    
    def _choose_next_activity(
        self,
        current_activity: str,
        transition_probs: Dict[str, List[Tuple[str, float]]],
        rng: random.Random
    ) -> Optional[str]:
        """
        Sceglie la prossima attività basandosi sulle probabilità.
        
        Args:
            current_activity: Attività corrente
            transition_probs: Probabilità di transizione
            rng: Generatore random
            
        Returns:
            Prossima attività o None se non ci sono transizioni
        """
        if current_activity not in transition_probs:
            return None
        
        targets = transition_probs[current_activity]
        if not targets:
            return None
        
        # Scegli basandosi sulle probabilità
        activities = [t for t, _ in targets]
        probs = [p for _, p in targets]
        
        return rng.choices(activities, weights=probs, k=1)[0]
    
    def _calculate_automation_delay(
        self,
        node: Dict[str, Any],
        modifications: Optional[Dict[str, Dict[str, float]]],
        rng: random.Random
    ) -> float:
        """
        Calcola il delay totale delle automazioni per un nodo.
        
        Args:
            node: Nodo del grafo con eventuali automation_rules
            modifications: Modifiche dell'utente
            rng: Generatore random
            
        Returns:
            Delay totale in giorni
        """
        automation_rules = node.get("automation_rules", [])
        if not automation_rules:
            return 0.0
        
        node_label = node.get("label", "")
        
        # Controlla se le automazioni sono disabilitate per questo nodo
        if modifications and node_label in modifications:
            mod = modifications[node_label]
            if mod.get("disable_automation", False):
                logger.info(f"✅ TOGGLE AUTOMAZIONI ATTIVO! Automazioni disabilitate per nodo: {node_label}")
                return 0.0
        
        # Calcola delay totale delle automazioni
        total_delay = 0.0
        trigger_prob = 1.0  # Probabilità base di attivazione
        
        # Applica modifica alla probabilità di trigger se specificata
        if modifications and node_label in modifications:
            mod = modifications[node_label]
            if "automation_trigger_prob" in mod:
                trigger_prob = mod["automation_trigger_prob"]
        
        # Verifica se l'automazione si attiva
        if rng.random() > trigger_prob:
            return 0.0
        
        # Somma i delay di tutte le azioni
        for rule in automation_rules:
            for action in rule.get("actions", []):
                action_delay = action.get("delay_days", 0.0)
                
                # Applica override delay se specificato
                if modifications and node_label in modifications:
                    mod = modifications[node_label]
                    if "override_automation_delay" in mod:
                        action_delay = mod["override_automation_delay"]
                
                total_delay += action_delay
        
        return total_delay

    def _simulate_single_case(
        self,
        env: simpy.Environment,
        case_id: int,
        start_activities: Dict[str, int],
        end_activities: Dict[str, int],
        transition_probs: Dict[str, List[Tuple[str, float]]],
        performance_dfg: Dict[Tuple[str, str], Dict[str, float]],
        simulated_log: List[Dict[str, Any]],
        rng: random.Random,
        graph_nodes: Optional[List[Dict[str, Any]]] = None,
        modifications: Optional[Dict[str, Dict[str, float]]] = None
    ):
        """
        Simula il percorso di un singolo caso attraverso il processo.
        
        Args:
            env: Ambiente SimPy
            case_id: ID del caso
            start_activities: Attività iniziali con frequenze
            end_activities: Attività finali con frequenze
            transition_probs: Probabilità di transizione
            performance_dfg: Performance delle attività
            simulated_log: Lista dove registrare gli eventi
            rng: Generatore random
            graph_nodes: Lista nodi del grafo con automation_rules
            modifications: Modifiche dell'utente
        """
        # Crea mappa nodi per lookup
        nodes_map = {}
        if graph_nodes:
            nodes_map = {node.get("label", ""): node for node in graph_nodes}
        
        # Scegli attività iniziale
        start_acts = list(start_activities.keys())
        start_weights = list(start_activities.values())
        current_activity = rng.choices(start_acts, weights=start_weights, k=1)[0]
        
        timestamp = 0.0
        
        while True:
            # Registra evento corrente
            simulated_log.append({
                "case_id": f"sim_case_{case_id}",
                "activity": current_activity,
                "timestamp": timestamp,
                "resource": "simulated"
            })
            
            # Se è un'attività finale, termina
            if current_activity in end_activities:
                break
            
            # Scegli prossima attività
            next_activity = self._choose_next_activity(
                current_activity, transition_probs, rng
            )
            
            if next_activity is None:
                # Nessuna transizione disponibile, termina
                break
            
            # Estrai durata dalla performance
            edge_key = (current_activity, next_activity)
            if edge_key in performance_dfg:
                val = performance_dfg[edge_key]
                # ✅ Gestisci sia dizionario PM4Py che float nativo Pandas
                duration = val.get("average", 1.0) if isinstance(val, dict) else float(val)
            else:
                # Fallback: usa durata media di tutte le transizioni
                all_avg = []
                for p in performance_dfg.values():
                    if isinstance(p, dict):
                        all_avg.append(p.get("average", 1.0))
                    else:
                        all_avg.append(float(p))
                duration = sum(all_avg) / len(all_avg) if all_avg else 1.0
            
            # Assicurati che la durata sia positiva
            duration = max(duration, 0.1)
            
            # Calcola delay automazioni per il nodo corrente
            automation_delay = 0.0
            if current_activity in nodes_map:
                current_node = nodes_map[current_activity]
                automation_delay = self._calculate_automation_delay(
                    current_node, modifications, rng
                )
            
            # Attendi durata attività + automazioni
            total_delay = duration + automation_delay
            yield env.timeout(total_delay)
            timestamp += total_delay
            
            # Passa alla prossima attività
            current_activity = next_activity
    
    def simulate_process(
        self,
        dfg: Dict[Tuple[str, str], int] | List[Dict[str, Any]],
        performance_dfg: Dict[Tuple[str, str], Dict[str, float]] | List[Dict[str, Any]],
        start_activities: Dict[str, int] | List[Dict[str, Any]],
        end_activities: Dict[str, int] | List[Dict[str, Any]],
        num_cases: int = 100,
        modifications: Optional[Dict[str, Dict[str, float]]] = None,
        graph_nodes: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Esegue la simulazione What-If Analysis.
        """
        logger.info(f"Avvio simulazione What-If: {num_cases} casi")
        
        # --- NORMALIZZAZIONE INPUT (Gestione Pydantic/JSON lists) ---
        
        # 1. Normalizza start_activities
        if isinstance(start_activities, list):
            start_dict = {}
            for item in start_activities:
                if isinstance(item, dict):
                    # Cerca chiavi e valori nei formati noti
                    k = item.get("activity") or item.get("node") or item.get("id") or next(iter(item.keys()), None)
                    v = item.get("count") or item.get("weight") or item.get("frequency") or 1
                    if k: start_dict[k] = v
                elif isinstance(item, str):
                    start_dict[item] = 1
            start_activities = start_dict
            
        # 2. Normalizza end_activities
        if isinstance(end_activities, list):
            end_dict = {}
            for item in end_activities:
                if isinstance(item, dict):
                    k = item.get("activity") or item.get("node") or item.get("id") or next(iter(item.keys()), None)
                    v = item.get("count") or item.get("weight") or item.get("frequency") or 1
                    if k: end_dict[k] = v
                elif isinstance(item, str):
                    end_dict[item] = 1
            end_activities = end_dict
            
        # 3. Normalizza DFG (Frequenze)
        if isinstance(dfg, list):
            dfg_dict = {}
            for edge in dfg:
                if isinstance(edge, dict):
                    src = edge.get("source")
                    tgt = edge.get("target")
                    val = edge.get("weight") or edge.get("frequency") or 1
                    if src and tgt:
                        dfg_dict[(src, tgt)] = val
            dfg = dfg_dict
            
        # 4. Normalizza Performance DFG
        if isinstance(performance_dfg, list):
            perf_dict = {}
            for edge in performance_dfg:
                if isinstance(edge, dict):
                    src = edge.get("source")
                    tgt = edge.get("target")
                    if src and tgt:
                        perf_data = edge.get("performance", {})
                        if not perf_data:
                            # Fallback flat mapping
                            perf_data = {
                                "average": edge.get("average", edge.get("weight", 1.0)),
                                "minimum": edge.get("minimum", edge.get("weight", 1.0)),
                                "maximum": edge.get("maximum", edge.get("weight", 1.0))
                            }
                        perf_dict[(src, tgt)] = perf_data
            performance_dfg = perf_dict

        # --- FINE NORMALIZZAZIONE ---

        # Inizializza random generator
        rng = random.Random(self.seed)
        
        # Normalizza DFG in probabilità
        transition_probs = self._normalize_dfg_to_probabilities(dfg)
        
        # Applica modifiche
        modified_performance = self._apply_modifications_to_performance(
            performance_dfg, modifications
        )
        modified_probs = self._apply_modifications_to_probabilities(
            transition_probs, modifications
        )
        
        # Crea ambiente SimPy
        env = simpy.Environment()
        
        # Lista per raccogliere eventi simulati
        simulated_log = []
        
        # Genera casi
        for case_id in range(num_cases):
            env.process(
                self._simulate_single_case(
                    env, case_id, start_activities, end_activities,
                    modified_probs, modified_performance, simulated_log, rng,
                    graph_nodes=graph_nodes, modifications=modifications
                )
            )
        
        # Esegui simulazione
        env.run()
        
        # Calcola metriche
        metrics = self._calculate_metrics(simulated_log, performance_dfg)
        
        logger.info(f"Simulazione completata: {len(simulated_log)} eventi generati")
        
        return {
            "simulated_cases": simulated_log,
            "num_cases": num_cases,
            "num_events": len(simulated_log),
            "metrics": metrics,
            "modifications_applied": modifications or {},
            "timestamp": datetime.now().isoformat()
        }
    
    def _calculate_metrics(
        self,
        simulated_log: List[Dict[str, Any]],
        original_performance: Dict[Tuple[str, str], Dict[str, float]]
    ) -> Dict[str, Any]:
        """
        Calcola le metriche dalla simulazione.
        
        Args:
            simulated_log: Log simulato
            original_performance: Performance originali per confronto
            
        Returns:
            Dizionario con metriche calcolate
        """
        # Raggruppa per case_id
        cases = defaultdict(list)
        for event in simulated_log:
            cases[event["case_id"]].append(event)
        
        # Calcola cycle time per ogni caso
        cycle_times = []
        for case_id, events in cases.items():
            if len(events) >= 2:
                # Ordina per timestamp
                sorted_events = sorted(events, key=lambda e: e["timestamp"])
                cycle_time = sorted_events[-1]["timestamp"] - sorted_events[0]["timestamp"]
                cycle_times.append(cycle_time)
        
        if not cycle_times:
            return {
                "avg_cycle_time": 0,
                "min_cycle_time": 0,
                "max_cycle_time": 0,
                "std_cycle_time": 0,
                "cases_completed": 0,
                "original_avg_cycle_time": 0,
                "improvement_percentage": 0
            }
        
        # Calcola statistiche
        avg_cycle_time = sum(cycle_times) / len(cycle_times)
        min_cycle_time = min(cycle_times)
        max_cycle_time = max(cycle_times)
        
        # Calcola deviazione standard
        variance = sum((ct - avg_cycle_time) ** 2 for ct in cycle_times) / len(cycle_times)
        std_cycle_time = variance ** 0.5
        
        # Calcola tempo originale medio
        original_avg = 0
        if original_performance:
            original_times = []
            for p in original_performance.values():
                if isinstance(p, dict):
                    original_times.append(p.get("average", 0))
                else:
                    original_times.append(float(p))
            if original_times:
                original_avg = sum(original_times) / len(original_times)
        
        # Calcola miglioramento
        improvement = 0
        if original_avg > 0:
            improvement = ((original_avg - avg_cycle_time) / original_avg) * 100
        
        return {
            "avg_cycle_time": round(avg_cycle_time, 2),
            "min_cycle_time": round(min_cycle_time, 2),
            "max_cycle_time": round(max_cycle_time, 2),
            "std_cycle_time": round(std_cycle_time, 2),
            "cases_completed": len(cycle_times),
            "original_avg_cycle_time": round(original_avg, 2),
            "improvement_percentage": round(improvement, 2)
        }


# Istanza globale del servizio
simulation_service = SimulationService()