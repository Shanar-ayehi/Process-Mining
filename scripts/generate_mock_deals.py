#!/usr/bin/env python3
"""
Script generazione dati mock realistici per Process Mining e Machine Learning
Genera 500 deal con varianti complesse, tempistiche realistiche e pattern nascosti.
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

DEAL_COUNT = 500
STAGES = ["appointmentscheduled", "qualifiedtobuy", "presentationscheduled", "contractsent", "closedwon", "closedlost"]
OWNER_IDS = ["owner_1_TopBiller", "owner_2_Junior", "owner_3_Average"]
DEAL_NAMES = ["Progetto Alfa", "Soluzione Beta", "Integrazione Delta", "Sviluppo Iota"]

def generate_timestamp(current_date: datetime, hours_added: int) -> str:
    """Genera timestamp aggiungendo ore e minuti variabili, rispettando l'orario di lavoro"""
    new_date = current_date + timedelta(hours=hours_added, minutes=random.randint(10, 59))
    if new_date.hour < 9: new_date = new_date.replace(hour=9)
    if new_date.hour > 18: new_date = new_date + timedelta(days=1)
    return new_date.isoformat() + "Z", new_date

def generate_deal(deal_id: int, scenario: str, start_date: datetime) -> dict:
    amount = random.randint(1000, 100000)
    owner_id = random.choice(OWNER_IDS)
    priority = "High" if amount > 50000 else random.choice(["Medium", "Low"])
    
    properties = {
        "dealname": f"{random.choice(DEAL_NAMES)} #{deal_id}",
        "amount": str(amount),
        "hubspot_owner_id": owner_id,
        "pipeline": "default",
        "priority": priority,
        "workflow_automated_routing": "false",
        "requires_approval": "true" if amount > 70000 else "false"
    }
    
    history = []
    current_time = start_date
    
    # Helper per aggiungere uno step alla history
    def add_step(stage, actor, min_hours, max_hours):
        nonlocal current_time
        ts, current_time = generate_timestamp(current_time, random.randint(min_hours, max_hours))
        # Nota: l'array dealstage in HubSpot ha il timestamp di "ingresso" nello stage, 
        # e le versioni più vecchie sono alla fine dell'array.
        history.insert(0, {"value": stage, "timestamp": ts, "sourceId": actor})

    if scenario == "happy_path":
        # Owner_1 è veloce
        multiplier = 0.5 if owner_id == "owner_1_TopBiller" else 1.0
        add_step("appointmentscheduled", owner_id, 0, 1)
        add_step("qualifiedtobuy", owner_id, int(24*multiplier), int(72*multiplier))
        add_step("presentationscheduled", owner_id, int(48*multiplier), int(120*multiplier))
        add_step("contractsent", owner_id, int(24*multiplier), int(96*multiplier))
        add_step("closedwon", owner_id, int(48*multiplier), int(144*multiplier))
        properties["dealstage"] = "closedwon"

    elif scenario == "automated_fast_track":
        properties["workflow_automated_routing"] = "true"
        add_step("qualifiedtobuy", "WORKFLOW_AUTOMATION", 0, 1) # Salta appointment, fa da solo
        add_step("presentationscheduled", "WORKFLOW_AUTOMATION", 1, 4)
        add_step("contractsent", owner_id, 24, 48)
        add_step("closedwon", owner_id, 12, 48)
        properties["dealstage"] = "closedwon"

    elif scenario == "bottleneck_rework":
        properties["requires_approval"] = "true"
        add_step("appointmentscheduled", owner_id, 0, 1)
        add_step("qualifiedtobuy", owner_id, 24, 48)
        add_step("presentationscheduled", owner_id, 24, 72)
        add_step("contractsent", owner_id, 24, 48)
        
        # Rework: Il contratto è sbagliato, torna in presentazione
        add_step("presentationscheduled", "APPROVAL_MANAGER", 48, 96)
        
        # Rivà a contract sent dopo molto tempo
        add_step("contractsent", owner_id, 120, 240)
        add_step("closedwon", owner_id, 48, 96)
        properties["dealstage"] = "closedwon"

    elif scenario == "closed_lost":
        add_step("appointmentscheduled", owner_id, 0, 1)
        add_step("qualifiedtobuy", owner_id, 48, 120)
        # I low value tendono a perdersi qui
        if priority == "Low" and random.random() > 0.5:
            add_step("closedlost", owner_id, 120, 300)
        else:
            add_step("presentationscheduled", owner_id, 72, 144)
            add_step("closedlost", owner_id, 48, 96)
        properties["dealstage"] = "closedlost"

    return {
        "id": str(1000000 + deal_id),
        "properties": properties,
        "propertiesWithHistory": {
            "dealstage": history
        }
    }

def main():
    # Distribuzione scenari per 500 casi
    scenarios = (["happy_path"] * 250) + (["automated_fast_track"] * 100) + (["bottleneck_rework"] * 80) + (["closed_lost"] * 70)
    random.shuffle(scenarios)
    
    base_start_date = datetime.now() - timedelta(days=180)
    deals = []
    
    for i in range(DEAL_COUNT):
        deal_start_date = base_start_date + timedelta(days=random.randint(0, 150))
        deal = generate_deal(i, scenarios[i], deal_start_date)
        deals.append(deal)
    
    output_path = Path(__file__).resolve().parent.parent / "app" / "data" / "raw" / "mock_deals.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(deals, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Generati {len(deals)} deal (Dataset per Machine Learning pronto).")
    print(f"📁 Salvato in: {output_path}")

if __name__ == "__main__":
    main()