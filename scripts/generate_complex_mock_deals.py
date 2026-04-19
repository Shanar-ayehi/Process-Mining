#!/usr/bin/env python3
"""
Script generazione dati mock avanzati per Process Mining (Complex Enterprise B2B)
Genera 800 deal su 180 giorni con 21 nodi, loop legali, code e routing guidato da automazioni.
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

DEAL_COUNT = 800

# 21 NODI (Espansione dell'intero funnel di vendita e onboarding)
STAGES = [
    "lead_created", "auto_data_enrichment", "marketing_nurturing", 
    "sdr_contact_1", "sdr_contact_2", "meeting_booked", 
    "discovery_call", "auto_lead_scoring", "technical_assessment", 
    "demo_scheduled", "demo_delivered", "auto_roi_calculation", 
    "proposal_drafting", "legal_review", "security_review", 
    "proposal_sent", "negotiation", "auto_contract_signed", 
    "onboarding_kickoff", "closed_won", "closed_lost", "recycled_to_marketing"
]

ACTORS = ["AE_Senior", "AE_Junior", "SDR_Team", "Legal_Dept", "Pre_Sales_Eng"]
SYSTEM = "WORKFLOW_AUTOMATION"

def generate_timestamp(current_date: datetime, hours_added: int) -> str:
    new_date = current_date + timedelta(hours=hours_added, minutes=random.randint(5, 55))
    if new_date.hour < 9: new_date = new_date.replace(hour=9)
    if new_date.hour > 18: new_date = new_date + timedelta(days=1)
    if new_date.weekday() > 4: new_date = new_date + timedelta(days=2) # Salta weekend
    return new_date.isoformat() + "Z", new_date

def generate_complex_deal(deal_id: int, scenario: str, start_date: datetime) -> dict:
    amount = random.randint(5000, 250000)
    company_size = "Enterprise" if amount > 100000 else random.choice(["SMB", "Mid-Market"])
    actor = random.choice(["AE_Senior", "AE_Junior"])
    
    properties = {
        "dealname": f"Deal {company_size} #{deal_id}",
        "amount": str(amount),
        "hubspot_owner_id": actor,
        "company_size": company_size,
        "enrichment_active": "false",
        "docusign_active": "false"
    }
    
    history = []
    current_time = start_date
    
    def add_step(stage, resource, min_h, max_h):
        nonlocal current_time
        ts, current_time = generate_timestamp(current_time, random.randint(min_h, max_h))
        history.insert(0, {"value": stage, "timestamp": ts, "sourceId": resource})

    # Nodo di partenza universale
    add_step("lead_created", "Marketing_API", 0, 0)

    # ---------------------------------------------------------
    # SCENARIO 1: SMB Fully Automated Fast Track (15% dei casi)
    # L'automazione decide il routing saltando tutto il lavoro umano
    # ---------------------------------------------------------
    if scenario == "smb_auto_fast_track":
        properties["enrichment_active"] = "true"
        properties["docusign_active"] = "true"
        add_step("auto_data_enrichment", SYSTEM, 0, 1)
        add_step("auto_lead_scoring", SYSTEM, 0, 1)
        add_step("demo_delivered", actor, 24, 72)
        add_step("auto_roi_calculation", SYSTEM, 0, 1)
        add_step("proposal_sent", SYSTEM, 1, 4)
        add_step("auto_contract_signed", SYSTEM, 24, 48)
        add_step("closed_won", SYSTEM, 0, 1)

    # ---------------------------------------------------------
    # SCENARIO 2: Enterprise Heavy Process con Loop Legale (25% dei casi)
    # Processo lunghissimo, nessun aiuto dalle automazioni, enormi colli di bottiglia
    # ---------------------------------------------------------
    elif scenario == "enterprise_heavy_loop":
        add_step("sdr_contact_1", "SDR_Team", 24, 48)
        add_step("sdr_contact_2", "SDR_Team", 48, 96)
        add_step("meeting_booked", "SDR_Team", 24, 72)
        add_step("discovery_call", actor, 48, 120)
        add_step("technical_assessment", "Pre_Sales_Eng", 72, 168)
        add_step("demo_delivered", actor, 48, 96)
        add_step("proposal_drafting", actor, 48, 120)
        
        # Inizia il calvario burocratico (Collo di bottiglia)
        add_step("security_review", "Legal_Dept", 120, 300)
        add_step("legal_review", "Legal_Dept", 120, 240)
        add_step("proposal_sent", actor, 24, 48)
        add_step("negotiation", actor, 48, 168)
        
        # Loop: il cliente rifiuta le clausole, torna in Legal
        add_step("legal_review", "Legal_Dept", 96, 192)
        add_step("negotiation", actor, 48, 96)
        
        add_step("closed_won", actor, 24, 72)
        add_step("onboarding_kickoff", actor, 48, 120)

    # ---------------------------------------------------------
    # SCENARIO 3: Standard Mid-Market con Automazioni Parziali (30% dei casi)
    # ---------------------------------------------------------
    elif scenario == "standard_mixed":
        properties["enrichment_active"] = "true"
        add_step("auto_data_enrichment", SYSTEM, 0, 1)
        add_step("meeting_booked", "SDR_Team", 24, 48)
        add_step("discovery_call", actor, 48, 96)
        add_step("demo_scheduled", actor, 24, 48)
        add_step("demo_delivered", actor, 48, 120)
        add_step("proposal_drafting", actor, 48, 96)
        add_step("proposal_sent", actor, 24, 48)
        add_step("negotiation", actor, 72, 240)
        add_step("closed_won", actor, 48, 120)

    # ---------------------------------------------------------
    # SCENARIO 4: Abbandono Prematuro e Reciclo (15% dei casi)
    # ---------------------------------------------------------
    elif scenario == "ghosted_recycled":
        add_step("sdr_contact_1", "SDR_Team", 24, 72)
        add_step("sdr_contact_2", "SDR_Team", 72, 144)
        add_step("marketing_nurturing", SYSTEM, 120, 240)
        add_step("recycled_to_marketing", SYSTEM, 24, 48)

    # ---------------------------------------------------------
    # SCENARIO 5: Perso in Fase Finale (15% dei casi)
    # ---------------------------------------------------------
    elif scenario == "late_loss":
        add_step("sdr_contact_1", "SDR_Team", 24, 48)
        add_step("meeting_booked", "SDR_Team", 48, 96)
        add_step("discovery_call", actor, 48, 96)
        add_step("demo_delivered", actor, 72, 144)
        add_step("proposal_drafting", actor, 72, 120)
        add_step("proposal_sent", actor, 24, 48)
        add_step("negotiation", actor, 120, 360) # Negoziazione lunghissima
        add_step("closed_lost", actor, 24, 48)

    properties["dealstage"] = history[-1]["value"] if history else "lead_created"

    return {
        "id": str(2000000 + deal_id),
        "properties": properties,
        "propertiesWithHistory": {
            "dealstage": history
        }
    }

def main():
    scenarios = (
        ["smb_auto_fast_track"] * 120 + 
        ["enterprise_heavy_loop"] * 200 + 
        ["standard_mixed"] * 240 + 
        ["ghosted_recycled"] * 120 + 
        ["late_loss"] * 120
    )
    random.shuffle(scenarios)
    
    base_start_date = datetime.now() - timedelta(days=180)
    deals = []
    
    for i in range(DEAL_COUNT):
        deal_start_date = base_start_date + timedelta(days=random.randint(0, 150))
        deal = generate_complex_deal(i, scenarios[i], deal_start_date)
        deals.append(deal)
    
    output_path = Path(__file__).resolve().parent.parent / "app" / "data" / "raw" / "mock_deals_complex.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(deals, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Generati {len(deals)} deal complessi (Dataset Avanzato Pronto).")
    print(f"📁 Salvato in: {output_path}")

if __name__ == "__main__":
    main()