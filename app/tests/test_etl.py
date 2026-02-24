from app.services.etl_services import process_hubspot_json
from app.core.database import save_event_log, load_event_log

def run_test():
    file_path = "data/raw/dummy_deals.json"
    
    print("🚀 FASE 1: ETL...")
    df = process_hubspot_json(file_path)
    
    if df.is_empty():
        print("❌ Nessun dato elaborato.")
        return

    print("\n💾 FASE 2: Salvataggio nel Database...")
    save_event_log(df)
    
    print("\n🔍 FASE 3: Rilettura dal Database (Query SQL under-the-hood)...")
    df_from_db = load_event_log()
    
    print("\n✅ Dati recuperati dal DB pronti per PM4Py:")
    print(df_from_db)

if __name__ == "__main__":
    run_test()