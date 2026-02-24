from app.core.database import load_event_log
from app.services.mining_services import discover_process_map, get_process_statistics

def run_mining_test():
    print("🔍 1. Lettura dati dal Database DuckDB...")
    df = load_event_log()
    
    if df.is_empty():
        print("❌ Database vuoto! Esegui prima test_etl.py")
        return

    print("📊 2. Calcolo Statistiche di Processo...")
    stats = get_process_statistics(df)
    
    print("\n--- 📈 RISULTATI KPI ---")
    print(f"Totale Deal (Casi): {stats['casi_totali']}")
    print(f"Totale Azioni (Eventi): {stats['eventi_totali']}")
    print(f"Percorsi Diversi (Varianti): {stats['numero_varianti']}")
    print(f"Happy Path (Più comune): {stats['percorso_piu_comune']} (eseguito {stats['frequenza_percorso_comune']} volte)")
    print("------------------------\n")

    print("🗺️ 3. Generazione Mappa del Processo (DFG)...")
    # Salveremo l'immagine direttamente nella cartella data/
    discover_process_map(df, "data/process_map.png")
    print("✅ Finito! Vai a controllare la cartella 'data/'. Dovresti trovare un file 'process_map.png'.")

if __name__ == "__main__":
    run_mining_test()