from taipy.gui import Gui
from app.core.database import load_event_log
from app.services.mining_services import get_process_statistics

# 1. Carichiamo i dati dal DB locale
df = load_event_log()

# 2. Calcoliamo i KPI
stats = get_process_statistics(df)
casi = stats.get("casi_totali", 0)
varianti = stats.get("numero_varianti", 0)
happy_path = stats.get("percorso_piu_comune", "N/A")

# 3. Percorso dell'immagine generata da PM4Py
image_path = "data/process_map.png"

# 4. Disegniamo l'interfaccia web (Markdown di Taipy)
page = """
# 📊 Process Mining Dashboard

Benvenuto nella piattaforma di analisi aziendale.

<|layout|columns=1 1 1|
<|card|
## 📦 Totale Deal
**<|{casi}|>**
|>

<|card|
## 🔀 Varianti
**<|{varianti}|>**
|>

<|card|
## ⭐ Happy Path
**<|{happy_path}|>**
|>
|>

---

### 🗺️ Mappa del Processo (Directly-Follows Graph)
<|{image_path}|image|width=800px|>
"""

if __name__ == "__main__":
    print("🚀 Avvio della Dashboard...")
    Gui(page=page).run(use_reloader=True, port=5000, dark_mode=False)