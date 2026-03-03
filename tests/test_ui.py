from taipy import Gui

# Una semplice pagina Markdown
page = """
# 📊 Test Process Mining

Benvenuto Francesco! Se leggi questo, Taipy funziona.

<|{valore}|slider|min=0|max=100|>

Valore selezionato: <|{valore}|>
"""

valore = 50

if __name__ == "__main__":
    # Avvia il server
    Gui(page=page).run(use_reloader=True, port=5000)

