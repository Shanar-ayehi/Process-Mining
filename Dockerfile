# Usa l'immagine ufficiale di Python 3.12 (versione leggera)
FROM python:3.12-slim

# Imposta la directory di lavoro dentro il container
WORKDIR /app

# Installa le dipendenze di sistema (incluso graphviz per le immagini di PM4Py)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    graphviz \
    && rm -rf /var/lib/apt/lists/*

# Installa Poetry per gestire i pacchetti Python
RUN pip install poetry

# Copia i file di configurazione delle dipendenze
# L'asterisco su poetry.lock serve nel caso il file non sia ancora stato generato
COPY pyproject.toml poetry.lock* ./

# Configura Poetry per installare i pacchetti direttamente nel sistema del container
RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi --no-root

# Copia tutto il resto del codice sorgente nel container
COPY . .

# Espone la porta su cui gira FastAPI
EXPOSE 8000

# Comando di default per avviare l'applicazione (può essere sovrascritto dal docker-compose)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]