import sys
from loguru import logger

# Remove default handler
logger.remove()

# Add a handler that prints to the console (for you to see)
logger.add(sys.stderr, level="INFO")

# Add a handler that saves errors to a file (for the thesis records)
logger.add("logs/errors.log", rotation="10 MB", level="ERROR", compression="zip")

# Add a handler for all events (traceability)
logger.add("logs/app.log", rotation="1 day", level="DEBUG")

def get_logger():
    return logger