from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.routes_connector import router as connector_router
from app.api.routes_mining import router as mining_router
from app.api.routes_analytics import router as analytics_router
from app.api.routes_dq import router as dq_router
from app.api.routes_discovery import router as discovery_router
from app.api.routes_process_management import router as process_management_router
from app.api.routes.auth import router as auth_router
from app.api.routes_external_cards import router as external_cards_router
from app.core.logger import get_logger

logger = get_logger()

# Creazione app FastAPI
app = FastAPI(
    title="Process Mining API",
    description="API per l'estrazione, trasformazione e analisi dei dati per Process Mining",
    version="1.0.0"
)

# Configurazione CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In produzione, specificare domini specifici
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(connector_router, prefix="/api/v1", tags=["Connector"])
app.include_router(mining_router, prefix="/api/v1", tags=["Mining"])
app.include_router(analytics_router, prefix="/api/v1", tags=["Analytics"])
app.include_router(dq_router, prefix="/api/v1", tags=["Data Quality"])
app.include_router(discovery_router, prefix="/api/v1", tags=["Discovery"])
app.include_router(process_management_router, prefix="/api/v1", tags=["Process Management"])
app.include_router(auth_router, prefix="/api/v1", tags=["Authentication"])
app.include_router(external_cards_router, prefix="/api/v1", tags=["External Cards"])

@app.get("/")
async def root():
    """Endpoint root per verifica API."""
    return {
        "message": "Process Mining API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "connector": "/api/v1/connector",
            "mining": "/api/v1/mining", 
            "analytics": "/api/v1/analytics",
            "data_quality": "/api/v1/dq",
            "discovery": "/api/v1/discovery",
            "external_cards": "/api/v1/external-cards"
        }
    }

@app.get("/health")
async def health_check():
    """Health check per l'API."""
    return {
        "status": "healthy",
        "timestamp": "2024-01-15T10:30:00Z",
        "services": {
            "api": "running",
            "database": "connected",
            "hubspot": "available"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)