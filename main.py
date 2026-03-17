#!/usr/bin/env python3
"""
Script principale di avvio del sistema Process Mining completamente plastico.
Questo script coordina tutti i componenti del sistema e fornisce
un'interfaccia unificata per l'avvio e la gestione del programma.
"""

import asyncio
import sys
import os
import signal
import time
from pathlib import Path
from typing import Dict, Any, Optional
import logging

# Aggiungi il percorso dell'app al Python path
sys.path.insert(0, str(Path(__file__).parent))

from app.core.logger import get_logger
from app.core.config import settings
from app.core.bootstrap import bootstrap_manager
from app.services.etl.reactive_etl import reactive_etl_manager
from app.core.integration import integration_manager
from app.ui.main import main as ui_main

logger = get_logger()

class ProcessMiningSystem:
    """Classe principale per la gestione del sistema Process Mining."""
    
    def __init__(self):
        self.is_running = False
        self.start_time = None
        self.components = {
            'bootstrap': False,
            'etl': False,
            'ui': False,
            'integration': False
        }
        
    async def initialize_system(self, auto_bootstrap: bool = True, 
                              auto_start_etl: bool = True,
                              auto_start_ui: bool = True) -> bool:
        """
        Inizializza il sistema completo.
        
        Args:
            auto_bootstrap: Abilita bootstrap automatico
            auto_start_etl: Abilita avvio ETL automatico
            auto_start_ui: Abilita avvio UI automatico
            
        Returns:
            True se l'inizializzazione ha successo
        """
        try:
            logger.info("🚀 Avvio sistema Process Mining completamente plastico")
            logger.info("=" * 60)
            
            self.start_time = time.time()
            self.is_running = True
            
            # Step 1: Bootstrap sistema
            if auto_bootstrap:
                logger.info("📋 Step 1: Bootstrap sistema")
                bootstrap_result = await bootstrap_manager.bootstrap_system()
                
                if bootstrap_result.get('success', False):
                    logger.info("✅ Bootstrap completato con successo")
                    self.components['bootstrap'] = True
                else:
                    logger.error("❌ Bootstrap fallito")
                    logger.error(f"Errori: {bootstrap_result.get('errors', [])}")
                    return False
            else:
                logger.info("⏭️ Bootstrap saltato")
            
            # Step 2: Avvio ETL reattivo
            if auto_start_etl:
                logger.info("🔄 Step 2: Avvio ETL reattivo")
                reactive_etl_manager.is_running = True
                logger.info("✅ ETL reattivo avviato")
                self.components['etl'] = True
            else:
                logger.info("⏭️ ETL reattivo saltato")
            
            # Step 3: Avvio integrazione
            logger.info("🔗 Step 3: Avvio integrazione sistema")
            integration_result = await integration_manager.start_integrated_system()
            
            if integration_result:
                logger.info("✅ Integrazione sistema completata")
                self.components['integration'] = True
            else:
                logger.warning("⚠️ Integrazione sistema parziale")
            
            logger.info("=" * 60)
            logger.info("🎉 Sistema Process Mining avviato con successo!")
            logger.info(f"📊 Componenti attivi: {sum(self.components.values())}/{len(self.components)}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Errore nell'inizializzazione sistema: {e}")
            return False
    
    async def run_ui(self):
        """Avvia l'interfaccia utente."""
        try:
            logger.info("🖥️ Avvio interfaccia utente")
            ui_main()
        except Exception as e:
            logger.error(f"❌ Errore nell'avvio UI: {e}")
    
    async def run_etl_background(self):
        """Esegue l'ETL in background."""
        try:
            logger.info("🔄 Avvio ETL in background")
            await reactive_etl_manager.start_reactive_etl()
        except Exception as e:
            logger.error(f"❌ Errore ETL background: {e}")
    
    async def run_system_test(self) -> Dict[str, Any]:
        """Esegue il test completo del sistema."""
        try:
            logger.info("🧪 Esecuzione test sistema completo")
            test_result = await integration_manager.full_system_test()
            
            if test_result.get('success', False):
                logger.info("✅ Test sistema completato con successo")
            else:
                logger.warning("⚠️ Test sistema completato con errori")
            
            return test_result
            
        except Exception as e:
            logger.error(f"❌ Errore nel test sistema: {e}")
            return {'success': False, 'error': str(e)}
    
    async def shutdown_system(self):
        """Arresta il sistema in modo controllato."""
        try:
            logger.info("🛑 Arresto sistema Process Mining")
            
            self.is_running = False
            
            # Ferma ETL
            if self.components['etl']:
                logger.info("🔄 Arresto ETL reattivo")
                reactive_etl_manager.is_running = False
            
            # Ferma integrazione
            if self.components['integration']:
                logger.info("🔗 Arresto integrazione sistema")
                await integration_manager.stop_integrated_system()
            
            # Calcola tempo di esecuzione
            if self.start_time:
                runtime = time.time() - self.start_time
                logger.info(f"⏱️ Tempo di esecuzione: {runtime:.2f} secondi")
            
            logger.info("✅ Sistema arrestato correttamente")
            
        except Exception as e:
            logger.error(f"❌ Errore nell'arresto sistema: {e}")

def signal_handler(signum, frame):
    """Gestisce i segnali di interruzione."""
    logger.info(f"⏹️ Ricevuto segnale {signum}, arresto sistema...")
    # In un contesto reale, qui si chiamerebbe system.shutdown_system()
    sys.exit(0)

async def main():
    """Funzione principale."""
    # Configura gestione segnali
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Crea istanza sistema
    system = ProcessMiningSystem()
    
    # Leggi argomenti da linea di comando
    import argparse
    
    parser = argparse.ArgumentParser(description='Process Mining System')
    parser.add_argument('--mode', choices=['full', 'bootstrap', 'etl', 'ui', 'test'], 
                       default='full', help='Modalità di esecuzione')
    parser.add_argument('--no-bootstrap', action='store_true', 
                       help='Disabilita bootstrap automatico')
    parser.add_argument('--no-etl', action='store_true', 
                       help='Disabilita ETL automatico')
    parser.add_argument('--no-ui', action='store_true', 
                       help='Disabilita UI automatica')
    parser.add_argument('--test-only', action='store_true', 
                       help='Esegui solo test, poi esci')
    
    args = parser.parse_args()
    
    try:
        # Esegui bootstrap se richiesto
        if args.mode in ['full', 'bootstrap']:
            bootstrap_success = await system.initialize_system(
                auto_bootstrap=not args.no_bootstrap,
                auto_start_etl=False,  # Non avviare ETL in modalità bootstrap
                auto_start_ui=False    # Non avviare UI in modalità bootstrap
            )
            
            if not bootstrap_success:
                logger.error("Bootstrap fallito, impossibile continuare")
                return 1
        
        # Esegui test se richiesto
        if args.mode == 'test' or args.test_only:
            test_result = await system.run_system_test()
            
            if args.test_only:
                return 0 if test_result.get('success', False) else 1
        
        # Avvia sistema completo
        if args.mode == 'full':
            # Avvia inizializzazione completa
            init_success = await system.initialize_system(
                auto_bootstrap=not args.no_bootstrap,
                auto_start_etl=not args.no_etl,
                auto_start_ui=not args.no_ui
            )
            
            if not init_success:
                logger.error("Inizializzazione fallita")
                return 1
            
            # Avvia UI se richiesto
            if not args.no_ui:
                await system.run_ui()
        
        # Modalità ETL standalone
        elif args.mode == 'etl':
            if not args.no_bootstrap:
                await bootstrap_manager.bootstrap_system()
            
            await system.run_etl_background()
        
        # Modalità UI standalone
        elif args.mode == 'ui':
            if not args.no_bootstrap:
                await bootstrap_manager.bootstrap_system()
            
            await system.run_ui()
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("⏹️ Interruzione da tastiera ricevuta")
        await system.shutdown_system()
        return 0
    
    except Exception as e:
        logger.error(f"❌ Errore fatale: {e}")
        return 1

if __name__ == "__main__":
    # Esegui il main asincrono
    exit_code = asyncio.run(main())
    sys.exit(exit_code)