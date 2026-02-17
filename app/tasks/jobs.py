@celery_app.task(bind=True)
def run_full_analysis(self, pipeline_id):
    # 1. Chiama HubSpot Connector
    raw_data = hubspot.get_data(...)
    self.update_state(state='PROGRESS', meta={'msg': 'Dati scaricati'})
    
    # 2. Pulisce i dati
    df = etl_service.process(raw_data)
    
    # 3. Fa Mining
    graph = mining_service.discover(df)
    
    # 4. Salva su DuckDB
    save_to_db(graph)
    return "Done"