import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from app.core.logging import logger

class HubSpotClient:
    def __init__(self, api_key: str):
        self.base_url = "https://api.hubapi.com/crm/v3/objects/deals"
        self.headers = {"Authorization": f"Bearer {api_key}"}

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
    def get_deals_with_history(self, limit=100, after=None):
        """
        Fetches deals INCLUDING the history of specific properties.
        Crucial for Process Mining: We need 'dealstage' history.
        """
        properties_to_fetch = ["dealname", "amount", "dealstage", "closedate"]
        
        # We must ask HubSpot specifically for the history of 'dealstage'
        params = {
            "limit": limit,
            "properties": properties_to_fetch,
            "propertiesWithHistory": ["dealstage"] # <--- THIS IS THE MAGIC KEY
        }
        if after:
            params["after"] = after

        response = requests.get(self.base_url, headers=self.headers, params=params)
        
        if response.status_code == 429:
            logger.warning("HubSpot Rate Limit hit! Tenacity will retry automatically.")
            raise Exception("Rate Limit")
            
        return response.json()

    def process_deal_history(self, deal_json):
        """
        Transforms nested HubSpot history into a linear Event Log.
        """
        events = []
        deal_id = deal_json['id']
        
        # 1. Get the history of stage changes
        history = deal_json.get('propertiesWithHistory', {}).get('dealstage', [])
        
        for record in history:
            # Each record is an 'Activity' in Process Mining
            events.append({
                "Case ID": deal_id,
                "Activity": f"Move to Stage: {record['value']}", # e.g., "Move to Qualified"
                "Timestamp": record['timestamp'], # HubSpot gives this in ISO format
                "Resource": record.get('sourceId', 'System') # Who did it?
            })
            
        return events