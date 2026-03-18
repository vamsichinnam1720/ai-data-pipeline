"""API connector"""
import requests
import pandas as pd
import time
from config.config import Config
from src.monitoring.logger import logger

class APIConnector:
    def __init__(self):
        self.api_config = Config.load_api_config()
    
    def fetch_weather_data(self, city: str, api_key=None):
        api_key = api_key or Config.WEATHER_API_KEY
        if not api_key:
            logger.error("Weather API key not configured")
            return None
        
        config = self.api_config.get('weather', {}).get('openweathermap', {})
        url = config.get('base_url')
        params = {'q': city, 'appid': api_key, 'units': 'metric'}
        
        return self._make_request('OpenWeatherMap', url, params)
    
    def _make_request(self, api_name: str, url: str, params: dict):
        for attempt in range(Config.API_RETRY_ATTEMPTS):
            try:
                logger.info(f"API call to {api_name} (attempt {attempt + 1})")
                start = time.time()
                response = requests.get(url, params=params, timeout=Config.API_TIMEOUT)
                elapsed = time.time() - start
                
                if response.status_code == 200:
                    logger.success(f"API call successful ({elapsed:.2f}s)")
                    return pd.DataFrame([response.json()])
                else:
                    logger.warning(f"API returned {response.status_code}")
            except Exception as e:
                logger.error(f"API call failed: {e}")
        return None
