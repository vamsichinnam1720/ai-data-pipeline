"""Fallback manager"""
from src.monitoring.logger import logger
from src.monitoring.alerter import alerter
from .csv_loader import CSVLoader
from .api_connector import APIConnector

class FallbackManager:
    def __init__(self):
        self.csv_loader = CSVLoader()
        self.api_connector = APIConnector()
        self.current_source = 'csv'
        self.fallback_active = False
    
    def get_data(self, mode: str = 'csv', **kwargs):
        if mode == 'api':
            return self._get_with_api_fallback(**kwargs)
        else:
            return self._get_from_csv(**kwargs)
    
    def _get_with_api_fallback(self, **kwargs):
        api_type = kwargs.get('api_type', 'weather')
        
        if api_type == 'weather':
            df = self.api_connector.fetch_weather_data(kwargs.get('city', 'London'))
        else:
            df = None
        
        if df is None:
            logger.warning("API failed, switching to CSV backup")
            self.fallback_active = True
            self.current_source = 'csv'
            df = self.csv_loader.get_backup()
            if df is not None:
                logger.success("Fallback successful")
            else:
                logger.error("No backup available")
        else:
            self.current_source = 'api'
            self.fallback_active = False
        
        return df
    
    def _get_from_csv(self, **kwargs):
        filepath = kwargs.get('filepath')
        if filepath:
            return self.csv_loader.load_csv(filepath)
        return None
