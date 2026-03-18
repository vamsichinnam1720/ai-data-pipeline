"""Alert system"""
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum
from .logger import logger

class AlertSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"

class AlertType(Enum):
    ANOMALY_DETECTED = "anomaly_detected"
    API_FAILURE = "api_failure"

class Alerter:
    def __init__(self):
        self.enabled = True
        self.alert_history = []
    
    def anomaly_alert(self, anomaly_count: int, anomaly_types: Dict[str, int], auto_fixed: int):
        logger.warning(f"Detected {anomaly_count} anomalies ({auto_fixed} auto-fixed)")

alerter = Alerter()
