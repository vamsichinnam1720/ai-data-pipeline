"""Anomaly fixer"""
import pandas as pd
import numpy as np
from src.monitoring.logger import logger
from src.monitoring.alerter import alerter

class AnomalyFixer:
    def __init__(self):
        self.fix_log = []
    
    def fix_anomalies(self, df: pd.DataFrame, anomaly_flags):
        logger.info("Auto-fixing anomalies...")
        df_fixed = df.copy()
        fixed_count = 0
        
        df_fixed, outlier_fixes = self._fix_outliers(df_fixed, anomaly_flags)
        fixed_count += outlier_fixes
        
        logger.success(f"Auto-fixed {fixed_count} anomalies")
        
        if anomaly_flags.sum() > 0:
            alerter.anomaly_alert(anomaly_flags.sum(), {'outliers': outlier_fixes}, fixed_count)
        
        return df_fixed
    
    def _fix_outliers(self, df, anomaly_flags):
        df_fixed = df.copy()
        fixed = 0
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            outlier_idx = anomaly_flags & df[col].notna()
            if outlier_idx.sum() == 0:
                continue
            
            lower = df[col].quantile(0.01)
            upper = df[col].quantile(0.99)
            
            original = df.loc[outlier_idx, col].copy()
            df_fixed.loc[outlier_idx, col] = df_fixed.loc[outlier_idx, col].clip(lower, upper)
            
            fixes = (original != df_fixed.loc[outlier_idx, col]).sum()
            fixed += fixes
            
            if fixes > 0:
                logger.debug(f"Fixed {fixes} outliers in '{col}'")
        
        return df_fixed, fixed
