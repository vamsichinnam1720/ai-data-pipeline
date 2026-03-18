"""
Data transformation module
"""

import pandas as pd
import numpy as np
from typing import List, Optional
from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder

from src.monitoring.logger import logger


class DataTransformer:
    """Data transformation and feature engineering"""
    
    def __init__(self):
        """Initialize transformer"""
        self.scalers = {}
        self.encoders = {}
    
    def normalize_numeric(self, df: pd.DataFrame, columns: Optional[List[str]] = None,
                         method: str = 'standard') -> pd.DataFrame:
        """
        Normalize numeric columns
        
        Args:
            df: Input DataFrame
            columns: Columns to normalize (None = all numeric)
            method: 'standard' or 'minmax'
            
        Returns:
            DataFrame with normalized columns
        """
        df_transformed = df.copy()
        
        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()
        
        for col in columns:
            if col not in df.columns:
                continue
            
            if method == 'standard':
                scaler = StandardScaler()
            else:
                scaler = MinMaxScaler()
            
            df_transformed[col] = scaler.fit_transform(df[[col]])
            self.scalers[col] = scaler
            
            logger.debug(f"Normalized column '{col}' using {method} scaling")
        
        return df_transformed
    
    def encode_categorical(self, df: pd.DataFrame, 
                          columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Encode categorical variables
        
        Args:
            df: Input DataFrame
            columns: Columns to encode (None = all object types)
            
        Returns:
            DataFrame with encoded columns
        """
        df_transformed = df.copy()
        
        if columns is None:
            columns = df.select_dtypes(include=['object']).columns.tolist()
        
        for col in columns:
            if col not in df.columns:
                continue
            
            encoder = LabelEncoder()
            df_transformed[f'{col}_encoded'] = encoder.fit_transform(df[col].astype(str))
            self.encoders[col] = encoder
            
            logger.debug(f"Encoded column '{col}'")
        
        return df_transformed
    
    def create_time_features(self, df: pd.DataFrame, 
                           date_column: str) -> pd.DataFrame:
        """
        Create time-based features from datetime column
        
        Args:
            df: Input DataFrame
            date_column: Name of datetime column
            
        Returns:
            DataFrame with additional time features
        """
        if date_column not in df.columns:
            logger.warning(f"Column '{date_column}' not found")
            return df
        
        df_transformed = df.copy()
        
        # Ensure datetime type
        df_transformed[date_column] = pd.to_datetime(df_transformed[date_column])
        
        # Extract features
        df_transformed[f'{date_column}_year'] = df_transformed[date_column].dt.year
        df_transformed[f'{date_column}_month'] = df_transformed[date_column].dt.month
        df_transformed[f'{date_column}_day'] = df_transformed[date_column].dt.day
        df_transformed[f'{date_column}_dayofweek'] = df_transformed[date_column].dt.dayofweek
        df_transformed[f'{date_column}_quarter'] = df_transformed[date_column].dt.quarter
        df_transformed[f'{date_column}_weekofyear'] = df_transformed[date_column].dt.isocalendar().week
        
        logger.info(f"Created time features from '{date_column}'")
        
        return df_transformed
    
    def create_bins(self, df: pd.DataFrame, column: str, 
                   bins: int = 5, labels: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Create binned categories from numeric column
        
        Args:
            df: Input DataFrame
            column: Column to bin
            bins: Number of bins
            labels: Bin labels
            
        Returns:
            DataFrame with binned column
        """
        df_transformed = df.copy()
        
        if column not in df.columns:
            logger.warning(f"Column '{column}' not found")
            return df
        
        df_transformed[f'{column}_binned'] = pd.cut(
            df[column], 
            bins=bins, 
            labels=labels
        )
        
        logger.info(f"Created {bins} bins for column '{column}'")
        
        return df_transformed
    
    def aggregate_by_group(self, df: pd.DataFrame, 
                          group_by: str, 
                          agg_dict: dict) -> pd.DataFrame:
        """
        Perform group-by aggregation
        
        Args:
            df: Input DataFrame
            group_by: Column to group by
            agg_dict: Aggregation dictionary
            
        Returns:
            Aggregated DataFrame
        """
        try:
            result = df.groupby(group_by).agg(agg_dict).reset_index()
            logger.info(f"Aggregated by '{group_by}'")
            return result
        except Exception as e:
            logger.error(f"Aggregation failed: {e}")
            return df