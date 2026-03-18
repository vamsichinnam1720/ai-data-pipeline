"""Query executor"""
import pandas as pd
from src.monitoring.logger import logger

class QueryExecutor:
    def __init__(self, df: pd.DataFrame):
        self.df = df
    
    def execute(self, parsed_query: dict):
        result = self.df.copy()
        
        if parsed_query.get('group_by') and parsed_query.get('operation') == 'aggregate':
            result = self._apply_aggregation(
                result, 
                parsed_query['group_by'],
                parsed_query.get('aggregation_type', 'sum'),
                parsed_query.get('columns', [])
            )
        elif parsed_query.get('columns'):
            valid_cols = [c for c in parsed_query['columns'] if c in result.columns]
            if valid_cols:
                result = result[valid_cols]
        
        logger.info(f"Query returned {len(result)} rows")
        return result
    
    def _apply_aggregation(self, df, group_col: str, agg_type: str, agg_cols: list):
        if group_col not in df.columns:
            return df
        
        agg_func_map = {'sum': 'sum', 'total': 'sum', 'average': 'mean', 'mean': 'mean', 
                       'count': 'count', 'max': 'max', 'min': 'min'}
        agg_func = agg_func_map.get(agg_type, 'sum')
        
        if not agg_cols:
            agg_cols = df.select_dtypes(include=['number']).columns.tolist()
            agg_cols = [c for c in agg_cols if c != group_col]
        
        if not agg_cols:
            return df.groupby(group_col).size().reset_index(name='count')
        
        result = df.groupby(group_col)[agg_cols].agg(agg_func).reset_index()
        return result
