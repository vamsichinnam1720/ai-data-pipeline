"""Query parser"""
import re
from fuzzywuzzy import process
from src.monitoring.logger import logger

class QueryParser:
    AGGREGATIONS = ['sum', 'total', 'count', 'average', 'mean', 'max', 'min']
    
    def __init__(self, df_columns):
        self.columns = df_columns
    
    def parse(self, query: str):
        query_lower = query.lower()
        parsed = {'original': query, 'operation': 'select', 'columns': [], 'group_by': None}
        
        for agg in self.AGGREGATIONS:
            if agg in query_lower:
                parsed['operation'] = 'aggregate'
                parsed['aggregation_type'] = agg
                break
        
        words = query_lower.split()
        for word in words:
            clean = re.sub(r'[^a-z0-9_]', '', word)
            if len(clean) < 3:
                continue
            match = process.extractOne(clean, self.columns, score_cutoff=70)
            if match and match[0] not in parsed['columns']:
                parsed['columns'].append(match[0])
        
        for keyword in ['by', 'per']:
            if keyword in query_lower:
                idx = query_lower.find(keyword)
                rest = query_lower[idx + len(keyword):].strip().split()
                if rest:
                    match = process.extractOne(rest[0], self.columns, score_cutoff=70)
                    if match:
                        parsed['group_by'] = match[0]
                        break
        
        logger.info(f"Parsed query: {parsed}")
        return parsed
