import yaml
import logging
from typing import List, Any

class DataGatherer:
    def __init__(self, **params):
        self.params = params

class DataGathererRegistry:
    def __init__(self, gatherers: List[DataGatherer]):
        self.gatherers = gatherers

    @staticmethod
    def load(path: str) -> "DataGathererRegistry":
        import os
        
        # 1. Защита от отсутствующего файла (Fresh Clone Protection)
        if not os.path.exists(path):
            logging.warning(f"Gatherer config not found at {path}. Returning empty registry.")
            return DataGathererRegistry(gatherers=[])
            
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
                
            items = cfg.get("gatherers", [])
            out: List[DataGatherer] = []
            
            for item in items:
                gtype = item.get("type")
                params = item.get("params", {}) or {}
                cls = _TYPE_MAP.get(gtype)
                
                if not cls:
                    # 2. Мягкий пропуск вместо жесткого падения (Fail-Open)
                    logging.warning(f"Unknown gatherer type skipped: {gtype}")
                    continue
                    
                out.append(cls(**params))
                
            return DataGathererRegistry(gatherers=out)
            
        except Exception as e:
            logging.error(f"Failed to load gatherer registry from {path}: {e}")
            return DataGathererRegistry(gatherers=[])

_TYPE_MAP = {
    # Placeholder for types
}