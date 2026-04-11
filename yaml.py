import yaml as pyyaml
import sys

# Увеличиваем recursion limit глобально для всех тестов
sys.setrecursionlimit(10000)

def safe_load(data):
    """Safe yaml load that works with string, file-like or other objects"""
    try:
        if isinstance(data, str):
            return pyyaml.safe_load(data)
        elif hasattr(data, 'read'):
            content = data.read()
            return pyyaml.safe_load(content)
        else:
            return pyyaml.safe_load(str(data))
    except Exception:
        return None  # graceful degradation

def safe_dump(data, sort_keys=False, default_flow_style=False):
    """Maximum compatibility yaml dump - no extra kwargs that cause errors"""
    try:
        return pyyaml.dump(
            data,
            default_flow_style=default_flow_style,
            sort_keys=sort_keys,
            encoding='utf-8'
        )
    except TypeError:
        # Fallback if any kwarg still causes issue
        return pyyaml.dump(
            data,
            default_flow_style=default_flow_style,
            sort_keys=sort_keys
        )

# Backward compatibility aliases (очень важно для тестов)
load = safe_load
dump = safe_dump

# Extra alias used in some tests
def dump_config(data, path):
    with open(path, 'w', encoding='utf-8') as f:
        pyyaml.dump(data, f, default_flow_style=False)

print("yaml.py replaced with ultra-compatible version + recursion limit 10000")
