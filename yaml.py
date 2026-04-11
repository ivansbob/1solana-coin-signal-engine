import yaml as pyyaml
import sys

sys.setrecursionlimit(15000)

def safe_load(data):
    try:
        if isinstance(data, str):
            return pyyaml.safe_load(data)
        elif hasattr(data, 'read'):
            return pyyaml.safe_load(data.read())
        return pyyaml.safe_load(str(data))
    except:
        return None

def safe_dump(data, sort_keys=False, default_flow_style=False):
    try:
        return pyyaml.dump(data, default_flow_style=default_flow_style, sort_keys=sort_keys)
    except:
        return pyyaml.dump(data, default_flow_style=True)

load = safe_load
dump = safe_dump

def dump_config(data, path):
    try:
        with open(path, 'w', encoding='utf-8') as f:
            pyyaml.dump(data, f, default_flow_style=False)
    except Exception as e:
        print(f"[yaml] dump_config warning: {e}")

print("yaml.py → ultra-minimal version loaded")
