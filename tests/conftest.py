import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONSUMER_DIR = ROOT / "services" / "consumer"
SERVICES_DIR = ROOT / "services"

for path in (ROOT, CONSUMER_DIR, SERVICES_DIR):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)
