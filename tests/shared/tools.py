import logging
import json
from pathlib import Path

logger = logging.getLogger(__name__)

def load_secrets_file(path):
    """
    Load secrets from a JSON file.

    Args:
        path (str | Path): Path to a secrets file (must be JSON).
    Returns:
        Parsed secrets as a dictionary.
    """
    path = Path(path)
    if path.suffix != ".json": raise ValueError(f"unsupported secrets file type: {path.suffix}")
    with path.open("r", encoding="utf-8") as f: return json.load(f)

def get_api(secrets, dotted_path):
    """
    Retrieve API credentials from a nested secrets dictionary using a dot-separated path.

    Example: 
        get_api(secrets, "binance.api_key")
    Args:
        secrets (dict): Nested dictionary containing secret values.
        dotted_path (str): Dot-separated path to the desired value.
    Returns (dict):
        The dictionary located at the given path.
    """
    current = secrets
    for key in dotted_path.split('.'):
        try: current = current[key]
        except KeyError as e: raise KeyError(f"path '{dotted_path}' does not exist") from e
    return current
