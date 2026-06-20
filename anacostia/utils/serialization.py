import pickle
import json


def bytes_to_str(b: bytes) -> str:
    """
    Convert bytes to string using UTF-8 encoding.
    """
    return b.decode("utf-8")

def bytes_to_json(b: bytes) -> dict:
    """
    Convert bytes to JSON using UTF-8 encoding.
    """
    return json.loads(bytes_to_str(b))

def bytes_to_pickle(b: bytes) -> object:
    """
    Convert bytes to a Python object using pickle.
    """
    return pickle.loads(b)

def bytes_to_csv(b: bytes) -> str:
    """
    Convert bytes to CSV string using UTF-8 encoding.
    """
    return bytes_to_str(b)