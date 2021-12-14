import json
import traceback


def load_config(config_path: str) -> dict:
    """
    Configuration in JSON form. To be used by the helper functions for formats and directories paths.
    :param config_path:
    :return:
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as fin:
            return json.load(fin)
    except FileNotFoundError as e:
        traceback.print_exc()
        raise Exception(f"Failed to load config file from {config_path}: {str(e)}")


def proxy_to_jobdate(date: str, default_value:str=None) -> str:
    new_date = date[2:4] + date[5:7] + date[8:10]
    if new_date > '0901001':
        return default_value
    return new_date
