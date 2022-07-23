"""
Utils to build a MetricFlow configuration file
"""
from typing import Dict, Optional, Union

from metricflow.configuration.config_handler import ConfigHandler
from yaml import YAMLError, dump, safe_load

from prefect_metricflow.exceptions import MetricFlowFailureException


def get_config_file_path(config_file_path: Optional[str] = None) -> str:
    """
    TODO
    """
    return config_file_path or ConfigHandler().file_path


def persist_config(config: Union[Dict, str], file_path: str) -> None:
    """
    TODO
    """

    if isinstance(config, dict):
        mf_config = config
    elif isinstance(config, str):
        try:
            mf_config = safe_load(config)
        except YAMLError as e:
            msg = f"Error while parsing provided MetricFlow config string: {e}"
            raise MetricFlowFailureException(msg)

    with open(file_path, "w") as config_file:
        dump(mf_config, config_file)
