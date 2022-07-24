"""
Utils to build a MetricFlow configuration file
"""
from typing import Dict, Optional, Union

from metricflow.configuration.config_handler import ConfigHandler
from yaml import YAMLError, dump, safe_load

from prefect_metricflow.exceptions import MetricFlowFailureException


def get_config_file_path(config_file_path: Optional[str] = None) -> str:
    """
    Returns MetricFlow config file path.

    Args:
        config_file_path: The absolute path of MetricFlow config file.

    Returns:
        The absolute path of the configuration file.
        The default path is returned if `config_file_path` is not provided.
    """
    return config_file_path or ConfigHandler().file_path


def persist_config(config: Union[Dict, str], file_path: str) -> None:
    """
    Persist the MetricFlow configuration on the file system.

    Args:
        config: Either a `dict` or a valid YAML string describing
            the MetricFlow configuration.
        file_path: Absolute path of the file where the configuration will be persisted.
    """

    if isinstance(config, dict):
        mf_config = config

    # If the config is a string, try parsing as YAML
    elif isinstance(config, str):
        try:
            mf_config = safe_load(config)
        except YAMLError as e:
            msg = f"Error while parsing provided MetricFlow config string: {e}"
            raise MetricFlowFailureException(msg)

    # Persist the configuration on the file system
    with open(file_path, "w") as config_file:
        dump(mf_config, config_file)
