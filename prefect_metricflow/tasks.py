"""
Collections of tasks to interact with MetricFlow
"""
from typing import Dict, Optional, Union

from metricflow.api.metricflow_client import MetricFlowClient
from metricflow.dataflow.sql_table import SqlTable
from prefect import task

from prefect_metricflow.utils import get_config_file_path, persist_config


@task
def materialize(
    materialization_name: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    config: Optional[Union[Dict, str]] = None,
    config_file_path: Optional[str] = None,
) -> SqlTable:
    """
    Materialize metrics on the target DWH.

    Args:
        materialization_name: The name of the materialization to be created.
        start_time: The start time range to be used to build the materialization.
        end_time: The end time range to be used to build the materialization.
        config: MetricFlow configuration. Can be either a `dict` or a YAML string.
            If provided, will be persisted at the path specified in `config_file_path`.
        config_file_path: Path to MetricFlow config file.
            If not provided, the default path will be used.

    Raises:
        `MetricFlowFailureException` if `config` is not a valid YAML string.

    Returns:
        a SqlTable with references to the newly created materialization.
    """

    mf_config_file_path = get_config_file_path(config_file_path=config_file_path)

    # If a config is provided, try to use it.
    if config:

        persist_config(config=config, file_path=mf_config_file_path)

    # Create MetricFlow client
    mfc = MetricFlowClient.from_config(config_file_path=mf_config_file_path)

    # Build materialization and return result
    return mfc.materialize(
        materialization_name=materialization_name,
        start_time=start_time,
        end_time=end_time,
    )


@task
def drop_materialization(
    materialization_name: str,
    config: Optional[Union[Dict, str]] = None,
    config_file_path: Optional[str] = None,
) -> bool:
    """
    Drop a materialization that was previously created by MetricFlow.

    Args:
        materialization_name: The name of the materialization to drop.
        config: MetricFlow configuration. Can be either a `dict` or a YAML string.
            If provided, will be persisted at the path specified in `config_file_path`.
        config_file_path: Path to MetricFlow config file.
            If not provided, the default path will be used.

    Returns:
        `True` if MetricFlow has successfully dropped the materialization table,
        `False` if the materialization table does not exist.
    """

    mf_config_file_path = get_config_file_path(config_file_path=config_file_path)

    # If a config is provided, try to use it.
    if config:

        persist_config(config=config, file_path=mf_config_file_path)

    # Create MetricFlow client
    mfc = MetricFlowClient.from_config(config_file_path=mf_config_file_path)

    # Build materialization and return result
    return mfc.drop_materialization(materialization_name=materialization_name)
