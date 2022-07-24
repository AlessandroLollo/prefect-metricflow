import os
from typing import Dict, Optional, Union
from unittest import mock

import pytest
from metricflow.dataflow.sql_table import SqlTable
from prefect import flow

from prefect_metricflow.exceptions import MetricFlowFailureException
from prefect_metricflow.tasks import materialize


@mock.patch.dict(os.environ, {"MF_CONFIG_DIR": "/tmp/mf_config_dir"})
@mock.patch("metricflow.api.metricflow_client.MetricFlowClient")
def test_materialize_success_with_config(mf_client_mock):
    class MetricFlowClientMock:
        def materialize(
            materialization_name: str,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            config: Optional[Union[Dict, str]] = None,
            config_file_path: Optional[str] = None,
        ) -> SqlTable:

            return SqlTable(db_name="foo", schema_name="foo", table_name="foo")

    mf_client_mock.return_value = MetricFlowClientMock

    @flow(name="test_flow_1")
    def test_flow():
        return materialize(
            materialization_name="foo",
            config={
                "dwh_dialect": "redshift",
                "dwh_host": "localhost",
                "dwh_port": 5439,
                "dwh_user": "foo",
                "dwh_password": "foo",
                "dwh_database": "db",
                "dwh_schema": "foo",
                "model_path": "foo",
            },
        )

    response = test_flow()

    assert response == SqlTable(db_name="foo", schema_name="foo", table_name="foo")


@mock.patch.dict(os.environ, {"MF_CONFIG_DIR": "/tmp/mf_config_dir"})
@mock.patch("metricflow.api.metricflow_client.MetricFlowClient")
def test_materialize_success_with_yaml_config(mf_client_mock):
    class MetricFlowClientMock:
        def materialize(
            materialization_name: str,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            config: Optional[Union[Dict, str]] = None,
            config_file_path: Optional[str] = None,
        ) -> SqlTable:

            return SqlTable(db_name="foo", schema_name="foo", table_name="foo")

    mf_client_mock.return_value = MetricFlowClientMock

    @flow(name="test_flow_2")
    def test_flow():
        return materialize(
            materialization_name="foo",
            config="""
            dwh_dialect: redshift
            dwh_host: 'localhost'
            dwh_port: 5439
            dwh_user: 'foo'
            dwh_password: 'foo'
            dwh_database: 'db'
            dwh_schema: 'foo'
            model_path: 'foo'
            """,
        )

    response = test_flow()

    assert response == SqlTable(db_name="foo", schema_name="foo", table_name="foo")


@mock.patch.dict(os.environ, {"MF_CONFIG_DIR": "/tmp/mf_config_dir"})
@mock.patch("metricflow.api.metricflow_client.MetricFlowClient")
def test_materialize_failure(mf_client_mock):
    class MetricFlowClientMock:
        def materialize(
            materialization_name: str,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            config: Optional[Union[Dict, str]] = None,
            config_file_path: Optional[str] = None,
        ) -> SqlTable:

            return SqlTable(db_name="foo", schema_name="foo", table_name="foo")

    mf_client_mock.return_value = MetricFlowClientMock

    msg = "Cannot build materialization!"
    mf_client_mock.side_effect = MetricFlowFailureException(msg)

    @flow(name="test_flow_3")
    def test_flow():
        return materialize(
            materialization_name="foo",
            config={
                "dwh_dialect": "redshift",
                "dwh_host": "localhost",
                "dwh_port": 5439,
                "dwh_user": "foo",
                "dwh_password": "foo",
                "dwh_database": "db",
                "dwh_schema": "foo",
                "model_path": "foo",
            },
        )

    with pytest.raises(MetricFlowFailureException, match=msg):
        test_flow()
