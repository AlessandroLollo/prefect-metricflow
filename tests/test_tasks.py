from prefect import flow

from prefect_metricflow.tasks import (
    goodbye_prefect_metricflow,
    hello_prefect_metricflow,
)


def test_hello_prefect_metricflow():
    @flow
    def test_flow():
        return hello_prefect_metricflow()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Hello, prefect-metricflow!"


def goodbye_hello_prefect_metricflow():
    @flow
    def test_flow():
        return goodbye_prefect_metricflow()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Goodbye, prefect-metricflow!"
