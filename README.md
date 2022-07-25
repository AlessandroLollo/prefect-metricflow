# prefect-metricflow

## Welcome!

Collection of tasks to interact with MetricFlow

## Getting Started

### Python setup

Requires an installation of Python 3.8 or 3.9.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-metricflow` with `pip`:

```bash
pip install prefect-metricflow
```

### Write and run a flow

```python
from prefect import flow
from prefect_metricflow.tasks import (
    materialize
)


@flow
def create_materialization_with_metricflow():
    return materialize(
        materialization_name="my_materialization",
        config={
            "dwh_dialect": "redshift",
            "dwh_host": "host",
            "dwh_port": 5439,
            "dwh_user": "dw_user",
            "dwh_password": "dw_pwd",
            "dwh_database": "dw_db",
            "dwh_schema": "dw_schema",
            "model_path": "path/to/models",
        }
    )

create_materialization_with_metricflow()


@flow
def drop_materialization_with_metricflow():
    return drop_materialization(
        materialization_name="my_materialization",
        config={
            "dwh_dialect": "redshift",
            "dwh_host": "host",
            "dwh_port": 5439,
            "dwh_user": "dw_user",
            "dwh_password": "dw_pwd",
            "dwh_database": "dw_db",
            "dwh_schema": "dw_schema",
            "model_path": "path/to/models",
        }
    )

drop_materialization_with_metricflow()
```

## Resources

If you encounter any bugs while using `prefect-metricflow`, feel free to open an issue in the [prefect-metricflow](https://github.com/AlessandroLollo/prefect-metricflow) repository.

If you have any questions or issues while using `prefect-metricflow`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

## Development

If you'd like to install a version of `prefect-metricflow` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/AlessandroLollo/prefect-metricflow.git

cd prefect-metricflow/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
