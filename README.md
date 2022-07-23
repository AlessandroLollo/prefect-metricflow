# prefect-metricflow

## Welcome!

Collection of tasks to interact with MetricFlow

## Getting Started

### Python setup

Requires an installation of Python 3.7+.

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
    goodbye_prefect_metricflow,
    hello_prefect_metricflow,
)


@flow
def example_flow():
    hello_prefect_metricflow
    goodbye_prefect_metricflow

example_flow()
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
