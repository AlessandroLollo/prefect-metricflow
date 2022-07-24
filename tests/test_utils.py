import os
from unittest.mock import patch

import pytest
from yaml import safe_load

from prefect_metricflow.exceptions import MetricFlowFailureException
from prefect_metricflow.utils import get_config_file_path, persist_config


def test_invalid_yaml_builder_raises(fs):
    doc = """
    root:
    breaking_node
    """
    file_path = "mf_config_dir/config.yaml"
    fs.create_file(file_path)

    msg_match = "Error while parsing provided MetricFlow config string"
    with pytest.raises(MetricFlowFailureException, match=msg_match):
        persist_config(config=doc, file_path=file_path)


@patch.dict(os.environ, {"MF_CONFIG_DIR": "mf_config_dir"})
def test_builder_persist_yaml_config_on_default_file(fs):
    doc = """
    root:
        leaf: foo
    """
    fs.create_file(f'{os.environ["MF_CONFIG_DIR"]}/config.yaml')
    file_path = get_config_file_path()

    persist_config(config=doc, file_path=file_path)

    assert os.path.isfile(file_path)

    with open(file_path, "r") as config_file:
        config = safe_load(config_file)

    config_doc = safe_load(doc)

    assert config == config_doc


def test_builder_persist_yaml_config_on_file(fs):
    doc = """
    root:
        leaf: foo
    """
    fs.create_file("mf_config_dir/config.yaml")
    file_path = get_config_file_path(config_file_path="mf_config_dir/config.yaml")

    persist_config(config=doc, file_path=file_path)

    assert os.path.isfile(file_path)

    with open(file_path, "r") as config_file:
        config = safe_load(config_file)

    config_doc = safe_load(doc)

    assert config == config_doc


@patch.dict(os.environ, {"MF_CONFIG_DIR": "mf_config_dir"})
def test_builder_persist_config_on_default_file(fs):
    doc = {"root": {"leaf": "foo"}}

    fs.create_file(f'{os.environ["MF_CONFIG_DIR"]}/config.yaml')
    file_path = get_config_file_path()

    persist_config(config=doc, file_path=file_path)

    assert os.path.isfile(file_path)

    with open(file_path, "r") as config_file:
        config = safe_load(config_file)

    assert config == doc


def test_builder_persist_config_on_file(fs):
    doc = {"root": {"leaf": "foo"}}

    fs.create_file("mf_config_dir/config.yaml")
    file_path = get_config_file_path(config_file_path="mf_config_dir/config.yaml")

    persist_config(config=doc, file_path=file_path)

    assert os.path.isfile(file_path)

    with open(file_path, "r") as config_file:
        config = safe_load(config_file)

    assert config == doc
