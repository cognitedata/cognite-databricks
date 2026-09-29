"""Runtime ``base_url`` flows TOML -> Secret Manager -> view SQL -> UDTF signature.

Private Link / dedicated deployments set ``base_url`` in the TOML. It is stored in Secret Manager as
``base_url`` (defaulting to the public cluster URL), passed by every generated view / query SQL, and
declared as the trailing parameter of every registered UDTF.
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.databricks.data_model_query_rewriter import DataModelQueryRewriter
from cognite.databricks.generator import (
    UDTFGenerator,
    generate_time_series_sql_view,
    generate_time_series_udtf_view_sql,
    generate_udtf_sql_query,
)
from cognite.databricks.models import time_series_udtf_registry
from cognite.databricks.secret_manager import SecretManagerHelper

SECRET_SCOPE = "cdf_sailboat_sailboat"
BASE_URL_ARG = f"base_url => SECRET('{SECRET_SCOPE}', 'base_url')"


def _named_args(sql: str) -> list[str]:
    return re.findall(r"(\w+)\s*=>", sql)


def _stored_secrets(workspace_client: MagicMock) -> dict[str, str]:
    return {call.args[1]: call.kwargs["string_value"] for call in workspace_client.secrets.put_secret.call_args_list}


@pytest.fixture
def secret_workspace_client() -> MagicMock:
    client = MagicMock()
    scope = MagicMock()
    scope.name = SECRET_SCOPE
    client.secrets.list_scopes.return_value = [scope]
    return client


@pytest.fixture
def generator(
    mock_workspace_client: MagicMock,
    mock_cognite_client: CogniteClient,
    temp_output_dir: Path,
) -> UDTFGenerator:
    from cognite.pygen_spark import SparkUDTFGenerator

    view = dm.View(
        space="sailboat",
        external_id="SmallBoat",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name="",
        description="",
        properties={"name": dm.Text()},  # type: ignore[dict-item]
        filter=None,
        implements=None,
        writable=False,
        used_for="node",
        is_global=False,
    )
    model = dm.DataModel(
        space="sailboat",
        external_id="sailboat",
        version="v1",
        created_time=1,
        last_updated_time=2,
        name=None,
        description=None,
        is_global=False,
        views=[view],
    )
    code_generator = SparkUDTFGenerator(client=mock_cognite_client, output_dir=temp_output_dir, data_model=model)
    return UDTFGenerator(
        workspace_client=mock_workspace_client,
        cognite_client=mock_cognite_client,
        catalog="f0connectortest",
        schema="sailboat_sailboat_v1",
        code_generator=code_generator,
    )


def test_set_cdf_credentials_stores_private_link_base_url(secret_workspace_client: MagicMock) -> None:
    SecretManagerHelper(secret_workspace_client).set_cdf_credentials(
        scope_name=SECRET_SCOPE,
        project="cognite-cogsail",
        cdf_cluster="az-xyz-001",
        client_id="id",
        client_secret="secret",
        tenant_id="tenant",
        base_url=" https://p001.plink.az-xyz-001.cognitedata.com/ ",
    )
    assert _stored_secrets(secret_workspace_client)["base_url"] == "https://p001.plink.az-xyz-001.cognitedata.com"


def test_set_cdf_credentials_defaults_base_url_to_public_cluster_url(secret_workspace_client: MagicMock) -> None:
    SecretManagerHelper(secret_workspace_client).set_cdf_credentials(
        scope_name=SECRET_SCOPE,
        project="cognite-cogsail",
        cdf_cluster="westeurope-1",
        client_id="id",
        client_secret="secret",
        tenant_id="tenant",
    )
    assert _stored_secrets(secret_workspace_client)["base_url"] == "https://westeurope-1.cognitedata.com"


def _scope_with_keys(workspace_client: MagicMock, *keys: str) -> None:
    metadata = []
    for key in keys:
        item = MagicMock()
        item.key = key
        metadata.append(item)
    workspace_client.secrets.list_secrets.return_value = metadata


def test_ensure_base_url_backfills_missing_key(secret_workspace_client: MagicMock) -> None:
    _scope_with_keys(secret_workspace_client, "client_id", "cdf_cluster")

    written = SecretManagerHelper(secret_workspace_client).ensure_base_url(
        SECRET_SCOPE, "https://p001.plink.az-xyz-001.cognitedata.com/"
    )

    assert written is True
    assert _stored_secrets(secret_workspace_client) == {"base_url": "https://p001.plink.az-xyz-001.cognitedata.com"}


def test_ensure_base_url_never_overwrites_existing_key(secret_workspace_client: MagicMock) -> None:
    _scope_with_keys(secret_workspace_client, "client_id", "base_url")

    written = SecretManagerHelper(secret_workspace_client).ensure_base_url(
        SECRET_SCOPE, "https://westeurope-1.cognitedata.com"
    )

    assert written is False
    secret_workspace_client.secrets.put_secret.assert_not_called()


def test_generator_backfills_base_url_from_toml_loaded_client(
    generator: UDTFGenerator, secret_workspace_client: MagicMock
) -> None:
    _scope_with_keys(secret_workspace_client, "client_id")
    generator.secret_helper = SecretManagerHelper(secret_workspace_client)
    generator.cognite_client = MagicMock()
    generator.cognite_client.config.base_url = "https://p001.plink.az-xyz-001.cognitedata.com"

    generator._ensure_base_url_secret(SECRET_SCOPE)

    assert _stored_secrets(secret_workspace_client) == {"base_url": "https://p001.plink.az-xyz-001.cognitedata.com"}


def test_generator_backfill_without_client_url_fails_with_guidance(
    generator: UDTFGenerator, secret_workspace_client: MagicMock
) -> None:
    _scope_with_keys(secret_workspace_client, "client_id")
    generator.secret_helper = SecretManagerHelper(secret_workspace_client)
    generator.cognite_client = None  # type: ignore[assignment]

    with pytest.raises(ValueError, match="set_cdf_credentials"):
        generator._ensure_base_url_secret(SECRET_SCOPE)


class _Stop(Exception):
    pass


@pytest.mark.parametrize("method_name", ["register_udtfs", "register_views"])
def test_registration_ensures_base_url_secret_first(
    generator: UDTFGenerator, monkeypatch: pytest.MonkeyPatch, method_name: str
) -> None:
    ensured: list[str] = []

    def _ensure(secret_scope: str) -> None:
        ensured.append(secret_scope)
        raise _Stop

    monkeypatch.setattr(generator, "_ensure_base_url_secret", _ensure)

    with pytest.raises(_Stop):
        getattr(generator, method_name)(secret_scope=SECRET_SCOPE)
    assert ensured == [SECRET_SCOPE]


def test_data_model_signature_ends_with_optional_string_base_url(generator: UDTFGenerator) -> None:
    last = generator._parse_udtf_params("SmallBoat")[-1]
    assert last.name == "base_url"
    assert last.type_text == "STRING"
    assert last.parameter_default == "NULL"


@pytest.mark.parametrize("udtf_name", time_series_udtf_registry.get_all_udtf_names())
def test_time_series_view_sql_passes_base_url_secret_last(udtf_name: str) -> None:
    sql = generate_time_series_udtf_view_sql(
        udtf_name=udtf_name, secret_scope=SECRET_SCOPE, catalog="f0connectortest", schema="sailboat_sailboat_v1"
    )
    assert _named_args(sql)[-1] == "base_url"
    assert BASE_URL_ARG in sql


def test_time_series_sql_view_passes_base_url_secret_last() -> None:
    sql = generate_time_series_sql_view(
        secret_scope=SECRET_SCOPE, catalog="f0connectortest", schema="sailboat_sailboat_v1"
    )
    assert _named_args(sql)[-1] == "base_url"
    assert BASE_URL_ARG in sql


def test_time_series_signature_accepts_every_view_sql_arg(generator: UDTFGenerator, temp_output_dir: Path) -> None:
    generated = generator.code_generator.generate_time_series_udtfs(output_dir=temp_output_dir).generated_files
    for udtf_name in time_series_udtf_registry.get_all_udtf_names():
        udtf_file = generated[f"{udtf_name}_catalog"]
        udtf_class = generator._extract_udtf_class_from_ast(udtf_file.read_text(encoding="utf-8"), udtf_file)
        signature = [p.name for p in generator._parse_udtf_params_from_class(udtf_class)]
        view_sql = generate_time_series_udtf_view_sql(udtf_name=udtf_name, secret_scope=SECRET_SCOPE)

        assert signature[-1] == "base_url", udtf_name
        missing = [name for name in _named_args(view_sql) if name not in signature]
        assert missing == [], f"{udtf_name}: view SQL passes undeclared args {missing}"


def test_rewriter_passes_base_url_secret_last() -> None:
    rewritten = DataModelQueryRewriter.rewrite_to_udtf_sql(
        "SELECT * FROM f0connectortest.sailboat_sailboat_v1.SmallBoat WHERE space = 'inst_sailboat_fleet_a' LIMIT 5",
        secret_scope=SECRET_SCOPE,
    )
    assert rewritten is not None
    assert _named_args(rewritten)[-1] == "base_url"
    assert BASE_URL_ARG in rewritten


def test_generated_query_example_passes_base_url_secret() -> None:
    sql = generate_udtf_sql_query("f0connectortest", "sailboat_sailboat_v1", "small_boat_udtf", SECRET_SCOPE)
    assert BASE_URL_ARG in sql
