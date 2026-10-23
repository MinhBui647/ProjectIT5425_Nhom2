"""Great Expectations File Data Context mirrored from operational contracts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import great_expectations as gx
import great_expectations.checkpoint.actions as gx_actions
import great_expectations.expectations as gxe
import polars as pl

from data_governance.config import CONTRACT_VERSION, DATA_DOCS_PATH, PROJECT_ROOT
from data_governance.contracts import CONTRACTS, gx_expectation_specs
from lakehouse_storage.schemas import normalize_table_name


# Keep datasource/asset identifiers short: GX combines them into validation
# filenames, and verbose names exceed the legacy Windows MAX_PATH limit when
# the repository itself lives below a long OneDrive path.
DATASOURCE_NAME = "dg"
DATA_DOCS_SITE_NAME = "local_site"


def _suite_name(table_name: str) -> str:
    return f"governance_{table_name}_{CONTRACT_VERSION}"


def _asset_name(table_name: str) -> str:
    return table_name


def _validation_name(table_name: str) -> str:
    return f"governance_{table_name}_{CONTRACT_VERSION}_validation"


def _checkpoint_name(table_name: str) -> str:
    return f"governance_{table_name}_{CONTRACT_VERSION}_checkpoint"


def create_gx_context(
    *,
    project_root: str | Path = PROJECT_ROOT,
    data_docs_path: str | Path = DATA_DOCS_PATH,
):
    """Create a persisted GX context with six suites and checkpoints."""
    project_root = Path(project_root).resolve()
    project_root.mkdir(parents=True, exist_ok=True)
    data_docs_path = Path(data_docs_path).resolve()
    context = gx.get_context(mode="file", project_root_dir=project_root)

    site_config = context.list_data_docs_sites()[DATA_DOCS_SITE_NAME]
    site_config["store_backend"] = {
        "class_name": "TupleFilesystemStoreBackend",
        "base_directory": str(data_docs_path),
    }
    context.update_data_docs_site(DATA_DOCS_SITE_NAME, site_config)

    datasource = context.data_sources.add_pandas(name=DATASOURCE_NAME)
    for table_name in CONTRACTS:
        suite = gx.ExpectationSuite(name=_suite_name(table_name))
        for spec in gx_expectation_specs(table_name):
            expectation_class = getattr(gxe, spec.expectation_type)
            suite.add_expectation(
                expectation_class(
                    **spec.kwargs,
                    meta={"operational_rule_id": spec.rule_id},
                )
            )
        suite = context.suites.add(suite)

        asset = datasource.add_dataframe_asset(name=_asset_name(table_name))
        batch_definition = asset.add_batch_definition_whole_dataframe(
            name=f"{table_name}_whole_dataframe"
        )
        validation = context.validation_definitions.add(
            gx.ValidationDefinition(
                name=_validation_name(table_name),
                data=batch_definition,
                suite=suite,
            )
        )
        context.checkpoints.add(
            gx.Checkpoint(
                name=_checkpoint_name(table_name),
                validation_definitions=[validation],
                result_format="COMPLETE",
                actions=[
                    gx_actions.UpdateDataDocsAction(
                        name="update_governance_data_docs",
                        site_names=[DATA_DOCS_SITE_NAME],
                    )
                ],
            )
        )
    return context


def run_gx_checkpoint(
    context,
    df: pl.DataFrame,
    table_name: str,
) -> dict[str, Any]:
    """Run one mirrored suite against a canonical Polars dataframe."""
    table_name = normalize_table_name(table_name)
    if table_name not in CONTRACTS:
        raise ValueError(f"Unknown governance contract: {table_name}")
    checkpoint = context.checkpoints.get(_checkpoint_name(table_name))
    result = checkpoint.run(batch_parameters={"dataframe": df.to_pandas()})
    docs = context.build_data_docs(site_names=[DATA_DOCS_SITE_NAME])
    return {
        "success": bool(result.success),
        "suite_name": _suite_name(table_name),
        "checkpoint_name": _checkpoint_name(table_name),
        "data_docs": docs,
        "result": result,
    }

