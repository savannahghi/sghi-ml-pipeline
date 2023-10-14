from collections.abc import Callable, Mapping, Sequence

import sghi.app
from sghi.config import ImproperlyConfiguredError
from sghi.exceptions import SGHIError
from sghi.ml_pipeline.domain import ETLWorkflow
from sghi.ml_pipeline.runtime.constants import (
    ETL_WORKFLOW_REG_KEY,
    ETL_WORKFLOWS_CONFIG_KEY,
)
from sghi.task import execute_concurrently
from sghi.utils import (
    ensure_instance_of,
    ensure_optional_instance_of,
    ensure_predicate,
    type_fqn,
)

from .run_workflow import RunWorkflow

# =============================================================================
# TYPES
# =============================================================================


ETLWorkflowFactory = Callable[[], ETLWorkflow]


# =============================================================================
# HELPERS
# =============================================================================


def _etl_workflow_factory_to_instance(
    workflow_factory: ETLWorkflowFactory,
) -> ETLWorkflow:
    ensure_predicate(
        test=workflow_factory is not None,
        message="ETL workflow factory MUST not be None.",
        exc_factory=ImproperlyConfiguredError,
    )

    etl_workflow = workflow_factory()
    ensure_predicate(
        test=isinstance(etl_workflow, ETLWorkflow),
        message=(
            f"The ETLWorkflow factory '{type_fqn(workflow_factory)}' returned "
            "a None ETLWorkflow instance."
        ),
        exc_factory=ImproperlyConfiguredError,
    )

    return etl_workflow


def _init_and_register_workflows() -> None:
    """
    Load all workflows from the config and add them to the application
    registry.
    """
    wf_factories: Sequence[ETLWorkflowFactory]
    wf_factories = sghi.app.conf.get(ETL_WORKFLOWS_CONFIG_KEY, ())
    workflows: Mapping[str, ETLWorkflow] = {
        workflow.id: workflow
        for workflow in map(_etl_workflow_factory_to_instance, wf_factories)
    }

    sghi.app.registry[ETL_WORKFLOW_REG_KEY] = workflows


# =============================================================================
# USE CASES
# =============================================================================


class NoSuchWorkflowsError(SGHIError):
    """Indicates that selected workflow(s) are not known."""

    def __init__(
        self,
        missing_workflows: Sequence[str],
        message: str | None = None,
    ) -> None:
        ensure_instance_of(missing_workflows, Sequence)
        self._missing_workflows: Sequence[str] = missing_workflows
        _msg: str = message or (
            "The following workflow(s) do not exists: "
            f"'{','.join(missing_workflows)}'."
        )
        super().__init__(message=_msg)

    @property
    def missing_workflows(self) -> Sequence[str]:
        return self._missing_workflows


# =============================================================================
# USE CASES
# =============================================================================


def list_workflows() -> Mapping[str, ETLWorkflow]:
    """List all loaded and registered ETLWorkflows."""

    _init_and_register_workflows()
    return sghi.app.registry.get(ETL_WORKFLOW_REG_KEY, {})


def run(select: Sequence[str] | None = None) -> None:
    """Load and run the selected ETL Workflows. Run all when ``None``."""
    ensure_optional_instance_of(
        select,
        klass=Sequence,
        message="'select' MUST be a Sequence of workflow IDs to execute.",
    )

    workflows: Mapping[str, ETLWorkflow] = list_workflows()
    _selected: set[str] | None = None if select is None else set(select)

    if _selected and (missing_workflows := _selected.difference(workflows)):
        raise NoSuchWorkflowsError(tuple(missing_workflows))

    selected_workflows: Sequence[RunWorkflow] = [
        RunWorkflow(workflow)
        for wf_id, workflow in workflows.items()
        if _selected is None or wf_id in _selected
    ]

    if not selected_workflows:
        return
    with execute_concurrently(*selected_workflows) as executor:
        executor(None)


# =============================================================================
# MODULE EXPORTS
# =============================================================================


__all__ = [
    "list_workflows",
    "run",
]
