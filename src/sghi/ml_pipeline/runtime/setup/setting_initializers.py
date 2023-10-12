import logging
from collections.abc import Callable, Mapping, Sequence
from logging.config import dictConfig
from typing import Any, Literal, TypedDict, TypeVar

import sghi.app
from sghi.config import ImproperlyConfiguredError, SettingInitializer
from sghi.ml_pipeline.domain import ETLWorkflow
from sghi.registry import RegistryItemSet
from sghi.utils import import_string

from ..constants import (
    APP_LOG_LEVEL_REG_KEY,
    DEFAULT_CONFIG,
    ETL_WORKFLOWS_CONFIG_KEY,
    LOGGING_CONFIG_KEY,
)

# =============================================================================
# TYPES
# =============================================================================


_ET = TypeVar("_ET")

_LT = TypeVar("_LT")

ETLWorkflowConfigKinds = Literal["factory", "mapping"]


class ETLWorkflowDefinition(TypedDict):
    id: str
    name: str
    description: str | None
    extractor_factory: str
    transformer_factory: str
    loader_factory: str


class ETLWorkflowConfigMapping(TypedDict):
    kind: ETLWorkflowConfigKinds
    value: str | ETLWorkflowDefinition


ETLWorkflowFactory = Callable[[], ETLWorkflow[_ET, _LT]]


# =============================================================================
# HELPERS
# =============================================================================


def on_logging_level_changed(signal: RegistryItemSet) -> None:
    if signal.item_key != APP_LOG_LEVEL_REG_KEY:
        return

    logging.getLogger("sghi").setLevel(signal.item_value)


# =============================================================================
# INITIALIZERS
# =============================================================================


class ETLWorkflowsInitializer(SettingInitializer):
    """:class:`SettingInitializer` that loads workflows from the config."""

    @property
    def setting(self) -> str:
        return ETL_WORKFLOWS_CONFIG_KEY

    def execute(
        self,
        an_input: Sequence[ETLWorkflowConfigMapping] | None,
    ) -> Sequence[ETLWorkflowFactory]:
        workflow_factories: list[ETLWorkflowFactory] = []

        for workflow_config in an_input or ():
            self._check_workflow_config(workflow_config)
            match workflow_config["kind"]:
                case "factory":
                    workflow_factories.append(
                        self._load_factory_kind(workflow_config),
                    )
                case "mapping":
                    _err_msg: str = (
                        "'mapping' workflow kind is currently not supported "
                        "(Coming soon). Please use 'factory' instead."
                    )
                    raise ImproperlyConfiguredError(message=_err_msg)
                case _:
                    _err_msg: str = (
                        f"Unknown workflow kind '{workflow_config['kind']}'."
                    )
                    raise ImproperlyConfiguredError(message=_err_msg)
        return workflow_factories

    @staticmethod
    def _check_workflow_config(
        workflow_config: ETLWorkflowConfigMapping,
    ) -> None:
        """Perform basic checks on a workflow config value.

        :param workflow_config:
        :return:
        """
        if not isinstance(workflow_config, Mapping):
            _err_msg: str = "A workflow config must be a mapping."
            raise ImproperlyConfiguredError(message=_err_msg)

        if "kind" not in workflow_config:
            _err_msg: str = "A workflow config must have a 'kind' entry."
            raise ImproperlyConfiguredError(message=_err_msg)

    @staticmethod
    def _load_factory_kind(
        workflow_config: ETLWorkflowConfigMapping,
    ) -> ETLWorkflowFactory:
        value = workflow_config["value"]
        if not isinstance(value, str):
            _err_msg: str = (
                "'value' should be a string containing a dotted path to a "
                "callable factory when kind of ETL workflow is set to "
                "'factory'."
            )
            raise ImproperlyConfiguredError(message=_err_msg)

        try:
            return import_string(value)
        except ImportError as exp:
            _err_msg: str = (
                f"Invalid workflow factory given. '{value}' does not seem to "
                "be a valid dotted path."
            )
            raise ImproperlyConfiguredError(message=_err_msg) from exp


class LoggingInitializer(SettingInitializer):
    """:class:`SettingInitializer` that configures logging for the app."""

    @property
    def setting(self) -> str:
        return LOGGING_CONFIG_KEY

    def execute(self, an_input: Mapping[str, Any] | None) -> Mapping[str, Any]:
        logging_config: dict[str, Any] = dict(
            an_input or DEFAULT_CONFIG[self.setting],
        )
        dictConfig(logging_config)
        logging.getLogger("sghi").setLevel(
            sghi.app.registry.get(APP_LOG_LEVEL_REG_KEY, logging.CRITICAL),
        )
        sghi.app.registry.dispatcher.connect(
            signal_type=RegistryItemSet,
            receiver=on_logging_level_changed,
            weak=False,  # Last for the lifetime of the application
        )
        return logging_config
