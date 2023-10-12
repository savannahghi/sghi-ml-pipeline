import logging
from contextlib import ExitStack
from logging import Logger
from typing import Any

from attrs import define, field, validators

from sghi.ml_pipeline.domain import ETLWorkflow, Extract, Load, Transform
from sghi.task import Task, pipe
from sghi.utils import type_fqn


@define
class RunWorkflow(Task[None, None]):
    _etl_workflow: ETLWorkflow[Any, Any] = field(
        validator=validators.instance_of(ETLWorkflow),
    )
    _logger: Logger = field(init=False, repr=False)
    _workflow_stack: ExitStack = field(
        factory=ExitStack,
        init=False,
        repr=False,
    )

    def __attrs_post_init__(self) -> None:
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

    def __call__(self, an_input: None = None) -> None:
        return self.execute()

    def execute(self, an_input: None = None) -> None:
        self._logger.info(
            "[%s - %s] Execute ETL workflow.",
            self._etl_workflow.id,
            self._etl_workflow.name,
        )
        with self._workflow_stack:
            extractor: Extract = self._workflow_stack.enter_context(
                self._etl_workflow.extractor,
            )
            transformer: Transform = self._workflow_stack.enter_context(
                self._etl_workflow.transformer,
            )
            loader: Load = self._workflow_stack.enter_context(
                self._etl_workflow.loader,
            )

            pipe(lambda _: extractor(), transformer, loader)(None)
