from typing import TYPE_CHECKING

from attrs import frozen

import sghi.app
from sghi.ml_pipeline.runtime import signals
from sghi.ml_pipeline.runtime.ui import UI

from .printers import print_debug, print_error, print_info, print_success

if TYPE_CHECKING:
    from sghi.dispatch import Dispatcher


@frozen
class SimpleUI(UI):
    """
    Simple :class:`UI` that prints information on the console using
    `click.echo`.
    """

    def start(self) -> None:
        print_info("Starting ...")

        app_dispatcher: Dispatcher = sghi.app.dispatcher
        app_dispatcher.connect(signals.AppReady, self.on_app_ready)
        app_dispatcher.connect(signals.AppStopping, self.on_app_stopping)
        app_dispatcher.connect(signals.ConfigFailed, self.on_config_error)
        app_dispatcher.connect(
            signals.UnhandledRuntimeErrorSignal,
            self.on_runtime_error,
        )
        app_dispatcher.connect(
            signals.CompletedETLWorkflow,
            self.on_etl_workflow_completion,
        )
        app_dispatcher.connect(
            signals.ETLWorkflowFailed,
            self.on_etl_workflow_error,
        )
        app_dispatcher.connect(
            signals.StartingETLWorkflow,
            self.on_etl_workflow_starting,
        )

    def stop(self) -> None:
        print_info("Done 😁")

    @staticmethod
    def on_app_ready(signal: signals.AppReady) -> None:
        print_info("Started")

    @staticmethod
    def on_app_stopping(signal: signals.AppStopping) -> None:
        print_info("Stopping ...")

    @staticmethod
    def on_config_error(signal: signals.ConfigFailed) -> None:
        print_error(
            error_message=signal.err_message,
            exception=signal.exception,
        )

    @staticmethod
    def on_runtime_error(signal: signals.UnhandledRuntimeErrorSignal) -> None:
        print_error(
            error_message=signal.err_message,
            exception=signal.exception,
        )

    @staticmethod
    def on_etl_workflow_completion(
        signal: signals.CompletedETLWorkflow,
    ) -> None:
        print_success(
            "- Completed ETLWorkflow for extract '{}:{}' ✔️".format(
                signal.etl_workflow.id,
                signal.etl_workflow.name,
            ),
        )

    @staticmethod
    def on_etl_workflow_error(signal: signals.ETLWorkflowFailed) -> None:
        print_error(
            "- Error running ETLWorkflow for extract '{}:{}' ⚠️".format(
                signal.etl_workflow.id,
                signal.etl_workflow.name,
            ),
            exception=signal.exception,
        )

    @staticmethod
    def on_etl_workflow_starting(signal: signals.StartingETLWorkflow) -> None:
        print_debug(
            "- Running the workflow '{}:{}' ...".format(
                signal.etl_workflow.id,
                signal.etl_workflow.name,
            ),
        )
