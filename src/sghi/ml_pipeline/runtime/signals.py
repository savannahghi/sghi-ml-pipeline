from attrs import field, frozen

from sghi.dispatch import Signal
from sghi.ml_pipeline.domain import ETLWorkflow


@frozen
class AppReady(Signal):
    """
    Signal indicating that application setup is done and normal application
    operations can now start.
    """


@frozen
class AppStopping(Signal):
    """Signal indicating that the application is about stop."""


@frozen
class CompletedETLWorkflow(Signal):
    """Signal indicating that an :class:`ETLWorkflow` has completed.

    Only called if the `ETLWorkflow` completes successfully.
    """

    etl_workflow: ETLWorkflow = field()


@frozen
class ConfigFailed(Signal):
    """Signal indicating that a configuration error occurred."""

    err_message: str = field()
    exception: BaseException | None = field(default=None)


@frozen
class ETLWorkflowFailed(Signal):
    """
    Signal indicating that an :class:`ETLWorkflow` failed when executing.
    """

    etl_workflow: ETLWorkflow = field()
    err_message: str = field()
    exception: BaseException | None = field(default=None)


@frozen
class StartingETLWorkflow(Signal):
    """
    Signal indicating that an :class:`ETLWorkflow` is about to be executed.
    """

    etl_workflow: ETLWorkflow = field()


@frozen
class UnhandledRuntimeErrorSignal(Signal):
    """Signal indicating that an unhandled error occurred at runtime."""

    err_message: str = field()
    exception: BaseException | None = field(default=None)
