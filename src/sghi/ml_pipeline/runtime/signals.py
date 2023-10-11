from attrs import field, frozen

from sghi.dispatch import Signal


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
class ConfigErrorSignal(Signal):
    """Signal indicating that a configuration error occurred."""

    err_message: str = field()
    exception: BaseException | None = field(default=None)


@frozen
class UnhandledRuntimeErrorSignal(Signal):
    """Signal indicating that an unhandled error occurred at runtime."""

    err_message: str = field()
    exception: BaseException | None = field(default=None)
