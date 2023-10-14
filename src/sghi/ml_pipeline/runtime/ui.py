from abc import ABCMeta, abstractmethod

# =============================================================================
# UI INTERFACE
# =============================================================================


class UI(metaclass=ABCMeta):
    """Application user interface provider."""

    __slots__ = ()

    @abstractmethod
    def start(self) -> None:
        """Start the UI.

        Called at the beginning of the application.

        .. note::

            - Only called once.
            - Should not block the caller. Specifically, should not block the
              calling thread.

        :return: None.
        """
        ...

    @abstractmethod
    def stop(self) -> None:
        """Stop the UI.

        Called at the end of the application.

        :return: None.
        """
        ...


# =============================================================================
# UI IMPLEMENTATIONS
# =============================================================================


class NoUI(UI):
    """:class:`UI` implementation that provides no UI."""

    def start(self) -> None:
        ...

    def stop(self) -> None:
        ...
