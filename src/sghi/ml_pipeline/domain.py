"""The specification for the SGHI ML Pipeline tool."""

from abc import ABCMeta, abstractmethod
from typing import Generic, TypeVar

from sghi.disposable import Disposable

# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")
"""Processed Data Type."""

_RDT = TypeVar("_RDT")
"""Raw Data Type."""


# =============================================================================
# BASE INTERFACES
# =============================================================================


class DataSource(Disposable, Generic[_RDT], metaclass=ABCMeta):
    """
    An interface representing an entity that contains or provides data of
    interest.

    This class defines a blueprint for a data source, which is a provider of
    raw data. Subclasses implementing this interface should override the
    :meth:`draw` method to specify how the data is obtained.

    This class implements the :class:`~sghi.disposable.Disposable` interface
    allowing for easy resource(s) management and clean up.
    """

    __slots__ = ()

    def __call__(self) -> _RDT:
        """Obtain raw data from this :class:`data source<DataSource>`.

        Call this ``DataSource`` instance as a callable. Delegate actual call
        to :meth:`draw`.

        :return: The raw data from this `DataSource`.
        """
        return self.draw()

    @abstractmethod
    def draw(self) -> _RDT:
        """Obtain raw data from this :class:`data source<DataSource>`.

        :return: The raw data from this `DataSource`.
        """
        ...


class DataProcessor(Disposable, Generic[_RDT, _PDT], metaclass=ABCMeta):
    """
    An interface representing the post-extraction operation(s) and
    transformation(s) to be performed on raw data.

    This class defines a blueprint for processing raw data and converting it
    into processed data ready for further consumption downstream. Subclasses
    implementing this interface should override the :meth:`process` method to
    specify how the data processing occurs.

    This class implements the :class:`~sghi.disposable.Disposable` interface
    allowing for easy resource(s) management and clean up.
    """

    __slots__ = ()

    def __call__(self, raw_data: _RDT) -> _PDT:
        """Transform raw data into processed, clean data and return it.

        Call this ``DataProcessor`` as a callable. Delegate actual call to
        :meth:`process`.

        :param raw_data: The unprocessed data drawn from a `DataSource`.

        :return: The processed, cleaned data that is ready for further
            consumption downstream.
        """
        return self.process(raw_data)

    @abstractmethod
    def process(self, raw_data: _RDT) -> _PDT:
        """Transform raw data into processed, clean data and return it.

        :param raw_data: The unprocessed data drawn from a `DataSource`.

        :return: The processed, cleaned data that is ready for further
            consumption downstream.
        """
        ...


class DataSink(Disposable, Generic[_PDT], metaclass=ABCMeta):
    """An interface representing the final destination of processed data.

    This class defined a blueprint for entities that consume processed data.
    Subclasses implementing this interface should override the :meth:`drain`
    method to specify how the processed data is consumed.

    This class implements the :class:`~sghi.disposable.Disposable` interface
    allowing for easy resource(s) management and clean up.
    """

    __slots__ = ()

    def __call__(self, processed_data: _PDT) -> None:
        """Consume processed data.

        Call this ``DataSink`` as a callable. Delegate actual call to
        :meth:`drain`.

        :param processed_data: The processed data to be consumed.

        :return: None.
        """
        return self.drain(processed_data)

    @abstractmethod
    def drain(self, processed_data: _PDT) -> None:
        """Consume processed data.

        :param processed_data: The processed data to be consumed.

        :return: None.
        """
        ...


class WorkflowDescriptor(Generic[_RDT, _PDT], metaclass=ABCMeta):
    """An interface that describes a data workflow.

    This class defines a blueprint for describing a data workflow, which
    consists of a :class:`data supplier<DataSource>`,
    :class:`data processor<DataProcessor>`, and
    :class:`data consumer<DataSink>`. Subclasses should provide the necessary
    properties to describe the workflow.
    """

    __slots__ = ()

    @property
    @abstractmethod
    def id(self) -> str:  # noqa: A003
        """The unique identifier of this workflow.

        :return: The unique identifier of this workflow.
        """
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """The name of this workflow.

        :return: The name of this workflow.
        """
        ...

    @property
    @abstractmethod
    def description(self) -> str | None:
        """The description of this workflow, if available.

        :return: The description of this workflow or ``None`` if not available.
        """
        ...

    @property
    @abstractmethod
    def data_supplier(self) -> DataSource[_RDT]:
        """The :class:`data supplier<DataSource>` for this workflow.

        :return: The data supplier for this workflow.
        """
        ...

    @property
    @abstractmethod
    def data_processor(self) -> DataProcessor[_RDT, _PDT]:
        """The :class:`data processor<DataProcessor>` for this workflow.

        :return: The data processor for this workflow.
        """
        ...

    @property
    @abstractmethod
    def data_consumer(self) -> DataSink[_PDT]:
        """The :class:`data consumer<DataSink>` for this workflow.

        :return: The data consumer for this workflow.
        """
        ...


# =============================================================================
# MODULE EXPORTS
# =============================================================================


__all__ = [
    "DataProcessor",
    "DataSource",
    "DataSink",
    "WorkflowDescriptor",
]
