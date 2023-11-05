from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

from attrs import field, frozen, validators

from sghi.ml_pipeline.domain import (
    DataProcessor,
    DataSink,
    DataSource,
    WorkflowDescriptor,
)

from .data_processors import NoOpDataProcessor
from .data_sinks import NullDataSink

if TYPE_CHECKING:
    from typing_extensions import Self

# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")
"""Processed Data Type."""

_RDT = TypeVar("_RDT")
"""Raw Data Type."""


# =============================================================================
# SPEC IMPLEMENTATIONS
# =============================================================================


@frozen
class SimpleWorkflowDescriptor(
    WorkflowDescriptor[_RDT, _PDT],
    Generic[_RDT, _PDT],
):
    """
    A simple implementation of the :class:`WorkflowDescriptor` interface.
    """

    _id: str = field(
        validator=[validators.instance_of(str), validators.min_len(2)],
    )
    _name: str = field(validator=validators.instance_of(str))
    _description: str | None = field(
        default=None,
        kw_only=True,
        validator=validators.optional(validator=validators.instance_of(str)),
    )
    _data_supplier: DataSource[_RDT] = field(
        kw_only=True,
        repr=False,
        validator=validators.instance_of(DataSource),
    )
    _data_processor: DataProcessor[_RDT, _PDT] = field(
        kw_only=True,
        repr=False,
        validator=validators.instance_of(DataProcessor),
    )
    _data_consumer: DataSink[_PDT] = field(
        kw_only=True,
        repr=False,
        validator=validators.instance_of(DataSink),
    )

    @property
    def id(self) -> str:  # noqa: A003
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str | None:
        return self._description

    @property
    def data_supplier(self) -> DataSource[_RDT]:
        return self._data_supplier

    @property
    def data_processor(self) -> DataProcessor[_RDT, _PDT]:
        return self._data_processor

    @property
    def data_consumer(self) -> DataSink[_PDT]:
        return self._data_consumer

    @classmethod
    def of(
        cls,
        id: str,  # noqa: A002
        name: str,
        data_supplier: DataSource[_RDT],
        description: str | None = None,
        data_processor: DataProcessor[_RDT, _PDT] | None = None,
        data_consumer: DataSink[_PDT] | None = None,
    ) -> Self:
        return cls(
            id=id,  # pyright: ignore
            name=name,  # pyright: ignore
            description=description,  # pyright: ignore
            data_supplier=data_supplier,  # pyright: ignore
            data_processor=data_processor
            or NoOpDataProcessor(),  # pyright: ignore
            data_consumer=data_consumer or NullDataSink(),  # pyright: ignore
        )
