from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Generic, TypeVar

from attrs import define, field, validators

from sghi.ml_pipeline.domain import (
    DataProcessor,
    DataSink,
    DataSource,
    WorkflowDescriptor,
)
from sghi.utils import (
    ensure_instance_of,
    ensure_not_none_nor_empty,
)

from .data_processors import NoOpDataProcessor
from .data_sinks import FanOutDataSink, NullDataSink
from .data_sources import FanInDataSource
from .workflow_descriptors import SimpleWorkflowDescriptor

# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")
"""Processed Data Type."""

_RDT = TypeVar("_RDT")
"""Raw Data Type."""

_CompositeConsumerFactory = Callable[
    [Sequence[DataSink[_PDT]]],
    DataSink[Sequence[_PDT]],
]

_CompositeProcessorFactory = Callable[
    [Sequence[DataProcessor[_RDT, _PDT]]],
    DataProcessor[_RDT, _PDT],
]

_CompositeSupplierFactory = Callable[
    [Sequence[DataSource[_RDT]]],
    DataSource[Sequence[_RDT]],
]

_DefaultProcessorFactory = Callable[[], DataProcessor[_RDT, _PDT]]

_DefaultConsumerFactory = Callable[[], DataSink[_PDT]]


# =============================================================================
# SPEC IMPLEMENTATIONS
# =============================================================================


@define
class WorkflowBuilder(Generic[_RDT, _PDT]):
    """A builder of :class:`WorkflowDescriptor` instances."""

    id: str = field(  # noqa: A003
        validator=[validators.instance_of(str), validators.min_len(2)],
    )
    name: str = field(validator=validators.instance_of(str))
    description: str | None = field(
        default=None,
        kw_only=True,
        validator=validators.optional(validator=validators.instance_of(str)),
    )
    data_suppliers: Sequence[DataSource[_RDT]] | None = field(
        default=None,
        kw_only=True,
        repr=False,
        validator=validators.optional(
            validators.deep_iterable(
                iterable_validator=validators.instance_of(Sequence),
                member_validator=validators.instance_of(DataSource),
            ),
        ),
    )
    data_processors: Sequence[DataProcessor[_RDT, _PDT]] | None = field(
        default=None,
        kw_only=True,
        repr=False,
        validator=validators.optional(
            validators.deep_iterable(
                iterable_validator=validators.instance_of(Sequence),
                member_validator=validators.instance_of(DataProcessor),
            ),
        ),
    )
    data_consumers: Sequence[DataSink[_PDT]] | None = field(
        default=None,
        kw_only=True,
        repr=False,
        validator=validators.optional(
            validators.deep_iterable(
                iterable_validator=validators.instance_of(Sequence),
                member_validator=validators.instance_of(DataSink),
            ),
        ),
    )
    default_processor_factory: _DefaultProcessorFactory = field(
        default=NoOpDataProcessor,
        kw_only=True,
        repr=False,
        validator=validators.is_callable(),
    )
    default_consumer_factory: _DefaultConsumerFactory = field(
        default=NullDataSink,
        kw_only=True,
        repr=False,
        validator=validators.is_callable(),
    )
    composite_supplier_factory: _CompositeSupplierFactory = field(
        default=FanInDataSource[_RDT],
        kw_only=True,
        repr=False,
        validator=validators.is_callable(),
    )
    composite_consumer_factory: _CompositeConsumerFactory = field(
        default=FanOutDataSink[_PDT],
        kw_only=True,
        repr=False,
        validator=validators.is_callable(),
    )
    _data_suppliers: list[DataSource[_RDT]] = field(
        factory=list,
        init=False,
        repr=False,
    )
    _data_processors: list[DataProcessor[_RDT, _PDT]] = field(
        factory=list,
        init=False,
        repr=False,
    )
    _data_consumers: list[DataSink[_PDT]] = field(
        factory=list,
        init=False,
        repr=False,
    )

    def __attrs_post_init__(self) -> None:
        self._data_consumers.extend(self.data_consumers or ())
        self._data_processors.extend(self.data_processors or ())
        self._data_suppliers.extend(self.data_suppliers or ())

    def __call__(self) -> WorkflowDescriptor[_RDT, _PDT]:
        return self.build()

    def build(self) -> WorkflowDescriptor[_RDT, _PDT]:
        return SimpleWorkflowDescriptor.of(
            id=self.id,
            name=self.name,
            description=self.description,
            data_supplier=self._build_supplier(),
            data_processor=self.default_processor_factory(),
            data_consumer=self._build_consumer(),
        )

    def add_consumer(self, consumer: DataSink[_PDT]) -> DataSink[_PDT]:
        ensure_instance_of(consumer, DataSink, "'consumer' MUST be DataSink.")
        self._data_consumers.append(consumer)
        return consumer

    def add_supplier(self, supplier: DataSource[_RDT]) -> DataSource[_RDT]:
        ensure_instance_of(
            value=supplier,
            klass=DataSource,
            message="'supplier' MUST be a DataSource.",
        )
        self._data_suppliers.append(supplier)
        return supplier

    def _build_consumer(self) -> DataSink[_PDT]:
        match self._data_consumers:
            case (_, _, *_):
                return self.composite_consumer_factory(self._data_consumers)
            case (entry, *_):
                return entry
            case _:
                return self.default_consumer_factory()

    def _build_supplier(self) -> DataSource[_RDT]:
        ensure_not_none_nor_empty(
            self._data_suppliers,
            "A 'data_supplier' MUST be provided.",
        )
        match self._data_suppliers:
            case (_, _, *_):
                return self.composite_supplier_factory(self._data_suppliers)
            case (entry, *_):
                return entry
