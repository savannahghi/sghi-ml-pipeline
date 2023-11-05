from __future__ import annotations

import logging
from collections.abc import Callable
from logging import Logger
from typing import TYPE_CHECKING, Generic, TypeVar

from attrs import define, field, validators

from sghi.disposable import not_disposed
from sghi.ml_pipeline.domain import DataProcessor
from sghi.utils import ensure_not_none, type_fqn

if TYPE_CHECKING:
    from typing_extensions import Self

# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")
"""Processed Data Type."""

_RDT = TypeVar("_RDT")
"""Raw Data Type."""

_DataProcessorCallable = Callable[[_RDT], _PDT]


# =============================================================================
# SPEC IMPLEMENTATIONS
# =============================================================================


@define
class ComposeDataProcessors:  # Aggregate Data Processors
    ...


@define
class PipeDataProcessors:
    ...


@define
class NoOpDataProcessor(DataProcessor[_RDT, _RDT], Generic[_RDT]):
    """
    A :class:`DataProcessor` implementation that performs no processing on the
    received data and returns it as is.
    """

    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

    @not_disposed
    def __call__(self, raw_data: _RDT) -> _RDT:
        return self.process(raw_data)

    @not_disposed
    def __enter__(self) -> Self:
        return super(DataProcessor, self).__enter__()

    @property
    def is_disposed(self) -> bool:
        return self._is_disposed

    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.debug("Disposal complete.")

    @not_disposed
    def process(self, raw_data: _RDT) -> _RDT:
        self._logger.debug("Skipping data processing. Return raw data as is.")
        return raw_data


@define
class _DataProcessorOfCallable(DataProcessor[_RDT, _PDT]):
    _callable: _DataProcessorCallable[_RDT, _PDT] = field(
        validator=validators.is_callable(),
    )
    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

    @not_disposed
    def __call__(self, raw_data: _RDT) -> _PDT:
        return self.process(raw_data)

    @not_disposed
    def __enter__(self) -> Self:
        return super(DataProcessor, self).__enter__()

    @property
    def is_disposed(self) -> bool:
        return self._is_disposed

    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.debug("Disposal complete.")

    def process(self, raw_data: _RDT) -> _PDT:
        self._logger.debug(
            "Processing data using '%s'.",
            type_fqn(self._callable),
        )
        return self._callable(raw_data)


# =============================================================================
# DECORATORS
# =============================================================================


def data_processor(f: Callable[[_RDT], _PDT]) -> DataProcessor[_RDT, _PDT]:
    """Mark a ``Callable`` as a :class:`DataProcessor`.

    :param f: The callable to be decorated. The callable *MUST* have at *MOST*,
        one required argument (the raw data to be processed).

    :return: A ``DataProcessor`` instance.

    :raise ValueError: If ``f`` is ``None``.
    """
    ensure_not_none(f, "'f' MUST not be None.")

    def wrapper(_f: Callable[[_RDT], _PDT]) -> DataProcessor[_RDT, _PDT]:
        return _DataProcessorOfCallable(callable=_f)  # pyright: ignore

    return wrapper(f)
