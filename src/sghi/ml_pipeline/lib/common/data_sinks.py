from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from contextlib import ExitStack
from logging import Logger
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from attrs import define, field, validators

from sghi.disposable import not_disposed
from sghi.ml_pipeline.domain import DataSink
from sghi.task import ConcurrentExecutor
from sghi.utils import ensure_not_none, type_fqn

if TYPE_CHECKING:
    from typing_extensions import Self

# =============================================================================
# TYPES
# =============================================================================


_PDT = TypeVar("_PDT")
"""Processed Data Type."""

_DataSinkCallable = Callable[[_PDT], None]


# =============================================================================
# SPEC IMPLEMENTATIONS
# =============================================================================


@define
class FanOutDataSink(DataSink[_PDT], Generic[_PDT]):  # CompositeDataSink
    """One-To-Many Multiplexer"""

    _data_sinks: Sequence[DataSink[_PDT]] = field(
        converter=tuple,  # Make a copy
        repr=False,
        validator=[
            validators.min_len(1),
            validators.deep_iterable(
                member_validator=validators.instance_of(DataSink),
                iterable_validator=validators.instance_of(Sequence),
            ),
        ],
    )
    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)
    _executor: ConcurrentExecutor[_PDT, None] = field(init=False, repr=False)
    _exit_stack: ExitStack = field(factory=ExitStack, init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))
        self._executor = ConcurrentExecutor(*self._data_sinks)

    @not_disposed
    def __call__(self, processed_data: Any) -> None:  # noqa: ANN401
        return self.drain(processed_data)

    @not_disposed
    def __enter__(self) -> Self:
        return super(DataSink, self).__enter__()

    @property
    def is_disposed(self) -> bool:
        return self._is_disposed

    def dispose(self) -> None:
        self._is_disposed = True
        self._exit_stack.close()
        self._executor.dispose()
        self._logger.debug("Disposal complete.")

    def drain(self, processed_data: _PDT) -> None:  # TODO: Add error handling
        self._logger.debug("Multiplexing processed data to consumers.")
        with self._exit_stack:
            self._executor(an_input=processed_data)


@define
class NullDataSink(DataSink[Any]):
    """
    A :class:`DataSink` implementation that discards processed the data it
    receives.
    """

    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

    @not_disposed
    def __call__(self, processed_data: Any) -> None:  # noqa: ANN401
        return self.drain(processed_data)

    @not_disposed
    def __enter__(self) -> Self:
        return super(DataSink, self).__enter__()

    @property
    def is_disposed(self) -> bool:
        return self._is_disposed

    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.debug("Disposal complete.")

    @not_disposed
    def drain(self, processed_data: Any) -> None:  # noqa: ANN401
        self._logger.debug("Discarding processed data.")


@define
class _DataSinkOfCallable(DataSink[_PDT]):
    _callable: _DataSinkCallable[_PDT] = field(
        validator=validators.is_callable(),
    )
    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

    @not_disposed
    def __call__(self, processed_data: Any) -> None:  # noqa: ANN401
        return self.drain(processed_data)

    @not_disposed
    def __enter__(self) -> Self:
        return super(DataSink, self).__enter__()

    @property
    def is_disposed(self) -> bool:
        return self._is_disposed

    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.debug("Disposal complete.")

    @not_disposed
    def drain(self, processed_data: Any) -> None:  # noqa: ANN401
        self._logger.debug(
            "Draining data to '%s'.",
            type_fqn(self._callable),
        )
        self._callable(processed_data)


# =============================================================================
# DECORATORS
# =============================================================================


def data_sink(f: Callable[[_PDT], None]) -> DataSink[_PDT]:
    """Mark a ``Callable`` as a :class:`DataSink`.

    :param f: The callable to be decorated. The callable *MUST* have at *MOST*,
        one required argument (the processed data to consume).

    :return: A ``DataSink`` instance.

    :raise ValueError: If ``f`` is ``None``.
    """
    ensure_not_none(f, "'f' MUST not be None.")

    def wrapper(_f: Callable[[_PDT], None]) -> DataSink[_PDT]:
        return _DataSinkOfCallable(callable=_f)  # pyright: ignore

    return wrapper(f)
