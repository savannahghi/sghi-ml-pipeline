from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from contextlib import ExitStack
from logging import Logger
from typing import TYPE_CHECKING, Generic, TypeVar

from attrs import define, field, validators

from sghi.disposable import not_disposed
from sghi.ml_pipeline.domain import DataSource
from sghi.task import ConcurrentExecutor, Task
from sghi.utils import ensure_not_none, future_succeeded, type_fqn

if TYPE_CHECKING:
    from typing_extensions import Self

# =============================================================================
# TYPES
# =============================================================================


_RDT = TypeVar("_RDT")
"""Raw Data Type."""

_DataSourceCallable = Callable[[], _RDT]


# =============================================================================
# SPEC IMPLEMENTATIONS
# =============================================================================


@define
class FanInDataSource(DataSource[Sequence[_RDT]], Generic[_RDT]):
    """Aggregator"""

    _data_sources: Sequence[DataSource[_RDT]] = field(
        converter=tuple,  # Make a copy
        repr=False,
        validator=[
            validators.min_len(1),
            validators.deep_iterable(
                member_validator=validators.instance_of(DataSource),
                iterable_validator=validators.instance_of(Sequence),
            ),
        ],
    )
    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)
    _executor: ConcurrentExecutor[None, _RDT] = field(init=False, repr=False)
    _exit_stack: ExitStack = field(factory=ExitStack, init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))
        self._executor = ConcurrentExecutor(
            *map(self._ds_to_task, self._data_sources),
        )

    @not_disposed
    def __call__(self) -> Sequence[_RDT]:
        return self.draw()

    @not_disposed
    def __enter__(self) -> Self:
        return super(DataSource, self).__enter__()

    @property
    def is_disposed(self) -> bool:
        return self._is_disposed

    def dispose(self) -> None:
        self._is_disposed = True
        self._exit_stack.close()
        self._executor.dispose()
        self._logger.debug("Disposal complete.")

    @not_disposed
    def draw(self) -> Sequence[_RDT]:  # TODO: Add error handling
        self._logger.info("Aggregating data from different data sources.")

        with self._exit_stack:
            for _data_source in self._data_sources:
                self._exit_stack.enter_context(_data_source)

            futures = self._exit_stack.enter_context(self._executor)(None)
        return [
            future.result()
            for future in filter(future_succeeded, futures)  # pyright: ignore
        ]

    @staticmethod
    def _ds_to_task(data_source: DataSource[_RDT]) -> Task[None, _RDT]:
        return Task.of_callable(lambda _: data_source.draw())


@define
class _DataSourceOfCallable(DataSource[_RDT]):
    _callable: _DataSourceCallable[_RDT] = field(
        validator=validators.is_callable(),
    )
    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

    @not_disposed
    def __call__(self) -> _RDT:
        return self.draw()

    @not_disposed
    def __enter__(self) -> Self:
        return super(DataSource, self).__enter__()

    @property
    def is_disposed(self) -> bool:
        return self._is_disposed

    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.debug("Disposal complete.")

    @not_disposed
    def draw(self) -> _RDT:
        self._logger.debug(
            "Drawing data from callable '%s'.",
            type_fqn(self._callable),
        )
        return self._callable()


# =============================================================================
# DECORATORS
# =============================================================================


def data_source(f: Callable[[], _RDT]) -> DataSource[_RDT]:
    """Mark a ``Callable`` as a :class:`DataSource`.

    :param f: The callable to be decorated. The callable *MUST* not have any,
        required arguments but must return a value (the drawn data).
    :return: A ``DataSource`` instance.

    :raise ValueError: If ``f`` is ``None``.
    """
    ensure_not_none(f, "'f' MUST not be None.")

    def wrapper(_f: Callable[[], _RDT]) -> DataSource[_RDT]:
        return _DataSourceOfCallable(callable=_f)  # pyright: ignore

    return wrapper(f)
