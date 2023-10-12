import logging
from logging import Logger
from typing import Any, TypeVar

from attrs import define, field, frozen, validators
from typing_extensions import Self

from sghi.disposable import not_disposed
from sghi.ml_pipeline.domain import ETLWorkflow, Extract, Load, Transform
from sghi.utils import type_fqn

# =============================================================================
# TYPES
# =============================================================================


_ET = TypeVar("_ET")

_LT = TypeVar("_LT")


# =============================================================================
# SPEC IMPLEMENTATIONS
# =============================================================================


@frozen
class SimpleETLWorkflow(ETLWorkflow[_ET, _LT]):
    _id: str = field(validator=validators.instance_of(str))
    _name: str = field(validator=validators.instance_of(str))
    _description: str | None = field(
        default=None,
        kw_only=True,
        validator=validators.optional(validators.instance_of(str)),
    )
    _extractor: Extract[_ET] = field(
        kw_only=True,
        repr=False,
        validator=validators.instance_of(Extract),
    )
    _transformer: Transform[_ET, _LT] = field(
        kw_only=True,
        repr=False,
        validator=validators.instance_of(Transform),
    )
    _loader: Load[_LT] = field(
        kw_only=True,
        repr=False,
        validator=validators.instance_of(Load),
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
    def extractor(self) -> Extract[_ET]:
        return self._extractor

    @property
    def transformer(self) -> Transform[_ET, _LT]:
        return self._transformer

    @property
    def loader(self) -> Load[_LT]:
        return self._loader

    @classmethod
    def of(
        cls,
        id: str,  # noqa: A002
        name: str,
        extractor: Extract[_ET],
        transformer: Transform[_ET, _LT],
        loader: Load[_ET] | None = None,
        description: str | None = None,
    ) -> Self:
        return cls(
            id=id,  # pyright: ignore
            name=name,  # pyright: ignore
            description=description,  # pyright: ignore
            extractor=extractor,  # pyright: ignore
            transformer=transformer,  # pyright: ignore
            loader=loader or NoOpLoad(),  # pyright: ignore
        )


@define
class NoOpLoad(Load[Any]):
    """Accepts extracted data and discards it all."""

    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False)

    def __attrs_post_init__(self) -> None:
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

    @not_disposed
    def __enter__(self) -> Self:
        return super().__enter__()

    @not_disposed
    def __call__(self, output: Any) -> None:  # noqa: ANN401
        self._logger.debug("Discarding processed extract data.")

    @property
    def is_disposed(self) -> bool:
        return self._is_disposed

    def dispose(self) -> None:
        self._is_disposed = True


# =============================================================================
# MODULE EXPORTS
# =============================================================================


__all__ = [
    "NoOpLoad",
]
