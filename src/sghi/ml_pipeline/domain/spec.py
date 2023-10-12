from abc import ABCMeta, abstractmethod
from typing import Generic, TypeVar

from sghi.disposable import Disposable

# =============================================================================
# TYPES
# =============================================================================

_ET = TypeVar("_ET")
_LT = TypeVar("_LT")


# =============================================================================
# BASE OPERATIONS INTERFACES
# =============================================================================


class Extract(Disposable, Generic[_ET], metaclass=ABCMeta):
    __slots__ = ()

    @abstractmethod
    def __call__(self) -> _ET:
        ...


class Transform(Disposable, Generic[_ET, _LT], metaclass=ABCMeta):
    __slots__ = ()

    @abstractmethod
    def __call__(self, extract: _ET) -> _LT:
        ...


class Load(Disposable, Generic[_LT], metaclass=ABCMeta):
    __slots__ = ()

    @abstractmethod
    def __call__(self, output: _LT) -> None:
        ...


# =============================================================================
# WORKFLOW INTERFACE
# =============================================================================


class ETLWorkflow(Generic[_ET, _LT], metaclass=ABCMeta):
    __slots__ = ()

    @property
    @abstractmethod
    def id(self) -> str:  # noqa: A003
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @property
    @abstractmethod
    def description(self) -> str | None:
        ...

    @property
    @abstractmethod
    def extractor(self) -> Extract[_ET]:
        ...

    @property
    @abstractmethod
    def transformer(self) -> Transform[_ET, _LT]:
        ...

    @property
    @abstractmethod
    def loader(self) -> Load[_LT]:
        ...
