from abc import ABCMeta, abstractmethod
from typing import Generic, TypeVar

from sghi.disposable import Disposable

# =============================================================================
# TYPES
# =============================================================================

_ET = TypeVar("_ET")
_LD = TypeVar("_LD")


# =============================================================================
# BASE OPERATIONS INTERFACES
# =============================================================================


class Extract(Disposable, Generic[_ET], metaclass=ABCMeta):
    @abstractmethod
    def __call__(self) -> _ET:
        ...


class Transform(Disposable, Generic[_ET, _LD], metaclass=ABCMeta):
    @abstractmethod
    def __call__(self, extract: _ET) -> _LD:
        ...


class Load(Disposable, Generic[_LD], metaclass=ABCMeta):
    @abstractmethod
    def __call__(self, output: _LD) -> None:
        ...


# =============================================================================
# WORKFLOW INTERFACE
# =============================================================================


class ETLWorkflow(Generic[_ET, _LD], metaclass=ABCMeta):
    @property
    @abstractmethod
    def extractor(self) -> Extract[_ET]:
        ...

    @property
    @abstractmethod
    def transformer(self) -> Transform[_ET, _LD]:
        ...

    @property
    @abstractmethod
    def loader(self) -> Load[_LD]:
        ...
