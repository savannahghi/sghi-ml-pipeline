import io
import logging
from collections.abc import Callable
from logging import Logger
from typing import TypeVar, overload

from attrs import define, field
from google.cloud import storage
from typing_extensions import Self

from sghi.ml_pipeline.domain import Extract, Load
from sghi.utils import ensure_not_none, type_fqn

# =============================================================================
# TYPES
# =============================================================================


_ET = TypeVar("_ET")


_LT = TypeVar("_LT")


_FileFactory = Callable[[], io.IOBase]


_OutputToBlob = Callable[[_LT, storage.Bucket], storage.Blob]


_OutputToBytes = Callable[[_LT], bytes]


# =============================================================================
# SPEC IMPLEMENTATIONS
# =============================================================================


@define
class SimpleGoogleStorage(Extract[_ET], Load[_LT]):
    _bucket: storage.Bucket = field()
    _output_to_blob: _OutputToBlob[_LT] = field()
    _output_to_bytes: _OutputToBytes[_LT] = field()
    _client: storage.Client = field(factory=storage.Client)
    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False)

    def __attrs_post_init__(self) -> None:
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

    @overload
    def __call__(self, output: None = None) -> _ET:
        ...

    @overload
    def __call__(self, output: _LT) -> None:
        ...

    def __call__(self, output: _LT | None = None) -> _ET | None:
        if output is None:
            _err_msg: str = "Coming soon."
            raise NotImplementedError(_err_msg)

        return self._upload_to_bucket(output)

    @property
    def is_disposed(self) -> bool:
        return self._is_disposed

    def dispose(self) -> None:
        self._is_disposed = True
        self._client.close()
        self._logger.debug("Disposal complete.")

    def _upload_to_bucket(self, output: _LT) -> None:
        ensure_not_none(output)
        blob: storage.Blob = self._output_to_blob(output, self._bucket)
        with io.BytesIO(initial_bytes=self._output_to_bytes(output)) as file:
            file.seek(0)
            blob.upload_from_file(file)

    @classmethod
    def of_loader(
        cls,
        bucket: storage.Bucket | str,
        output_to_blob: _OutputToBlob,
        output_to_bytes: _OutputToBytes,
        client: storage.Client | None = None,
    ) -> Self:
        _client: storage.Client = client or storage.Client()
        _bucket: storage.Bucket = (
            bucket
            if isinstance(bucket, storage.Bucket)
            else storage.Bucket.from_string(uri=bucket, client=_client)
        )
        return cls(
            bucket=bucket,  # pyright: ignore
            output_to_blob=output_to_blob,  # pyright: ignore
            output_to_bytes=output_to_bytes,  # pyright: ignore
            client=client,  # pyright: ignore
        )
