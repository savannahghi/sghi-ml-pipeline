from collections.abc import Callable, Mapping
from typing import Final, Generic, TypeVar

import pandas as pd
from attrs import define, field, frozen, validators
from google.cloud import bigquery
from typing_extensions import Self

from sghi.disposable import not_disposed
from sghi.ml_pipeline.domain import Extract
from sghi.utils import ensure_instance_of, ensure_not_none

# =============================================================================
# TYPES
# =============================================================================

_ET = TypeVar("_ET")


_ExtractPreProcessor = Callable[[bigquery.QueryJob], _ET]


# =============================================================================
# CONSTANTS
# =============================================================================


DEFAULT_JOB_NAME: Final[str] = "default"


# =============================================================================
# HELPERS
# =============================================================================


def query_job_to_pd_dataframe(qjob: bigquery.QueryJob) -> pd.DataFrame:
    ensure_instance_of(qjob, bigquery.QueryJob)
    return qjob.to_dataframe()


# =============================================================================
# METADATA
# =============================================================================


@frozen
class BQueryExtractJobDescriptor:
    """Metadata describing a BigQuery extract job."""

    sql: str = field(
        repr=False,
        validator=[validators.instance_of(str), validators.min_len(2)],
    )
    project_id: str | None = field(
        default=None,
        validator=validators.optional(
            [validators.instance_of(str), validators.min_len(2)],
        ),
    )
    dataset_id: str | None = field(
        default=None,
        validator=validators.optional(
            [validators.instance_of(str), validators.min_len(2)],
        ),
    )
    table_name: str | None = field(
        default=None,
        validator=validators.optional(
            [validators.instance_of(str), validators.min_len(2)],
        ),
    )


@frozen
class BQueryExtractMeta:
    jobs: Mapping[str, BQueryExtractJobDescriptor] = field()


@frozen
class BQueryExtractResult(Generic[_ET]):
    results: Mapping[str, _ET] = field()

    @property
    def default_result(self) -> _ET:
        """The default result.

        .. note::

            Only available if a result with a job name equal to
            :attr:`DEFAULT_JOB_NAME` is present.

        :return: The default result.

        :raise KeyError: If no such result is present.
        """

        # TODO: Consider raising a more specific exception here instead of
        #  KeyError.
        return self.results[DEFAULT_JOB_NAME]


# =============================================================================
# SPEC IMPLEMENTATIONS
# =============================================================================


@define
class SimpleBQueryExtract(Extract[BQueryExtractResult[_ET]], Generic[_ET]):
    _extract_metadata: BQueryExtractMeta = field()
    _extract_preprocessor: _ExtractPreProcessor[_ET] = field()
    _client: bigquery.Client = field(factory=bigquery.Client)
    _is_disposed: bool = field(default=False, init=False)

    @not_disposed
    def __enter__(self) -> Self:
        return super().__enter__()

    @not_disposed
    def __call__(self) -> BQueryExtractResult[_ET]:
        extract_results = {
            job_name: self._query(job_descriptor=job_descriptor)
            for job_name, job_descriptor in self._extract_metadata.jobs.items()
        }
        return BQueryExtractResult(results=extract_results)

    @property
    def is_disposed(self) -> bool:
        return self._is_disposed

    def dispose(self) -> None:
        self._is_disposed = True
        self._client.close()

    def _query(self, job_descriptor: BQueryExtractJobDescriptor) -> _ET:
        job: bigquery.QueryJob = self._client.query(
            query=job_descriptor.sql,
            project=job_descriptor.project_id,
        )
        return self._extract_preprocessor(job)

    @classmethod
    def of(
        cls,
        extract_metadata: BQueryExtractMeta,
        extract_preprocessor: _ExtractPreProcessor = query_job_to_pd_dataframe,
        client: bigquery.Client | None = None,
    ) -> Self:
        ensure_not_none(
            value=extract_preprocessor,
            message="'extract_preprocessor' MUST not be None.",
        )
        return cls(
            extract_metadata=extract_metadata,  # pyright: ignore
            extract_preprocessor=extract_preprocessor,  # pyright: ignore
            client=client,  # pyright: ignore
        )

    @classmethod
    def of_single_job_descriptor(
        cls,
        extract_job_descriptor: BQueryExtractJobDescriptor,
        job_name: str = DEFAULT_JOB_NAME,
        extract_preprocessor: _ExtractPreProcessor = query_job_to_pd_dataframe,
        client: bigquery.Client | None = None,
    ) -> Self:
        ensure_instance_of(extract_job_descriptor, BQueryExtractJobDescriptor)
        ensure_not_none(
            value=extract_preprocessor,
            message="'extract_preprocessor' MUST not be None.",
        )
        extract_meta = BQueryExtractMeta({job_name: extract_job_descriptor})
        return cls(
            extract_metadata=extract_meta,  # pyright: ignore
            extract_preprocessor=extract_preprocessor,  # pyright: ignore
            client=client,  # pyright: ignore
        )


# =============================================================================
# MODULE EXPORTS
# =============================================================================


__all__ = [
    "DEFAULT_JOB_NAME",
    "BQueryExtractJobDescriptor",
    "BQueryExtractMeta",
    "BQueryExtractResult",
    "SimpleBQueryExtract",
    "query_job_to_pd_dataframe",
]
