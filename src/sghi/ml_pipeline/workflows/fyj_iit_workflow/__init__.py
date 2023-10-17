import logging
from collections.abc import Mapping, Sequence
from logging import Logger
from typing import Final

import pandas as pd
from attrs import define, field
from typing_extensions import Self

import sghi.app
from sghi.disposable import not_disposed
from sghi.ml_pipeline.domain import ETLWorkflow, Transform
from sghi.ml_pipeline.lib.common import SimpleETLWorkflow
from sghi.ml_pipeline.lib.google.big_query import (
    BQueryExtractJobDescriptor,
    BQueryExtractMeta,
    BQueryExtractResult,
    SimpleBQueryExtract,
)
from sghi.utils import ensure_instance_of, type_fqn

# =============================================================================
# CONSTANTS
# =============================================================================


AGE_BINS: Final[Sequence[int]] = (0, 18, 30, 40, 50, 60, 100)

AGE_LABELS: Final[Sequence[str]] = (
    "0-18",
    "19-30",
    "31-40",
    "41-50",
    "51-60",
    "61+",
)

COMMON_VALUE_MISSING_STATES: Final[Sequence[str | None]] = (
    None,
    "",
    "none",
    "None",
    "Nan",
)

NUTRITIONAL_STATUS_TRANSFORMATION_MAPPING: Final[Mapping[str, str]] = {
    "": "Unknown",
    "At Risk ": "Moderate acute malnutrition",
    "MAM": "Moderate acute malnutrition",
    "Mild": "Moderate acute malnutrition",
    "Moderate": "Moderate acute malnutrition",
    "Obese": "Overweight",
    "Overweight/Obese": "Overweight",
    "Overweight": "Overweight",
    "Normal": "Normal",
    "SAM": "Severe acute malnutrition",
    "Severe ": "Severe acute malnutrition",
    "Unknown": "Unknown",
}

SELECTED_FEATURES: Final[Sequence[str]] = (
    "age_group",
    "at_risk_population",
    "differentiated_care",
    "education_level",
    "ever_on_haart",
    "ever_on_ipt",
    "gender",
    "has_chronic_illnesses_cormobidities",
    "in_school",
    "marital_status",
    "nutritional_status",
    "occupation",
    "on_ipt",
    "population_type",
    "regimen",
    "stability",
    "who_stage",
    "years_on_art",
)

TARGET_COLUMN_TRANSFORMATION_MAPPING: Final[Mapping[str, int]] = {
    "honored": 1,
    "missed": 0,
}

WHO_STAGING_TRANSFORMATION_MAPPING: Final[Mapping[str, str]] = {
    "Stage 1": "1",
    "Stage 2": "2",
    "Stage 3": "3",
    "Stage 4": "4",
    "Uncategorized": "1",
}

YEARS_ON_ART_BINS: Final[Sequence[int]] = (0, 1, 3, 7, 10, 13, 17, 21, 200)

YEARS_ON_ART_LABELS: Final[Sequence[str]] = (
    "0-1",
    "2-3",
    "4-7",
    "8-10",
    "11-13",
    "14-17",
    "18-21",
    "22+",
)


# =============================================================================
# HELPERS
# =============================================================================


def _encode_target_column(df: pd.DataFrame) -> None:
    ensure_instance_of(df, pd.DataFrame)

    df["target"] = df["appointment_class"].map(
        TARGET_COLUMN_TRANSFORMATION_MAPPING,  # type: ignore
    )


def _preprocess_and_clean_data(df: pd.DataFrame) -> None:
    ensure_instance_of(df, pd.DataFrame)

    # Create a new column 'age_group' based on age bins
    df["age_group"] = pd.cut(
        df["Age"],
        bins=AGE_BINS,
        labels=AGE_LABELS,
        right=False,
    )

    # Convert all columns to lower-case
    df.columns = df.columns.str.lower()

    # Convert years on ART values to a numeric type
    df["years_on_art"] = pd.to_numeric(df.years_on_art)

    # Create bins for years on art
    df["years_on_art"] = pd.cut(
        df["years_on_art"],
        bins=YEARS_ON_ART_BINS,
        labels=YEARS_ON_ART_LABELS,
        include_lowest=True,
    )

    # Standardize missing values
    df.in_school = df.in_school.replace(
        COMMON_VALUE_MISSING_STATES,
        "No",
    )
    df.in_school = df.in_school.astype(str)
    df.marital_status = df.marital_status.replace(
        COMMON_VALUE_MISSING_STATES,
        df["marital_status"].mode()[0],
    )
    df.ever_on_ipt = df.ever_on_ipt.replace(
        COMMON_VALUE_MISSING_STATES,
        "No",
    )
    df.years_on_art = df.years_on_art.replace(
        COMMON_VALUE_MISSING_STATES,
        df["years_on_art"].mode()[0],
    )
    df["weight"] = pd.to_numeric(df["weight"])
    df.weight = df.weight.replace(
        COMMON_VALUE_MISSING_STATES,
        df["weight"].mean(),
    )
    # TODO: Confirm with Denzel why this is necessary.
    # -> mean_value = df["weight"].mean()
    # -> df.weight.fillna(mean_value, inplace=True)

    df.occupation = df.occupation.replace(
        COMMON_VALUE_MISSING_STATES,
        "unknown",
    )
    df.education_level = df.education_level.replace(
        COMMON_VALUE_MISSING_STATES,
        "unknown",
    )
    # fill regimen with most prescribed regimen
    df.regimen = df.regimen.replace(
        COMMON_VALUE_MISSING_STATES,
        df["regimen"].mode()[0],
    )
    df.at_risk_population = df.at_risk_population.replace(
        [None, "none", "None"],
        "general population",
    )
    df.differentiated_care = df.differentiated_care.replace(
        COMMON_VALUE_MISSING_STATES,
        "Standard Care",
    )
    df.ever_on_haart = df.ever_on_haart.replace(
        COMMON_VALUE_MISSING_STATES,
        "No",
    )
    df.stability = df.stability.replace(
        COMMON_VALUE_MISSING_STATES,
        "unknown",
    )

    # Standardize nutrition status
    df["nutritional_status"] = df["nutritional_status"].replace(
        NUTRITIONAL_STATUS_TRANSFORMATION_MAPPING,
    )

    # Standardize nutrition status
    df["who_stage"] = df["who_stage"].replace(
        WHO_STAGING_TRANSFORMATION_MAPPING,
    )


# =============================================================================
# SPEC IMPLEMENTATIONS
# =============================================================================


@define
class ProcessTestData(
    Transform[BQueryExtractResult[pd.DataFrame], pd.DataFrame],
):
    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False)

    def __attrs_post_init__(self) -> None:
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

    @not_disposed
    def __enter__(self) -> Self:
        return super(Transform, self).__enter__()

    @not_disposed
    def __call__(
        self,
        extract: BQueryExtractResult[pd.DataFrame],
    ) -> pd.DataFrame:
        ensure_instance_of(extract, BQueryExtractResult)
        self._logger.info("Process IIT Test data.")

        df1: pd.DataFrame = extract.results["query1"]
        df2: pd.DataFrame = extract.results["query2"]
        df: pd.DataFrame = pd.concat([df1, df2], ignore_index=True)

        self._logger.debug("Preprocess and clean test data.")
        _preprocess_and_clean_data(df)

        self._logger.debug("Encode target column.")
        _encode_target_column(df)

        return df

    @property
    def is_disposed(self) -> bool:
        return self._is_disposed

    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.debug("Disposal complete.")


@define
class ProcessTrainingData(
    Transform[BQueryExtractResult[pd.DataFrame], pd.DataFrame],
):
    _is_disposed: bool = field(default=False, init=False)
    _logger: Logger = field(init=False)

    def __attrs_post_init__(self) -> None:
        self._logger: Logger = logging.getLogger(type_fqn(self.__class__))

    @not_disposed
    def __enter__(self) -> Self:
        return super(Transform, self).__enter__()

    @not_disposed
    def __call__(
        self,
        extract: BQueryExtractResult[pd.DataFrame],
    ) -> pd.DataFrame:
        ensure_instance_of(extract, BQueryExtractResult)
        self._logger.info("Process IIT Training data.")

        df: pd.DataFrame = extract.default_result

        self._logger.debug("Preprocess and clean training data.")
        _preprocess_and_clean_data(df)

        self._logger.debug("Encode target column.")
        _encode_target_column(df)

        return df

    @property
    def is_disposed(self) -> bool:
        return self._is_disposed

    def dispose(self) -> None:
        self._is_disposed = True
        self._logger.debug("Disposal complete.")


# =============================================================================
# ETL WORKFLOW FACTORY
# =============================================================================


def fyj_iit_test_data_etl_workflow_factory() -> (
    ETLWorkflow[
        BQueryExtractResult[pd.DataFrame],
        pd.DataFrame,
    ]
):
    return SimpleETLWorkflow.of(
        id="fyj-iit-test-data",
        name="FyJ IIT Test Data",
        extractor=SimpleBQueryExtract.of(
            BQueryExtractMeta(
                jobs={
                    "query1": BQueryExtractJobDescriptor(
                        sql=sghi.app.conf.FYJ_IIT_TEST_DATA_QUERY_1,
                    ),
                    "query2": BQueryExtractJobDescriptor(
                        sql=sghi.app.conf.FYJ_IIT_TEST_DATA_QUERY_2,
                    ),
                },
            ),
        ),
        transformer=ProcessTestData(),
    )


def fyj_iit_train_data_etl_workflow_factory() -> (
    ETLWorkflow[
        BQueryExtractResult[pd.DataFrame],
        pd.DataFrame,
    ]
):
    return SimpleETLWorkflow.of(
        id="fyj-iit-training-data",
        name="FyJ IIT Training Data",
        extractor=SimpleBQueryExtract.of_single_job_descriptor(
            BQueryExtractJobDescriptor(
                sql=sghi.app.conf.FYJ_IIT_TRAINING_DATA_QUERY,
            ),
        ),
        transformer=ProcessTrainingData(),
    )
