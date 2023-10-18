from unittest import TestCase

from attrs import frozen

from sghi.ml_pipeline.domain import DataProcessor, DataSink, DataSource

# =============================================================================
# TESTS
# =============================================================================


class TestDataProcessor(TestCase):
    """
    Tests for the :class:`DataProcessor` interface default implementations.
    """

    def test_call_magic_method_return_value(self) -> None:
        """
        The default implementation of :meth:`DataProcessor.__call__` should
        return the same value as :meth:`DataProcessor.process`.
        """

        @frozen
        class MockDataProcessor(DataProcessor[str, int]):
            @property
            def is_disposed(self) -> bool:
                return False

            def dispose(self) -> None:
                ...

            def process(self, raw_data: str) -> int:
                return int(raw_data)

        str_to_int: DataProcessor[str, int] = MockDataProcessor()

        assert str_to_int("10") == 10
        assert str_to_int("20") == 20
        assert str_to_int("30") == 30
        assert str_to_int("40") == 40
        assert str_to_int("50") == 50


class TestDataSink(TestCase):
    """Tests for the :class:`DataSink` interface default implementations."""

    def test_call_magic_method_side_effects(self) -> None:
        """
        The default implementation of :meth:`DataSink.__call__` should result
        in the same side effects as :meth:`DataSink.drain`.
        """

        output: list[int] = []

        @frozen
        class MockDataSink(DataSink[int]):
            @property
            def is_disposed(self) -> bool:
                return False

            def dispose(self) -> None:
                ...

            def drain(self, processed_data: int) -> None:
                output.append(processed_data)

        data_sink: DataSink[int] = MockDataSink()
        data_sink(0)
        data_sink(1)
        data_sink(2)

        assert len(output) == 3
        assert output[0] == 0
        assert output[1] == 1
        assert output[2] == 2


class TestDataSource(TestCase):
    """Tests for the :class:`DataSource` interface default implementations."""

    def test_call_magic_method_return_value(self) -> None:
        """
        The default implementation of :meth:`DataSource.__call__` should
        return the same value as :meth:`DataSource.draw`.
        """

        @frozen
        class MockDataSource(DataSource[int]):
            @property
            def is_disposed(self) -> bool:
                return False

            def dispose(self) -> None:
                ...

            def draw(self) -> int:
                return 10

        assert MockDataSource()() == 10
