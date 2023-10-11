import logging
from collections.abc import Mapping
from logging.config import dictConfig
from typing import Any

import sghi.app
from sghi.config import SettingInitializer
from sghi.registry import RegistryItemSet

from ..constants import (
    APP_LOG_LEVEL_REG_KEY,
    DEFAULT_CONFIG,
    LOGGING_CONFIG_KEY,
)

# =============================================================================
# HELPERS
# =============================================================================


def on_logging_level_changed(signal: RegistryItemSet) -> None:
    if signal.item_key != APP_LOG_LEVEL_REG_KEY:
        return

    logging.getLogger("sghi").setLevel(signal.item_value)


# =============================================================================
# INITIALIZERS
# =============================================================================


class LoggingInitializer(SettingInitializer):
    """:class:`SettingInitializer` that configures logging for the app."""

    @property
    def setting(self) -> str:
        return LOGGING_CONFIG_KEY

    def execute(self, an_input: Mapping[str, Any] | None) -> Mapping[str, Any]:
        logging_config: dict[str, Any] = dict(
            an_input or DEFAULT_CONFIG[self.setting],
        )
        dictConfig(logging_config)
        logging.getLogger("sghi").setLevel(
            sghi.app.registry.get(APP_LOG_LEVEL_REG_KEY, logging.CRITICAL),
        )
        sghi.app.registry.dispatcher.connect(
            signal_type=RegistryItemSet,
            receiver=on_logging_level_changed,
            weak=False,  # Last for the lifetime of the application
        )
        return logging_config
